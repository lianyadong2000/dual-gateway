package cend

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"dual-gateway/internal/config"
	"dual-gateway/internal/redis"
	"dual-gateway/internal/shadow"
	"dual-gateway/pkg/logger"
)

// C端网关服务器
type Server struct {
	ID          string
	Config      *config.CendConfig
	AuthConfig  *config.AuthConfig
	Upgrader    websocket.Upgrader
	Connections sync.Map
	Handler     *MessageHandler
	ConnCount   atomic.Int64
	Ctx         context.Context
	Cancel      context.CancelFunc
	wg          sync.WaitGroup
	Logger      logger.Logger
}

// 创建C端网关
func NewServer(cfg *config.Config, log logger.Logger) *Server {
	ctx, cancel := context.WithCancel(context.Background())

	server := &Server{
		ID:         generateServerID(),
		Config:     &cfg.Cend,
		AuthConfig: &cfg.Auth,
		Logger:     log,
		Ctx:        ctx,
		Cancel:     cancel,
	}

	// 缓冲大小从配置读取（默认4KB/4KB，5万连接时显著降低内存占用）
	readBuf := cfg.Cend.ReadBufferSize
	writeBuf := cfg.Cend.WriteBufferSize
	if readBuf <= 0 {
		readBuf = 4096
	}
	if writeBuf <= 0 {
		writeBuf = 4096
	}

	server.Upgrader = websocket.Upgrader{
		ReadBufferSize:  readBuf,
		WriteBufferSize: writeBuf,
		CheckOrigin: func(r *http.Request) bool {
			return true // 生产环境需要严格检查
		},
		EnableCompression: cfg.Cend.EnableCompression,
	}

	server.Handler = NewMessageHandler(server, log, &cfg.Auth, &cfg.Shadow, &cfg.RateLimit)

	return server
}

// 启动服务器
func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWebSocket)
	mux.HandleFunc("/health", s.handleHealth)

	addr := fmt.Sprintf("%s:%d", s.Config.Host, s.Config.Port)

	// 创建HTTP服务器
	httpServer := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// 注册网关到Redis（失败降级，不阻塞启动）
	if err := redis.RegisterGateway(s.ID, "cend", addr); err != nil {
		s.Logger.Warn("Failed to register gateway in Redis", "error", err)
	}

	s.Logger.Info("C-end gateway started", "id", s.ID, "addr", addr)

	// 启动HTTP服务器
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.Logger.Error("HTTP server error", "error", err)
		}
	}()

	// 启动心跳检查
	s.wg.Add(1)
	go s.heartbeatChecker()

	// 启动影子管理（预占超时回滚）
	s.Handler.Shadow.Start()

	// 启动状态事件订阅（桩状态变更/指令结果 → 推送订阅的C端用户）
	s.wg.Add(1)
	go s.shadowEventLoop()

	// 启动优雅关闭
	s.wg.Add(1)
	go s.waitForShutdown(httpServer)

	return nil
}

// 状态事件订阅循环：订阅 shadow:events 与 shadow:cmdresults，按订阅表推送C端
func (s *Server) shadowEventLoop() {
	defer s.wg.Done()

	// 状态变更事件
	go redis.SubscribeMessage(shadow.EventChannel, func(data []byte) {
		var evt shadow.ShadowEvent
		if err := json.Unmarshal(data, &evt); err != nil {
			return
		}
		push := map[string]interface{}{
			"event":     "shadow.update",
			"device_id": evt.DeviceID,
			"state":     evt.State,
			"version":   evt.Version,
			"timestamp": evt.Timestamp,
		}
		payload, _ := json.Marshal(push)
		s.Handler.PushShadowEvent(evt.DeviceID, payload)
	})

	// 指令执行结果事件
	redis.SubscribeMessage(shadow.CmdResultChan, func(data []byte) {
		var result map[string]interface{}
		if err := json.Unmarshal(data, &result); err != nil {
			return
		}
		deviceID, _ := result["cmd_id"].(string)
		payload := data // 原样透传
		// cmd_id 不含设备ID，需通过指令关联；简化：广播给全部订阅者
		// 更精确的做法由业务层维护 cmd_id->user 映射，此处按 cmd_id 前缀无设备信息，
		// 退化为遍历订阅表广播（订阅数量小，可接受）
		s.Handler.subMu.RLock()
		devices := make([]string, 0, len(s.Handler.subscribers))
		for dev := range s.Handler.subscribers {
			devices = append(devices, dev)
		}
		s.Handler.subMu.RUnlock()
		for _, dev := range devices {
			push := map[string]interface{}{
				"event":     "cmd.result",
				"device_id": dev,
				"result":    json.RawMessage(payload),
			}
			p, _ := json.Marshal(push)
			s.Handler.PushShadowEvent(dev, p)
			_ = deviceID
		}
	})
}

// 处理WebSocket连接
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// 接入限流：TCP accept 节流（防握手洪峰，超限快速拒绝）
	if !s.Handler.AcceptLimiter.Allow() {
		http.Error(w, "Rate limited", http.StatusTooManyRequests)
		return
	}

	// 先检查连接数限制（快速失败，避免无谓的资源分配）
	if s.ConnCount.Load() >= s.Config.MaxConnections {
		http.Error(w, "Server is full", http.StatusServiceUnavailable)
		return
	}

	// 升级HTTP连接为WebSocket
	conn, err := s.Upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.Logger.Debug("WebSocket upgrade failed", "error", err)
		return
	}

	// 升级后再次检查（竞态窗口）
	if s.ConnCount.Load() >= s.Config.MaxConnections {
		conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "Server is full"))
		conn.Close()
		return
	}

	// 创建连接
	clientConn := NewConnection(conn, s.ID, s.Logger)
	s.Connections.Store(clientConn.ID, clientConn)
	s.ConnCount.Add(1)

	s.Logger.Debug("New connection", "id", clientConn.ID,
		"remote", conn.RemoteAddr(), "total", s.ConnCount.Load())

	// 启动读写协程
	s.wg.Add(2)
	go func() {
		defer s.wg.Done()
		clientConn.WriteLoop()
	}()

	go func() {
		defer s.wg.Done()
		clientConn.ReadLoop(s.Handler)
	}()
}

// 处理健康检查
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// 心跳检查
func (s *Server) heartbeatChecker() {
	defer s.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now := time.Now().Unix()

			s.Connections.Range(func(key, value interface{}) bool {
				conn := value.(*Connection)

				if now-conn.LastActive.Load() > s.Config.HeartbeatTimeout {
					s.Logger.Debug("Connection timeout", "id", conn.ID)
					conn.Close()
					s.Connections.Delete(key)
					s.ConnCount.Add(-1)
				}

				return true
			})

		case <-s.Ctx.Done():
			return
		}
	}
}

// 等待关闭信号
func (s *Server) waitForShutdown(httpServer *http.Server) {
	defer s.wg.Done()

	<-s.Ctx.Done()

	s.Logger.Info("Shutting down C-end gateway...")

	// 关闭HTTP服务器
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		s.Logger.Error("HTTP server shutdown error", "error", err)
	}

	// 关闭所有连接
	s.Connections.Range(func(key, value interface{}) bool {
		conn := value.(*Connection)
		conn.Close()
		s.Connections.Delete(key)
		return true
	})

	// 注销网关
	redis.UnregisterGateway(s.ID)

	s.Logger.Info("C-end gateway shutdown complete")
}

// 停止服务器
func (s *Server) Stop() {
	s.Handler.Shadow.Stop()
	s.Cancel()
	s.wg.Wait()
}

// 生成服务器ID
func generateServerID() string {
	hostname, _ := os.Hostname()
	return fmt.Sprintf("gateway-%s-%d", hostname, os.Getpid())
}
