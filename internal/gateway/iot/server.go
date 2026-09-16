package iot

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/dtls/v2"
	"github.com/quic-go/quic-go"

	"dual-gateway/internal/command"
	"dual-gateway/internal/config"
	"dual-gateway/internal/ratelimit"
	"dual-gateway/internal/redis"
	"dual-gateway/internal/security"
	"dual-gateway/internal/shadow"
	"dual-gateway/pkg/logger"
	"dual-gateway/pkg/utils"
)

// IoT端网关服务器
type Server struct {
	ID          string
	Config      *config.IoTConfig
	UDPConn     *net.UDPConn
	DTLSConfig  *dtls.Config
	PSKStore    *security.PSKStore
	DefaultPSK  []byte
	Handler     *MessageHandler
	PacketCount atomic.Int64
	ByteCount   atomic.Int64
	Dropped     atomic.Int64
	Ctx         context.Context
	Cancel      context.CancelFunc
	wg          sync.WaitGroup
	Logger      logger.Logger

	// 设备影子 / 指令队列 / 接入限流
	Shadow         *shadow.Manager
	CmdQueue       *command.Queue
	ConnectLimiter *ratelimit.ConnectLimiter
	IPLimiter      *ratelimit.IPLimiter
	CmdChannel     string // 在线直发触发频道

	packetCh chan udpPacket
	dtlsLis  net.Listener
	quicLis  *quic.EarlyListener
	quicTrans *quic.Transport

	// AsyncRedis 连接后 Redis 异步工作队列（Register/Touch/唤醒补发），
	// 固定容量+固定消费者：5万连接不因 Redis 往返阻塞 CONNECT 主路径
	AsyncRedis chan func()
}

// UDP数据包任务（Peer 统一 UDP/DTLS/QUIC 传输端点）
type udpPacket struct {
	data []byte
	peer *Peer
}

// 创建IoT网关
func NewServer(cfg *config.Config, log logger.Logger) *Server {
	ctx, cancel := context.WithCancel(context.Background())

	// 创建PSK存储
	pskStore := security.NewPSKStore()

	// 添加PSK设备（不足16字节的PSK用SHA-256派生，保证DTLS可用）
	for deviceID, psk := range cfg.Auth.PSKDevices {
		pskBytes := derivePSK(psk)
		if err := pskStore.AddPSK(deviceID, pskBytes, []byte(deviceID)); err != nil {
			log.Warn("Failed to add PSK device", "device_id", deviceID, "error", err)
		}
	}

	// 默认PSK（配置了则所有未登记设备可用默认PSK连接，便于生产环境灰度/测试）
	var defaultPSK []byte
	if cfg.IoT.PSK != "" {
		defaultPSK = derivePSK(cfg.IoT.PSK)
	}

	// 配置DTLS
	dtlsConfig := &dtls.Config{
		PSK: func(hint []byte) ([]byte, error) {
			// 根据hint查找PSK
			deviceID := string(hint)
			if psk, ok := pskStore.GetPSK(deviceID); ok {
				return psk, nil
			}
			// 如果没有找到，使用默认PSK
			if defaultPSK != nil {
				return defaultPSK, nil
			}
			return nil, fmt.Errorf("PSK not found for device: %s", deviceID)
		},
		PSKIdentityHint: []byte("iot-gateway"),
		CipherSuites: []dtls.CipherSuiteID{
			dtls.TLS_PSK_WITH_AES_128_CCM,
			dtls.TLS_PSK_WITH_AES_128_CCM_8,
			dtls.TLS_PSK_WITH_AES_128_GCM_SHA256,
		},
		ConnectContextMaker: func() (context.Context, func()) {
			return context.WithTimeout(ctx, 30*time.Second)
		},
		MTU:            1200,
		FlightInterval: 100 * time.Millisecond,
	}

	return &Server{
		ID:             generateServerID(),
		Config:         &cfg.IoT,
		DTLSConfig:     dtlsConfig,
		PSKStore:       pskStore,
		DefaultPSK:     defaultPSK,
		Handler:        NewMessageHandler(log),
		Ctx:            ctx,
		Cancel:         cancel,
		Logger:         log,
		packetCh:       make(chan udpPacket, 1048576), // 1M：5万连接洪峰时吸收队列积压，避免PINGREQ/PUBLISH被丢
		Shadow:         shadow.NewManager(&cfg.Shadow),
		CmdQueue:       command.NewQueue(&cfg.Shadow),
		ConnectLimiter: ratelimit.NewConnectLimiter(cfg.RateLimit.ConnectRate, cfg.RateLimit.ConnectBurst, cfg.RateLimit.Enabled),
		IPLimiter:      ratelimit.NewIPLimiter(cfg.RateLimit.UDPPerIPRate, cfg.RateLimit.UDPPerIPBurst, cfg.RateLimit.Enabled),
		AsyncRedis:     make(chan func(), 65536),
		CmdChannel:     shadow.CmdChannel,
	}
}

// 启动服务器
func (s *Server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.Config.Host, s.Config.Port)

	// 解析UDP地址
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return fmt.Errorf("failed to resolve UDP addr: %w", err)
	}

	// 创建UDP连接
	s.UDPConn, err = net.ListenUDP("udp", udpAddr)
	if err != nil {
		return fmt.Errorf("failed to listen UDP: %w", err)
	}

	// 设置UDP缓冲区（增大以支撑高并发）
	if err := s.UDPConn.SetReadBuffer(32 * 1024 * 1024); err != nil { // 32MB
		s.Logger.Warn("Failed to set UDP read buffer", "error", err)
	}
	if err := s.UDPConn.SetWriteBuffer(32 * 1024 * 1024); err != nil { // 32MB
		s.Logger.Warn("Failed to set UDP write buffer", "error", err)
	}

	// 注册网关到Redis（失败降级，不阻塞启动）
	if err := redis.RegisterGateway(s.ID, "iot", addr); err != nil {
		s.Logger.Warn("Failed to register gateway in Redis", "error", err)
	}

	// worker数量：默认64，至少4
	workerCount := s.Config.WorkerCount
	if workerCount <= 0 {
		workerCount = 64
	}
	if workerCount < 4 {
		workerCount = 4
	}

	s.Logger.Info("IoT gateway started", "id", s.ID, "addr", addr, "workers", workerCount, "queue", cap(s.packetCh))

	// 启动UDP读取循环（只读+入队，不参与处理，避免被处理阻塞）
	s.wg.Add(1)
	go s.readLoop()

	// 启动worker池（处理与读取解耦）
	for i := 0; i < workerCount; i++ {
		s.wg.Add(1)
		go s.workerLoop()
	}

	// 启动DTLS监听（独立端口）
	if s.Config.DTLSEnabled && s.Config.DTLSPort > 0 && s.Config.DTLSPort != s.Config.Port {
		dtlsAddr := fmt.Sprintf("%s:%d", s.Config.Host, s.Config.DTLSPort)
		s.wg.Add(1)
		go s.dtlsLoop(dtlsAddr)
	}

	// 启动QUIC监听（独立端口；MQTT-SN over QUIC，0-RTT + 连接迁移）
	if s.Config.QUICEnabled && s.Config.QUICPort > 0 {
		quicAddr := fmt.Sprintf("%s:%d", s.Config.Host, s.Config.QUICPort)
		s.wg.Add(1)
		go s.quicLoop(quicAddr)
	}

	// 启动会话清理
	s.wg.Add(1)
	go s.sessionCleanup()

	// 启动影子管理（预占超时回滚 + 本地在线过期判定）
	s.Shadow.Start()

	// 启动指令触发订阅（C端指令经 Pub/Sub 触发在线直发）
	s.wg.Add(1)
	go s.cmdTriggerLoop()

	// 启动源IP限流桶清理
	s.wg.Add(1)
	go s.ipLimiterCleanup()

	// 启动连接后 Redis 异步工作消费者（限 256 并发，保护 Redis 连接池）
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		sem := make(chan struct{}, 256)
		for {
			select {
			case work := <-s.AsyncRedis:
				sem <- struct{}{}
				go func() {
					defer func() { <-sem }()
					defer func() { recover() }()
					work()
				}()
			case <-s.Ctx.Done():
				return
			}
		}
	}()

	return nil
}

// 指令触发订阅：C端指令入队后，经该频道通知本节点立即直发（桩在线时）
func (s *Server) cmdTriggerLoop() {
	defer s.wg.Done()
	redis.SubscribeMessage(s.CmdChannel, func(data []byte) {
		var trig shadow.CmdTrigger
		if err := json.Unmarshal(data, &trig); err != nil {
			return
		}
		s.Handler.dispatchPendingCommands(s, trig.DeviceID)
	})
}

// 源IP限流桶定期清理（防内存膨胀）
func (s *Server) ipLimiterCleanup() {
	defer s.wg.Done()
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.IPLimiter.Cleanup()
		case <-s.Ctx.Done():
			return
		}
	}
}

// UDP读取循环：只负责读与入队（非阻塞），处理由worker池完成
func (s *Server) readLoop() {
	defer s.wg.Done()

	buffer := make([]byte, 65535) // UDP最大包大小

	for {
		select {
		case <-s.Ctx.Done():
			return
		default:
		}

		n, remoteAddr, err := s.UDPConn.ReadFromUDP(buffer)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			select {
			case <-s.Ctx.Done():
				return
			default:
			}
			s.Logger.Error("UDP read error", "error", err)
			continue
		}

		s.PacketCount.Add(1)
		s.ByteCount.Add(int64(n))

		// 复制数据（防止buffer复用覆盖）
		data := make([]byte, n)
		copy(data, buffer[:n])

		// 非阻塞入队：队列满时丢弃并计数（读循环永不被处理阻塞）
		select {
		case s.packetCh <- udpPacket{data: data, peer: &Peer{Addr: remoteAddr}}:
		default:
			s.Dropped.Add(1)
		}
	}
}

// worker处理循环
func (s *Server) workerLoop() {
	defer s.wg.Done()

	for {
		select {
		case pkt := <-s.packetCh:
			s.Handler.HandlePacket(s, pkt.data, pkt.peer)
		case <-s.Ctx.Done():
			return
		}
	}
}

// DTLS监听循环
func (s *Server) dtlsLoop(addr string) {
	defer s.wg.Done()

	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		s.Logger.Error("Failed to resolve DTLS addr", "error", err)
		return
	}

	listener, err := dtls.Listen("udp", udpAddr, s.DTLSConfig)
	if err != nil {
		s.Logger.Error("Failed to listen DTLS", "error", err, "addr", addr)
		return
	}
	s.dtlsLis = listener
	s.Logger.Info("IoT DTLS gateway started", "addr", addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-s.Ctx.Done():
				return
			default:
			}
			s.Logger.Error("DTLS accept error", "error", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		s.wg.Add(1)
		go s.handleDTLSConn(conn)
	}
}

// 处理DTLS连接（每个连接一个读循环，MQTT-SN over DTLS按数据报读取）
// 修复：回包走 DTLS 连接本身（Peer.Conn），而非裸 UDP（旧实现 DTLS 客户端收不到响应）
func (s *Server) handleDTLSConn(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()
	defer s.Handler.dropDeviceByConn(s, conn) // DTLS 连接断开：设备下线

	// DTLS record MTU(1200) 限制单包 <=1300；原 65535/连接在百万连接下仅读缓冲即需 64GB，缩到 1500。
	buffer := make([]byte, 1500)

	for {
		n, err := conn.Read(buffer)
		if err != nil {
			return
		}

		if n < 2 {
			continue
		}

		// 复制数据并处理
		data := make([]byte, n)
		copy(data, buffer[:n])

		remoteAddr := &net.UDPAddr{}
		if udpAddr, ok := conn.RemoteAddr().(*net.UDPAddr); ok {
			remoteAddr = udpAddr
		}

		s.PacketCount.Add(1)
		s.ByteCount.Add(int64(n))

		peer := &Peer{Addr: remoteAddr, Conn: conn}
		select {
		case s.packetCh <- udpPacket{data: data, peer: peer}:
		default:
			s.Dropped.Add(1)
		}
	}
}

// 会话清理循环
func (s *Server) sessionCleanup() {
	defer s.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	timeout := time.Duration(s.Config.SessionTimeout) * time.Second
	if timeout <= 0 {
		timeout = 300 * time.Second
	}

	for {
		select {
		case <-ticker.C:
			expired := s.Handler.CleanupExpiredSessions(timeout)
			for _, deviceID := range expired {
				s.Shadow.MarkOffline(deviceID)
			}
		case <-s.Ctx.Done():
			return
		}
	}
}

// 发送UDP数据包（UDP 传输专用底层）
func (s *Server) SendUDP(data []byte, addr *net.UDPAddr) error {
	_, err := s.UDPConn.WriteToUDP(data, addr)
	return err
}

// 停止服务器
func (s *Server) Stop() {
	s.Cancel()

	// 网关停机/重启：本节点全部设备影子标记离线（防止残留"在线"误导C端）
	// 仅清影子在线标记，设备表随进程结束自然清空
	for _, deviceID := range s.Handler.GetAllDeviceIDs() {
		s.Shadow.MarkOffline(deviceID)
	}

	if s.dtlsLis != nil {
		s.dtlsLis.Close()
	}

	if s.quicLis != nil {
		s.quicLis.Close()
	}
	if s.quicTrans != nil {
		s.quicTrans.Close()
	}

	if s.UDPConn != nil {
		s.UDPConn.Close()
	}

	s.Shadow.Stop()
	redis.UnregisterGateway(s.ID)
	s.wg.Wait()
}

// 派生PSK：不足16字节时用SHA-256派生固定长度密钥，保证DTLS-PSK可用
func derivePSK(psk string) []byte {
	if len(psk) >= 16 {
		return []byte(psk)
	}
	sum := sha256.Sum256([]byte(psk))
	return sum[:]
}

// 生成服务器ID
func generateServerID() string {
	return utils.GenerateServerID()
}
