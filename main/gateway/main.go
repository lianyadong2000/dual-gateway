// main/gateway/main.go —— 双模网关（C端WebSocket + IoT MQTT-SN/DTLS）入口
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"dual-gateway/internal/auth"
	"dual-gateway/internal/gateway/admin"
	"dual-gateway/internal/config"
	"dual-gateway/internal/gateway/cend"
	"dual-gateway/internal/gateway/iot"
	"dual-gateway/internal/mq"
	"dual-gateway/internal/redis"
	"dual-gateway/pkg/logger"
)

var (
	version = "1.0.0"
)

func main() {
	// 解析命令行参数
	configFile := flag.String("config", "config/config.yml", "config file path")
	showVersion := flag.Bool("version", false, "show version")
	flag.Parse()

	if *showVersion {
		fmt.Printf("Dual Gateway v%s\n", version)
		return
	}

	// 加载配置
	cfg, err := config.Load(*configFile)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 初始化日志
	loggerInstance := logger.New(cfg.Log.Level, cfg.Log.Format)
	loggerInstance.Info("Starting Dual Gateway", "version", version)

	// 设置Go运行时参数
	runtime.GOMAXPROCS(cfg.Runtime.MaxProcs)

	// 确保JWT密钥有效（无效时自动生成，便于开发/单机部署）
	if _, err := auth.EnsureRSAKeys(&cfg.Auth.JWTPrivateKey, &cfg.Auth.JWTPublicKey, cfg.Auth.JWTIssuer); err != nil {
		loggerInstance.Fatal("Failed to ensure JWT keys", "error", err)
	} else {
		loggerInstance.Info("JWT keys ready", "issuer", cfg.Auth.JWTIssuer, "ttl_seconds", cfg.Auth.JWTTTLSeconds)
	}

	// 初始化Redis（失败自动降级为本地内存模式）
	if err := redis.Init(cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.DB, cfg.Redis.PoolSize); err != nil {
		loggerInstance.Warn("Redis init failed, using degraded mode", "error", err)
	} else if redis.IsLocalMode() {
		loggerInstance.Warn("Redis unavailable, running in local memory mode")
	} else {
		loggerInstance.Info("Redis connected", "addr", cfg.Redis.Addr)
	}

	// 初始化Kafka（失败自动降级为丢弃模式）
	if err := mq.Init(cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.Kafka.GroupID); err != nil {
		loggerInstance.Warn("Kafka init failed, using degraded mode", "error", err)
	} else if !mq.IsAvailable() {
		loggerInstance.Warn("Kafka unavailable, messages will be dropped (counted)")
	} else {
		loggerInstance.Info("Kafka connected", "brokers", cfg.Kafka.Brokers)
	}

	// 创建C端网关
	cendServer := cend.NewServer(cfg, loggerInstance)
	if err := cendServer.Start(); err != nil {
		loggerInstance.Fatal("Failed to start C-end gateway", "error", err)
	}

	// 创建IoT网关
	iotServer := iot.NewServer(cfg, loggerInstance)
	if err := iotServer.Start(); err != nil {
		loggerInstance.Fatal("Failed to start IoT gateway", "error", err)
	}

	// 创建管理后台（可视化管理 + 压测编排 + 全功能验收）
	var adminSrv *admin.Server
	if cfg.Admin.Enabled {
		adminSrv = admin.New(cfg, loggerInstance, cendServer, iotServer)
		loggerInstance.Info("Admin dashboard enabled", "path", "/admin", "api", "/api/admin")
	}

	// 启动监控服务
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		if adminSrv != nil {
			mux.HandleFunc("/admin", adminSrv.Page)
			mux.HandleFunc("/admin/", adminSrv.Page)
			mux.HandleFunc("/api/admin/", adminSrv.Handler)
		}
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)

			iotShadowStats := iotServer.Shadow.Stats()
			cendShadowStats := cendServer.Handler.Shadow.Stats()
			cmdStats := cendServer.Handler.CmdQueue.Stats()
			iotCmdStats := iotServer.CmdQueue.Stats()
			connAccepted, connRejected := iotServer.ConnectLimiter.Stats()

			json.NewEncoder(w).Encode(map[string]interface{}{
				"status":             "ok",
				"cend_conns":         cendServer.ConnCount.Load(),
				"iot_devices":        iotServer.Handler.GetOnlineDeviceCount(),
				"iot_sessions":       iotServer.Handler.GetSessionCount(),
				"iot_connect_cnt":    iotServer.Handler.Metrics.ConnectCount.Load(),
				"iot_publish_cnt":    iotServer.Handler.Metrics.PublishCount.Load(),
				"iot_packets":        iotServer.PacketCount.Load(),
				"iot_dropped":        iotServer.Dropped.Load(),
				"iot_errors":         iotServer.Handler.Metrics.ErrorCount.Load(),
				"quic_conns":         iot.GetQUICConnCount(),
				"redis_local":        redis.IsLocalMode(),
				"kafka_available":    mq.IsAvailable(),
				"shadow_events":      iotShadowStats["shadow_events"] + cendShadowStats["shadow_events"],
				"shadow_preoccupy_ok": iotShadowStats["shadow_preoccupy_ok"] + cendShadowStats["shadow_preoccupy_ok"],
				"shadow_preoccupy_reject": iotShadowStats["shadow_preoccupy_reject"] + cendShadowStats["shadow_preoccupy_reject"],
				"shadow_rollback":    iotShadowStats["shadow_rollback"] + cendShadowStats["shadow_rollback"],
				"shadow_offline":     iotShadowStats["shadow_offline"] + cendShadowStats["shadow_offline"],
				"cmd_enqueued":       cmdStats["cmd_enqueued"] + iotCmdStats["cmd_enqueued"],
				"cmd_done":           cmdStats["cmd_done"] + iotCmdStats["cmd_done"],
				"cmd_duplicated":     cmdStats["cmd_duplicated"] + iotCmdStats["cmd_duplicated"],
				"rate_connect_ok":    connAccepted,
				"rate_connect_rejected": connRejected,
				"rate_accept_rejected": cendServer.Handler.AcceptLimiter.RejectedCount(),
				"rate_ip_tracked":    iotServer.IPLimiter.ActiveIPCount(),
			})
		})

		monitorAddr := fmt.Sprintf("%s:%d", cfg.Monitor.Host, cfg.Monitor.Port)
		loggerInstance.Info("Monitor server started", "addr", monitorAddr)

		if err := http.ListenAndServe(monitorAddr, mux); err != nil {
			loggerInstance.Error("Monitor server error", "error", err)
		}
	}()

	// 等待退出信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	loggerInstance.Info("Received signal, shutting down...", "signal", sig)

	// 优雅关闭
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		cendServer.Stop()
		iotServer.Stop()
		mq.Close()
		close(done)
	}()

	select {
	case <-done:
		loggerInstance.Info("Shutdown complete")
	case <-shutdownCtx.Done():
		loggerInstance.Warn("Shutdown timeout")
	}
}
