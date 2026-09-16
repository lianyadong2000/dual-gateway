// internal/gateway/admin/admin.go —— 可视化管理后台后端
// 提供：实时指标聚合、设备影子/指令队列查询、压测任务编排、一键全功能验收
package admin

import (
	"bufio"
	_ "embed"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"dual-gateway/internal/config"
	"dual-gateway/internal/gateway/cend"
	"dual-gateway/internal/gateway/iot"
	"dual-gateway/internal/mq"
	redisPkg "dual-gateway/internal/redis"
	"dual-gateway/pkg/logger"
)

//go:embed web/index.html
var indexHTML string

// Page 返回管理后台页面
func (s *Server) Page(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(indexHTML))
}

// Server 管理后台服务
type Server struct {
	cfg   *config.Config
	log   logger.Logger
	cend  *cend.Server
	iot   *iot.Server
	mu    sync.Mutex
	tasks map[string]*TestTask
	last  *VerifyReport
	hist  []*TestTask // 已结束任务历史（cap 50）
}

// TestTask 压测任务
type TestTask struct {
	ID         string            `json:"id"`
	Kind       string            `json:"kind"` // iot / cend
	Name       string            `json:"name"`
	Status     string            `json:"status"` // running / done / failed / stopped
	Args       map[string]string `json:"args"`
	StartedAt  string            `json:"started_at"`
	FinishedAt string            `json:"finished_at"`
	Latest     string            `json:"latest"`
	Metrics    map[string]int64  `json:"metrics"`
	Log        []string          `json:"log"` // cap 300 行
	ExitErr    string            `json:"exit_err,omitempty"`
	Passed     *bool             `json:"passed,omitempty"`
	pid        int
	cmd        *exec.Cmd
}

// VerifyCase 验收用例结果
type VerifyCase struct {
	Name     string           `json:"name"`
	Status   string           `json:"status"` // pass / fail / skip / warn
	Duration string           `json:"duration"`
	Detail   string           `json:"detail"`
	Metrics  map[string]int64 `json:"metrics,omitempty"`
}

// VerifyReport 验收报告
type VerifyReport struct {
	ID         string       `json:"id"`
	StartedAt  string       `json:"started_at"`
	FinishedAt string       `json:"finished_at"`
	Cases      []VerifyCase `json:"cases"`
	Passed     int          `json:"passed"`
	Failed     int          `json:"failed"`
	Skipped    int          `json:"skipped"`
	AllPass    bool         `json:"all_pass"`
}

// New 创建管理后台
func New(cfg *config.Config, log logger.Logger, cs *cend.Server, is *iot.Server) *Server {
	return &Server{
		cfg:   cfg,
		log:   log,
		cend:  cs,
		iot:   is,
		tasks: make(map[string]*TestTask),
	}
}

// ============================ 路由 ============================

// Handler 管理后台 API 路由
func (s *Server) Handler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	path := strings.TrimPrefix(r.URL.Path, "/api/admin/")
	if path == "" {
		path = "overview"
	}
	switch path {
	case "overview":
		s.apiOverview(w)
	case "shadows":
		s.apiShadows(w, r)
	case "config":
		s.apiConfig(w)
	case "test":
		switch r.Method {
		case http.MethodPost:
			s.apiTestStart(w, r)
		case http.MethodGet:
			s.apiTestList(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case "test/stop":
		s.apiTestStop(w, r)
	case "verify":
		switch r.Method {
		case http.MethodPost:
			s.apiVerifyRun(w)
		case http.MethodGet:
			s.apiVerifyReport(w)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	default:
		http.Error(w, "unknown api", http.StatusNotFound)
	}
}

// ============================ 指标 ============================

func (s *Server) apiOverview(w http.ResponseWriter) {
	iotShadowStats := s.iot.Shadow.Stats()
	cendShadowStats := s.cend.Handler.Shadow.Stats()
	cmdStats := s.cend.Handler.CmdQueue.Stats()
	iotCmdStats := s.iot.CmdQueue.Stats()
	connAccepted, connRejected := s.iot.ConnectLimiter.Stats()

	// 影子 key 统计（Redis 实时扫描）
	shadowCount := int64(0)
	onlineCount := int64(0)
	queueCount := int64(0)
	if c := redisPkg.GetClient(); c != nil {
		ctx := redisPkg.Ctx()
		if keys, err := c.Keys(ctx, "shadow:dev:*").Result(); err == nil {
			shadowCount = int64(len(keys))
		}
		if keys, err := c.Keys(ctx, "shadow:online:*").Result(); err == nil {
			onlineCount = int64(len(keys))
		}
		if keys, err := c.Keys(ctx, "cmd:q:*").Result(); err == nil {
			queueCount = int64(len(keys))
		}
	}

	writeJSON(w, map[string]interface{}{
		"status":       "ok",
		"ts":           time.Now().Unix(),
		"cend_conns":   s.cend.ConnCount.Load(),
		"iot_devices":  s.iot.Handler.GetOnlineDeviceCount(),
		"iot_sessions": s.iot.Handler.GetSessionCount(),
		"quic_conns":   iot.GetQUICConnCount(),
		"iot_connect_cnt": s.iot.Handler.Metrics.ConnectCount.Load(),
		"iot_publish_cnt": s.iot.Handler.Metrics.PublishCount.Load(),
		"iot_packets":  s.iot.PacketCount.Load(),
		"iot_dropped":  s.iot.Dropped.Load(),
		"iot_errors":   s.iot.Handler.Metrics.ErrorCount.Load(),
		"redis_local":  redisPkg.IsLocalMode(),
		"kafka_available": mq.IsAvailable(),
		"shadow_total": shadowCount,
		"shadow_online": onlineCount,
		"cmd_queues":   queueCount,
		"shadow_events": iotShadowStats["shadow_events"] + cendShadowStats["shadow_events"],
		"shadow_preoccupy_ok": iotShadowStats["shadow_preoccupy_ok"] + cendShadowStats["shadow_preoccupy_ok"],
		"shadow_preoccupy_reject": iotShadowStats["shadow_preoccupy_reject"] + cendShadowStats["shadow_preoccupy_reject"],
		"shadow_rollback": iotShadowStats["shadow_rollback"] + cendShadowStats["shadow_rollback"],
		"shadow_offline": iotShadowStats["shadow_offline"] + cendShadowStats["shadow_offline"],
		"cmd_enqueued":  cmdStats["cmd_enqueued"] + iotCmdStats["cmd_enqueued"],
		"cmd_done":      cmdStats["cmd_done"] + iotCmdStats["cmd_done"],
		"cmd_duplicated": cmdStats["cmd_duplicated"] + iotCmdStats["cmd_duplicated"],
		"rate_connect_ok": connAccepted,
		"rate_connect_rejected": connRejected,
		"rate_accept_rejected": s.cend.Handler.AcceptLimiter.RejectedCount(),
		"rate_ip_tracked": s.iot.IPLimiter.ActiveIPCount(),
	})
}

// ============================ 影子 ============================

func (s *Server) apiShadows(w http.ResponseWriter, r *http.Request) {
	device := strings.TrimSpace(r.URL.Query().Get("device"))
	if device == "" {
		writeJSON(w, map[string]interface{}{"error": "device param required"})
		return
	}
	c := redisPkg.GetClient()
	if c == nil {
		writeJSON(w, map[string]interface{}{"device": device, "local_mode": true, "note": "Redis 本地降级模式，影子在内存中"})
		return
	}
	ctx := redisPkg.Ctx()
	shadow, _ := c.HGetAll(ctx, "shadow:dev:"+device).Result()
	online, _ := c.Get(ctx, "shadow:online:"+device).Result()
	queueLen, _ := c.LLen(ctx, "cmd:q:"+device).Result()
	queue, _ := c.LRange(ctx, "cmd:q:"+device, 0, 9).Result()
	iotDev, _ := c.HGet(ctx, "iot_devices", device).Result()

	writeJSON(w, map[string]interface{}{
		"device":      device,
		"shadow":      shadow,
		"online_ts":   online,
		"queue_len":   queueLen,
		"queue_head":  queue,
		"iot_devices": iotDev,
	})
}

// ============================ 配置 ============================

func (s *Server) apiConfig(w http.ResponseWriter) {
	cfg := s.cfg
	writeJSON(w, map[string]interface{}{
		"cend": map[string]interface{}{
			"host": cfg.Cend.Host, "port": cfg.Cend.Port,
			"max_connections": cfg.Cend.MaxConnections, "heartbeat_timeout": cfg.Cend.HeartbeatTimeout,
		},
		"iot": map[string]interface{}{
			"host": cfg.IoT.Host, "port": cfg.IoT.Port,
			"dtls_enabled": cfg.IoT.DTLSEnabled, "dtls_port": cfg.IoT.DTLSPort,
			"quic_enabled": cfg.IoT.QUICEnabled, "quic_port": cfg.IoT.QUICPort,
			"worker_count": cfg.IoT.WorkerCount, "session_timeout": cfg.IoT.SessionTimeout,
		},
		"redis":  map[string]interface{}{"addr": cfg.Redis.Addr, "local_mode": redisPkg.IsLocalMode(), "pool_size": cfg.Redis.PoolSize},
		"kafka":  map[string]interface{}{"available": mq.IsAvailable(), "brokers": cfg.Kafka.Brokers},
		"shadow": map[string]interface{}{"enabled": cfg.Shadow.Enabled, "offline_ttl": cfg.Shadow.OfflineTTLSeconds, "preoccupy_timeout": cfg.Shadow.PreoccupyTimeoutSeconds},
		"rate_limit": map[string]interface{}{
			"enabled": cfg.RateLimit.Enabled, "connect_rate": cfg.RateLimit.ConnectRate,
			"udp_per_ip_rate": cfg.RateLimit.UDPPerIPRate, "cend_accept_rate": cfg.RateLimit.CendAcceptRate,
		},
	})
}

// ============================ 压测任务 ============================

var metricLineRe = regexp.MustCompile(`(\w+)=(\d+)`)

// binaryPaths 返回压测二进制路径
func (s *Server) binaryPaths() (iotBin, cendBin string, err error) {
	ext := ""
	if runtime.GOOS == "windows" {
		ext = ".exe"
	}
	base := s.cfg.Admin.BuildDir
	if base == "" {
		base = "build"
	}
	iotBin = filepath.Join(base, "iot_loadtest"+ext)
	cendBin = filepath.Join(base, "cend_loadtest"+ext)
	for _, b := range []string{iotBin, cendBin} {
		if _, err2 := os.Stat(b); err2 != nil {
			return "", "", fmt.Errorf("压测二进制不存在: %s（请先构建: go build -o %s ./test/loadtest/iot/ 与 cend）", b, iotBin)
		}
	}
	return iotBin, cendBin, nil
}

// apiTestStart 启动压测任务
func (s *Server) apiTestStart(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Kind          string `json:"kind"` // iot / cend
		Transport     string `json:"transport"`
		Devices       int    `json:"devices"`
		Concurrency   int    `json:"concurrency"`
		KeepAlive     int    `json:"keep_alive"`
		PingInterval  int    `json:"ping_interval"`
		DeviceOffset  int    `json:"device_offset"`
		Mode          string `json:"mode"`
		Storm         bool   `json:"storm"`
		StormInterval int    `json:"storm_interval"`
		ConnS         int    `json:"conns"`
		Biz           bool   `json:"biz"`
		BizTarget     int    `json:"biz_target"`
		Tokens        string `json:"tokens"`
		Addr          string `json:"addr"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}

	iotBin, cendBin, err := s.binaryPaths()
	if err != nil {
		writeJSON(w, map[string]interface{}{"error": err.Error()})
		return
	}

	var args []string
	var kind, name string
	if req.Kind == "cend" {
		kind = "cend"
		name = "C端压测"
		if req.ConnS <= 0 {
			req.ConnS = 1000
		}
		if req.KeepAlive <= 0 {
			req.KeepAlive = 60
		}
		if req.BizTarget <= 0 {
			req.BizTarget = req.ConnS
		}
		tokens := req.Tokens
		if tokens == "" {
			tokens = "build/tokens_quic.txt"
		}
		args = append(args,
			"-conns", fmt.Sprint(req.ConnS),
			"-concurrency", fmt.Sprint(ifZero(req.Concurrency, 50)),
			"-tokens", tokens,
			"-keep-alive", fmt.Sprint(req.KeepAlive),
		)
		if req.Biz {
			args = append(args, "-biz", "-biz-target", fmt.Sprint(req.BizTarget),
				"-device-prefix", "device_", "-device-offset", fmt.Sprint(req.DeviceOffset))
		}
		if req.Addr != "" {
			args = append(args, "-addr", req.Addr)
		}
	} else {
		kind = "iot"
		transport := req.Transport
		if transport == "" {
			transport = "udp"
		}
		port := s.cfg.IoT.Port
		if transport == "quic" {
			port = s.cfg.IoT.QUICPort
		} else if transport == "dtls" {
			port = s.cfg.IoT.DTLSPort
		}
		name = "IoT压测(" + transport + ")"
		if req.Devices <= 0 {
			req.Devices = 1000
		}
		if req.KeepAlive <= 0 {
			req.KeepAlive = 60
		}
		args = append(args,
			"-transport", transport,
			"-addr", fmt.Sprintf("127.0.0.1:%d", port),
			"-devices", fmt.Sprint(req.Devices),
			"-concurrency", fmt.Sprint(ifZero(req.Concurrency, 100)),
			"-keep-alive", fmt.Sprint(req.KeepAlive),
		)
		if req.PingInterval > 0 {
			args = append(args, "-ping-interval", fmt.Sprint(req.PingInterval))
		}
		if req.Mode != "" {
			args = append(args, "-mode", req.Mode)
		}
		if req.DeviceOffset > 0 {
			args = append(args, "-device-offset", fmt.Sprint(req.DeviceOffset))
		}
		if req.Storm {
			args = append(args, "-storm", "-storm-interval", fmt.Sprint(ifZero(req.StormInterval, 30)))
		}
	}

	bin := iotBin
	if kind == "cend" {
		bin = cendBin
	}

	task := &TestTask{
		ID:        fmt.Sprintf("t%d", time.Now().UnixNano()%100000000),
		Kind:      kind,
		Name:      name,
		Status:    "running",
		Args:      map[string]string{"cmd": strings.Join(append([]string{bin}, args...), " ")},
		StartedAt: time.Now().Format("2006-01-02 15:04:05"),
		Metrics:   map[string]int64{},
	}
	s.mu.Lock()
	s.tasks[task.ID] = task
	s.mu.Unlock()

	go s.runTask(task, bin, args)

	writeJSON(w, map[string]interface{}{"id": task.ID, "status": "running", "cmd": task.Args["cmd"]})
}

// runTask 执行子进程并实时解析指标
func (s *Server) runTask(task *TestTask, bin string, args []string) {
	cmd := exec.Command(bin, args...)
	task.cmd = cmd
	cmd.Env = os.Environ()

	stdout, err := cmd.StdoutPipe()
	if err == nil {
		cmd.Stderr = cmd.Stdout
	}
	// 合并输出
	if err := cmd.Start(); err != nil {
		s.finishTask(task, "failed", err.Error())
		return
	}
	task.pid = cmd.Process.Pid

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		s.mu.Lock()
		task.Log = append(task.Log, line)
		if len(task.Log) > 300 {
			task.Log = task.Log[len(task.Log)-300:]
		}
		// 解析 [Ns] key=value 行
		if idx := strings.Index(line, "] "); idx >= 0 {
			segment := line[idx+2:]
			task.Latest = segment
			for _, m := range metricLineRe.FindAllStringSubmatch(segment, -1) {
				var v int64
				fmt.Sscanf(m[2], "%d", &v)
				task.Metrics[m[1]] = v
			}
		}
		s.mu.Unlock()
	}
	_ = cmd.Wait()

	s.mu.Lock()
	exited := !cmd.ProcessState.Success()
	code := cmd.ProcessState.ExitCode()
	s.mu.Unlock()

	if exited {
		s.finishTask(task, "failed", fmt.Sprintf("exit code=%d", code))
	} else {
		s.finishTask(task, "done", "")
	}
}

func (s *Server) finishTask(task *TestTask, status, errMsg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	task.Status = status
	task.FinishedAt = time.Now().Format("2006-01-02 15:04:05")
	task.ExitErr = errMsg
	s.hist = append([]*TestTask{task}, s.hist...)
	if len(s.hist) > 50 {
		s.hist = s.hist[:50]
	}
}

func (s *Server) apiTestList(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	s.mu.Lock()
	defer s.mu.Unlock()
	if id != "" {
		if t, ok := s.tasks[id]; ok {
			writeJSON(w, t)
			return
		}
		writeJSON(w, map[string]interface{}{"error": "task not found"})
		return
	}
	// 运行中 + 历史合并
	type row struct {
		ID, Kind, Name, Status string
		StartedAt, Latest      string
		Metrics                map[string]int64
		FinishedAt             string
	}
	rows := make([]row, 0, len(s.tasks)+len(s.hist))
	for _, t := range s.tasks {
		if t.Status == "running" {
			rows = append(rows, row{t.ID, t.Kind, t.Name, t.Status, t.StartedAt, t.Latest, cloneMetrics(t.Metrics), ""})
		}
	}
	for _, t := range s.hist {
		rows = append(rows, row{t.ID, t.Kind, t.Name, t.Status, t.StartedAt, t.Latest, cloneMetrics(t.Metrics), t.FinishedAt})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].StartedAt > rows[j].StartedAt })
	writeJSON(w, rows)
}

func cloneMetrics(m map[string]int64) map[string]int64 {
	c := make(map[string]int64, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}

func (s *Server) apiTestStop(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	s.mu.Lock()
	t, ok := s.tasks[id]
	s.mu.Unlock()
	if !ok || t.Status != "running" {
		writeJSON(w, map[string]interface{}{"error": "task not running"})
		return
	}
	if t.cmd != nil && t.cmd.Process != nil {
		_ = t.cmd.Process.Kill()
	}
	s.finishTask(t, "stopped", "manually stopped")
	writeJSON(w, map[string]interface{}{"id": id, "status": "stopped"})
}

// ============================ 一键验收 ============================

func (s *Server) apiVerifyRun(w http.ResponseWriter) {
	s.mu.Lock()
	if s.last != nil && s.last.FinishedAt == "" {
		s.mu.Unlock()
		writeJSON(w, map[string]interface{}{"error": "验收已在进行中，请等待完成"})
		return
	}
	report := &VerifyReport{
		ID:        fmt.Sprintf("v%d", time.Now().UnixNano()%100000000),
		StartedAt: time.Now().Format("2006-01-02 15:04:05"),
	}
	s.last = report
	s.mu.Unlock()

	go s.runVerify(report)
	writeJSON(w, map[string]interface{}{"id": report.ID, "status": "running"})
}

func (s *Server) apiVerifyReport(w http.ResponseWriter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.last == nil {
		writeJSON(w, map[string]interface{}{"error": "no report yet"})
		return
	}
	writeJSON(w, s.last)
}

// runVerify 串行执行预置验收用例
func (s *Server) runVerify(rp *VerifyReport) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	add := func(name string, status, detail string, d time.Duration, m map[string]int64) {
		c := VerifyCase{Name: name, Status: status, Detail: detail, Duration: d.Round(time.Millisecond).String()}
		if len(m) > 0 {
			c.Metrics = m
		}
		rp.Cases = append(rp.Cases, c)
		switch status {
		case "pass":
			rp.Passed++
		case "fail":
			rp.Failed++
		default:
			rp.Skipped++
		}
	}

	// 1. 健康检查
	t0 := time.Now()
	overview := s.overviewSnapshot()
	ok := overview["status"] == "ok"
	detail := fmt.Sprintf("cend_conns=%v iot_devices=%v quic_conns=%v", overview["cend_conns"], overview["iot_devices"], overview["quic_conns"])
	if !ok {
		add("健康检查 /health", "fail", "health status != ok", time.Since(t0), nil)
	} else {
		add("健康检查 /health", "pass", detail, time.Since(t0), nil)
	}

	// 2. Redis 状态
	t0 = time.Now()
	if redisPkg.IsLocalMode() {
		add("Redis 连接", "warn", "Redis 本地降级模式：影子跨进程/指令发布订阅不可用，验收功能受限", time.Since(t0), nil)
	} else {
		add("Redis 连接", "pass", fmt.Sprintf("addr=%s", s.cfg.Redis.Addr), time.Since(t0), nil)
	}

	// 3. C 端连接
	runAndAssert := func(caseName string, t *TestTask, expect map[string]int64, detail string) {
		<-ctx.Done()
	}
	_ = runAndAssert

	// 3. C 端连接验收
	verifyCend(ctx, rp, add, s)

	// 4. IoT UDP
	verifyIoT(ctx, rp, add, s, "udp")

	// 5. IoT QUIC
	verifyIoT(ctx, rp, add, s, "quic")

	// 6. 指令闭环（C 端对在线 QUIC 设备下发 → 断言 IoT 侧 cmd 计数）
	verifyCmdLoop(ctx, rp, add, s)

	// 7. 影子验证
	verifyShadow(ctx, rp, add, s)

	rp.FinishedAt = time.Now().Format("2006-01-02 15:04:05")
	rp.AllPass = rp.Failed == 0 && rp.Passed > 0
	s.mu.Lock()
	s.last = rp
	s.mu.Unlock()
}

// verifyCend C 端连接验收
func verifyCend(ctx context.Context, rp *VerifyReport, add func(string, string, string, time.Duration, map[string]int64), s *Server) {
	t0 := time.Now()
	_, cendBin, err := s.binaryPaths()
	if err != nil {
		add("C端连接验收", "skip", err.Error(), time.Since(t0), nil)
		return
	}
	args := []string{"-conns", "500", "-concurrency", "50", "-tokens", "build/tokens_quic.txt", "-keep-alive", "25"}
	task := s.launchForVerify(cendBin, args)
	metrics := s.waitTask(ctx, task, 90*time.Second)
	if metrics == nil {
		s.killTask(task)
		add("C端连接验收", "fail", "任务未在 90s 内完成或超时", time.Since(t0), nil)
		return
	}
	total := metrics["dialed"]
	authOK := metrics["auth_ok"]
	connFail := metrics["conn_fail"]
	pass := total > 0 && authOK*100 >= total*99 && connFail == 0
	detail := fmt.Sprintf("dialed=%d auth_ok=%d conn_fail=%d", total, authOK, connFail)
	if pass {
		add("C端连接验收", "pass", detail, time.Since(t0), metrics)
	} else {
		add("C端连接验收", "fail", detail, time.Since(t0), metrics)
	}
}

// verifyIoT IoT 连接验收
func verifyIoT(ctx context.Context, rp *VerifyReport, add func(string, string, string, time.Duration, map[string]int64), s *Server, transport string) {
	t0 := time.Now()
	iotBin, _, err := s.binaryPaths()
	if err != nil {
		add("IoT连接验收("+transport+")", "skip", err.Error(), time.Since(t0), nil)
		return
	}
	port := s.cfg.IoT.Port
	if transport == "quic" {
		port = s.cfg.IoT.QUICPort
	}
	args := []string{"-transport", transport, "-addr", fmt.Sprintf("127.0.0.1:%d", port),
		"-devices", "500", "-concurrency", "100", "-keep-alive", "25", "-ping-interval", "5"}
	task := s.launchForVerify(iotBin, args)
	metrics := s.waitTask(ctx, task, 120*time.Second)
	if metrics == nil {
		s.killTask(task)
		add("IoT连接验收("+transport+")", "fail", "任务未在 120s 内完成或超时", time.Since(t0), nil)
		return
	}
	sent := metrics["connect_sent"]
	connack := metrics["connack"]
	active := metrics["active"]
	errs := metrics["errors"]
	// sent 含 UDP CONNECT 重传（> 设备数），以 connack/active 为准，容忍 10% 重传
	pass := sent > 0 && connack*100 >= sent*90 && errs == 0 && active*100 >= sent*90
	detail := fmt.Sprintf("connect_sent=%d connack=%d active=%d errors=%d", sent, connack, active, errs)
	if pass {
		add("IoT连接验收("+transport+")", "pass", detail, time.Since(t0), metrics)
	} else {
		add("IoT连接验收("+transport+")", "fail", detail, time.Since(t0), metrics)
	}
}

// verifyCmdLoop 指令闭环：IoT 在线设备 + C 端 biz 下发 → 断言 cmd 计数
func verifyCmdLoop(ctx context.Context, rp *VerifyReport, add func(string, string, string, time.Duration, map[string]int64), s *Server) {
	t0 := time.Now()
	iotBin, cendBin, err := s.binaryPaths()
	if err != nil {
		add("指令闭环验收", "skip", err.Error(), time.Since(t0), nil)
		return
	}
	// 动态设备段（<200000，避开 cend_loadtest 的 %200000 取模），并清理该段影子残留，
	// 避免上轮验收 charge.start 成功后影子 charging 残留导致 409 拒发
	offset := 150000 + (int(time.Now().Unix())%50)*100
	if c := redisPkg.GetClient(); c != nil {
		ctx2 := redisPkg.Ctx()
		for _, pat := range []string{
			fmt.Sprintf("shadow:dev:device_%d*", offset),
			fmt.Sprintf("shadow:online:device_%d*", offset),
			fmt.Sprintf("cmd:q:device_%d*", offset),
		} {
			if keys, err := c.Keys(ctx2, pat).Result(); err == nil && len(keys) > 0 {
				c.Del(ctx2, keys...)
			}
		}
	}
	// 1) 起 IoT QUIC 设备（在线）
	iotArgs := []string{"-transport", "quic", "-addr", fmt.Sprintf("127.0.0.1:%d", s.cfg.IoT.QUICPort),
		"-devices", "100", "-device-offset", fmt.Sprint(offset), "-concurrency", "50", "-keep-alive", "70", "-ping-interval", "5"}
	iotTask := s.launchForVerify(iotBin, iotArgs)

	// 2) 等设备连接完成（25s）
	select {
	case <-ctx.Done():
		return
	case <-time.After(25 * time.Second):
	}

	// 3) C 端下发指令（对前 5 台设备）
	cendArgs := []string{"-conns", "5", "-concurrency", "1", "-tokens", "build/tokens_quic.txt",
		"-biz", "-biz-target", "5", "-device-prefix", "device_", "-device-offset", fmt.Sprint(offset), "-keep-alive", "20"}
	cendTask := s.launchForVerify(cendBin, cendArgs)
	cendMetrics := s.waitTask(ctx, cendTask, 60*time.Second)
	if cendMetrics == nil {
		add("指令闭环验收", "fail", "C端指令任务超时", time.Since(t0), nil)
		s.killTask(iotTask)
		return
	}

	// 4) 再等 IoT 侧收到指令（15s）
	select {
	case <-ctx.Done():
		return
	case <-time.After(15 * time.Second):
	}

	// 5) 读 IoT 任务最新指标
	s.mu.Lock()
	iotMetrics := cloneMetrics(iotTask.Metrics)
	s.mu.Unlock()
	_ = s.killTask(iotTask)

	bizSent := cendMetrics["biz_sent"]
	bizOK := cendMetrics["biz_ok"]
	biz409 := cendMetrics["biz_409"]
	cmdCnt := iotMetrics["cmd"]
	cmdAck := iotMetrics["cmdack"]

	// 断言：C端业务有响应（ok 或 409 均说明链路通）+ IoT 侧收到指令
	detail := fmt.Sprintf("biz_sent=%d biz_ok=%d biz_409=%d iot_cmd=%d iot_cmdack=%d", bizSent, bizOK, biz409, cmdCnt, cmdAck)
	pass := bizSent > 0 && (bizOK > 0 || biz409 > 0) && cmdCnt > 0 && cmdAck > 0
	if pass {
		add("指令闭环验收", "pass", detail, time.Since(t0), map[string]int64{"biz_ok": bizOK, "biz_409": biz409, "cmd": cmdCnt, "cmdack": cmdAck})
	} else {
		add("指令闭环验收", "fail", detail, time.Since(t0), nil)
	}
}

// verifyShadow 影子状态验证
func verifyShadow(ctx context.Context, rp *VerifyReport, add func(string, string, string, time.Duration, map[string]int64), s *Server) {
	t0 := time.Now()
	c := redisPkg.GetClient()
	if c == nil {
		add("设备影子验证", "skip", "Redis 本地模式，跳过", time.Since(t0), nil)
		return
	}
	keys, err := c.Keys(redisPkg.Ctx(), "shadow:dev:*").Result()
	if err != nil {
		add("设备影子验证", "fail", "查询 shadow:dev:* 失败: "+err.Error(), time.Since(t0), nil)
		return
	}
	pass := len(keys) > 0
	detail := fmt.Sprintf("影子设备数=%d（shadow:dev:* 键数）", len(keys))
	if pass {
		add("设备影子验证", "pass", detail, time.Since(t0), map[string]int64{"shadow_devices": int64(len(keys))})
	} else {
		add("设备影子验证", "fail", "无影子记录（需先有设备连接过）", time.Since(t0), nil)
	}
}

// ============================ 工具函数 ============================

func (s *Server) launchForVerify(bin string, args []string) *TestTask {
	task := &TestTask{
		ID:        fmt.Sprintf("t%d", time.Now().UnixNano()%100000000),
		Kind:      "verify",
		Name:      "验收子任务",
		Status:    "running",
		StartedAt: time.Now().Format("2006-01-02 15:04:05"),
		Metrics:   map[string]int64{},
	}
	s.mu.Lock()
	s.tasks[task.ID] = task
	s.mu.Unlock()
	go s.runTask(task, bin, args)
	return task
}

// waitTask 等待任务结束并返回最终指标；超时返回 nil
func (s *Server) waitTask(ctx context.Context, t *TestTask, timeout time.Duration) map[string]int64 {
	deadline := time.After(timeout)
	for {
		s.mu.Lock()
		status := t.Status
		metrics := cloneMetrics(t.Metrics)
		s.mu.Unlock()
		if status != "running" {
			if status == "done" {
				return metrics
			}
			return metrics // failed / stopped 也返回当前指标由调用方判定
		}
		select {
		case <-ctx.Done():
			return nil
		case <-deadline:
			return nil
		case <-time.After(2 * time.Second):
		}
	}
}

func (s *Server) killTask(t *TestTask) bool {
	s.mu.Lock()
	cmd := t.cmd
	s.mu.Unlock()
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
		return true
	}
	return false
}

func (s *Server) overviewSnapshot() map[string]interface{} {
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/health", s.cfg.Monitor.Port), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return map[string]interface{}{"status": "error"}
	}
	defer resp.Body.Close()
	var m map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&m)
	if m == nil {
		m = map[string]interface{}{"status": "unknown"}
	}
	return m
}

func ifZero(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
