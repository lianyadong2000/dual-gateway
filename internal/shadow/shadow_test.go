package shadow

import (
	"os"
	"testing"

	"dual-gateway/internal/config"
	gredis "dual-gateway/internal/redis"
)

// 测试入口：初始化Redis（本机6379已启动则走真实Redis+Lua路径；失败自动降级本地模式）
func TestMain(m *testing.M) {
	gredis.Init("127.0.0.1:6379", "", 0, 10)
	if !gredis.IsLocalMode() {
		// 清理历史测试遗留（Redis模式持久化）
		ctx := gredis.Ctx()
		for _, id := range []string{"dev_001", "dev_002", "dev_003"} {
			gredis.GetClient().Del(ctx, "shadow:dev:"+id, "shadow:online:"+id)
		}
	}
	os.Exit(m.Run())
}

func TestCanTransition(t *testing.T) {
	cases := []struct {
		from, to string
		want     bool
	}{
		{StateIdle, StatePreoccupy, true},
		{StatePreoccupy, StateCharging, true},
		{StateCharging, StateIdle, true},
		{StatePreoccupy, StateIdle, true},      // 超时回滚
		{StateOffline, StatePreoccupy, true},   // 离线排队
		{StateCharging, StatePreoccupy, false}, // 充电中不可抢占
		{StateFault, StatePreoccupy, false},    // 故障不可抢占
		{StateIdle, StateCharging, true},       // 容错直报
		{StateFault, StateIdle, true},          // 恢复
		{StateOffline, StateIdle, true},        // 重连上报
	}
	for _, c := range cases {
		if got := CanTransition(c.from, c.to); got != c.want {
			t.Errorf("CanTransition(%s,%s)=%v want %v", c.from, c.to, got, c.want)
		}
	}
}

func TestLegalReport(t *testing.T) {
	cases := []struct {
		cur, to string
		want    bool
	}{
		{StatePreoccupy, StateCharging, true}, // 桩确认充电开始
		{StateCharging, StateIdle, true},      // 充电结束
		{StatePreoccupy, StateIdle, true},     // 桩拒绝
		{StateIdle, StatePreoccupy, false},    // 桩不会主动上报预占
		{StateCharging, StatePreoccupy, false},
	}
	for _, c := range cases {
		if got := legalReport(c.cur, c.to); got != c.want {
			t.Errorf("legalReport(%s,%s)=%v want %v", c.cur, c.to, got, c.want)
		}
	}
}

// 本地降级模式：状态机与原子抢占行为（Redis 不可用时走同一套业务逻辑）
func TestManagerLocalMode(t *testing.T) {
	m := NewManager(testCfg())
	m.Enabled = true

	// 首次查询：不存在 → offline
	s, err := m.Get("dev_001")
	if err != nil {
		t.Fatal(err)
	}
	if s.State != StateOffline || s.Online {
		t.Errorf("fresh shadow = %+v, want offline", s)
	}

	// 上线（Touch 初始化 + 心跳）
	if err := m.Touch("dev_001", "gw-1"); err != nil {
		t.Fatal(err)
	}
	if err := m.ReportHeartbeat("dev_001", ""); err != nil {
		t.Fatal(err)
	}

	// 原子抢占成功
	s, err = m.TryPreoccupy("dev_001")
	if err != nil {
		t.Fatal(err)
	}
	if s.State != StatePreoccupy {
		t.Errorf("after preoccupy state=%s", s.State)
	}
	v1 := s.Version

	// 二次抢占必须失败（防双人抢桩）
	if _, err := m.TryPreoccupy("dev_001"); err == nil {
		t.Error("second preoccupy should fail")
	}

	// 桩上报充电开始
	if err := m.ReportState("dev_001", StateCharging, "gw-1"); err != nil {
		t.Fatal(err)
	}
	s, _ = m.Get("dev_001")
	if s.State != StateCharging {
		t.Errorf("after charging report state=%s", s.State)
	}
	if s.Version <= v1 {
		t.Errorf("version should increase: %d -> %d", v1, s.Version)
	}

	// 充电结束 → idle
	if err := m.ReportState("dev_001", StateIdle, "gw-1"); err != nil {
		t.Fatal(err)
	}
	s, _ = m.Get("dev_001")
	if s.State != StateIdle {
		t.Errorf("after idle report state=%s", s.State)
	}

	// 非法上报：idle 桩直接上报 preoccupy（桩不会发，应拒绝）
	if err := m.ReportState("dev_001", StatePreoccupy, "gw-1"); err != nil {
		// 允许报错（状态保持 idle）
	} else {
		s, _ = m.Get("dev_001")
		if s.State == StatePreoccupy {
			t.Error("illegal report should be rejected")
		}
	}
}

func TestManagerRollback(t *testing.T) {
	m := NewManager(testCfg())
	m.Enabled = true
	if err := m.Touch("dev_002", "gw-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.TryPreoccupy("dev_002"); err != nil {
		t.Fatal(err)
	}
	if err := m.RollbackPreoccupy("dev_002"); err != nil {
		t.Fatal(err)
	}
	s, _ := m.Get("dev_002")
	if s.State != StateIdle {
		t.Errorf("after rollback state=%s", s.State)
	}
}

func TestManagerMarkOffline(t *testing.T) {
	m := NewManager(testCfg())
	m.Enabled = true
	if err := m.Touch("dev_003", "gw-1"); err != nil {
		t.Fatal(err)
	}
	if err := m.MarkOffline("dev_003"); err != nil {
		t.Fatal(err)
	}
	s, _ := m.Get("dev_003")
	if s.State != StateOffline {
		t.Errorf("after offline state=%s", s.State)
	}
	// 离线可排队预占
	if _, err := m.TryPreoccupy("dev_003"); err != nil {
		t.Errorf("offline preoccupy should work: %v", err)
	}
}

// 未启用影子：所有操作降级为空操作/离线查询
func TestManagerDisabled(t *testing.T) {
	m := NewManager(&config.ShadowConfig{Enabled: false})
	if err := m.Touch("dev_x", "gw"); err != nil {
		t.Fatal(err)
	}
	s, err := m.TryPreoccupy("dev_x")
	if err != nil || s != nil {
		t.Errorf("disabled preoccupy: s=%v err=%v, want nil,nil", s, err)
	}
	s, _ = m.Get("dev_x")
	if s == nil || s.State != StateOffline {
		t.Errorf("disabled get = %+v, want offline", s)
	}
}

func testCfg() *config.ShadowConfig {
	return &config.ShadowConfig{
		Enabled:                 true,
		OfflineTTLSeconds:       120,
		PreoccupyTimeoutSeconds: 60,
		CmdTTLSeconds:           86400,
		MaxCmdBatch:             10,
	}
}
