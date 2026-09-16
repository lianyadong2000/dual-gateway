package command

import (
	"os"
	"testing"

	"dual-gateway/internal/config"
	gredis "dual-gateway/internal/redis"
)

// 测试入口：初始化Redis（本机6379已启动则走真实Redis路径；失败自动降级本地模式）
func TestMain(m *testing.M) {
	gredis.Init("127.0.0.1:6379", "", 0, 10)
	if !gredis.IsLocalMode() {
		// 清理历史测试遗留（Redis模式持久化）
		ctx := gredis.Ctx()
		for _, k := range []string{"cmd:q:dev_001", "cmd:q:dev_002", "cmd:q:dev_003"} {
			gredis.GetClient().Del(ctx, k)
		}
	}
	os.Exit(m.Run())
}

func testCfg() *config.ShadowConfig {
	return &config.ShadowConfig{
		Enabled:           true,
		OfflineTTLSeconds: 120,
		CmdTTLSeconds:     86400,
		MaxCmdBatch:       10,
	}
}

// 本地降级模式：入队→拉取→确认闭环
func TestQueueLocalLifecycle(t *testing.T) {
	q := NewQueue(testCfg())

	// 入队
	cmd, err := q.Enqueue("dev_001", "charge.start", map[string]interface{}{"user_id": "u1"})
	if err != nil {
		t.Fatal(err)
	}
	if cmd == nil || cmd.CmdID == "" || cmd.Type != "charge.start" {
		t.Fatalf("enqueue result: %+v", cmd)
	}

	// 拉取
	cmds, err := q.Fetch("dev_001", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(cmds) != 1 || cmds[0].CmdID != cmd.CmdID {
		t.Fatalf("fetch = %d cmds, want 1", len(cmds))
	}

	// 幂等：同一指令再次入队（模拟重发）→ 应拒绝重复
	if _, err := q.Enqueue("dev_001", "charge.start", nil); err != nil {
		// CmdID 每次新生成，不会重复——这是设计（指令级幂等靠全局唯一ID）
		// 真正幂等测试用 EnqueueWithID
	}
	if err := q.EnqueueWithID("dev_001", cmd.CmdID, "charge.start", nil); err == nil {
		t.Error("EnqueueWithID same cmdID should be rejected as duplicate")
	}

	// 确认完成 → 删除 cmd_1；cmd_2（第二个Enqueue）仍在队列
	if err := q.MarkDone("dev_001", cmd.CmdID); err != nil {
		t.Fatal(err)
	}
	n, _ := q.PendingCount("dev_001")
	if n != 1 {
		t.Errorf("pending after done = %d, want 1 (cmd_2 remains)", n)
	}
	// 确认 cmd_2 → 队列清空
	cmds, _ = q.Fetch("dev_001", 0)
	for _, c := range cmds {
		q.MarkDone("dev_001", c.CmdID)
	}
	n, _ = q.PendingCount("dev_001")
	if n != 0 {
		t.Errorf("pending after all done = %d, want 0", n)
	}
}

// 拉取不移除：未确认的指令可重拉（桩收包丢失后重试）
func TestQueueFetchKeepsPending(t *testing.T) {
	q := NewQueue(testCfg())
	cmd, _ := q.Enqueue("dev_002", "query", nil)

	if _, err := q.Fetch("dev_002", 0); err != nil {
		t.Fatal(err)
	}
	cmds, err := q.Fetch("dev_002", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(cmds) != 1 || cmds[0].CmdID != cmd.CmdID {
		t.Fatalf("fetch should keep pending command, got %d", len(cmds))
	}
}

// 批量上限
func TestQueueBatchLimit(t *testing.T) {
	q := NewQueue(testCfg())
	for i := 0; i < 5; i++ {
		if _, err := q.Enqueue("dev_003", "query", nil); err != nil {
			t.Fatal(err)
		}
	}
	cmds, _ := q.Fetch("dev_003", 3)
	if len(cmds) != 3 {
		t.Errorf("batch limit fetch = %d, want 3", len(cmds))
	}
}

// 未启用队列：Enqueue 返回 nil 不报错（网关降级）
func TestQueueDisabled(t *testing.T) {
	q := NewQueue(&config.ShadowConfig{Enabled: false})
	cmd, err := q.Enqueue("dev_x", "query", nil)
	if err != nil || cmd != nil {
		t.Errorf("disabled enqueue: cmd=%v err=%v, want nil,nil", cmd, err)
	}
}
