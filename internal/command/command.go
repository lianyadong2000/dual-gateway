// Package command 实现 IoT 指令队列：C端指令在桩休眠/离线时写入 Redis List，
// 桩唤醒后主动拉取执行；指令全局唯一ID + 幂等去重，防止重复执行（重复计费风险）。
package command

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"dual-gateway/internal/config"
	gredis "dual-gateway/internal/redis"
	"dual-gateway/pkg/utils"
)

// 指令状态
const (
	StatusQueued    = "queued"     // 已入队，待桩拉取
	StatusDelivered = "delivered"  // 已下发（等待确认）
	StatusDone      = "done"       // 桩已确认执行完成
	StatusExpired   = "expired"    // TTL过期作废
)

// 指令
type Command struct {
	CmdID     string                 `json:"cmd_id"`
	DeviceID  string                 `json:"device_id"`
	Type      string                 `json:"type"`
	Params    map[string]interface{} `json:"params,omitempty"`
	CreatedAt int64                  `json:"created_at"`
	TTL       int64                  `json:"ttl"`
	Status    string                 `json:"status"`
}

// 队列管理器
type Queue struct {
	cmdTTL   time.Duration
	maxBatch int
	enabled  bool

	// 统计
	Enqueued  int64
	Duplicated int64
	Fetched   int64
	Done      int64
	Expired   int64

	// 本地降级
	mu        sync.Mutex
	local     map[string][]*Command
	localDedup map[string]int64 // cmdID -> createdAt
}

// 创建指令队列
func NewQueue(cfg *config.ShadowConfig) *Queue {
	q := &Queue{
		cmdTTL:   time.Duration(cfg.CmdTTLSeconds) * time.Second,
		maxBatch: cfg.MaxCmdBatch,
		enabled:  cfg.Enabled,
		local:    make(map[string][]*Command),
		localDedup: make(map[string]int64),
	}
	if q.cmdTTL <= 0 {
		q.cmdTTL = 24 * time.Hour
	}
	if q.maxBatch <= 0 {
		q.maxBatch = 10
	}
	return q
}

func (q *Queue) cmdKey(deviceID string) string { return "cmd:q:" + deviceID }
func (q *Queue) dedupKey(cmdID string) string  { return "cmd:dedup:" + cmdID }

// Enqueue 入队（幂等：同一指令ID只入队一次）
func (q *Queue) Enqueue(deviceID, cmdType string, params map[string]interface{}) (*Command, error) {
	if !q.enabled {
		return nil, nil
	}
	cmd := &Command{
		CmdID:     utils.GenerateCommandID(),
		DeviceID:  deviceID,
		Type:      cmdType,
		Params:    params,
		CreatedAt: time.Now().Unix(),
		TTL:       int64(q.cmdTTL / time.Second),
		Status:    StatusQueued,
	}
	data, _ := json.Marshal(cmd)

	if gredis.IsLocalMode() {
		q.mu.Lock()
		defer q.mu.Unlock()
		if _, dup := q.localDedup[cmd.CmdID]; dup {
			q.Duplicated++
			return nil, fmt.Errorf("duplicate command %s", cmd.CmdID)
		}
		q.localDedup[cmd.CmdID] = cmd.CreatedAt
		q.local[deviceID] = append(q.local[deviceID], cmd)
		q.Enqueued++
		return cmd, nil
	}

	ctx := gredis.Ctx()
	client := gredis.GetClient()
	// 幂等：SET NX + TTL
	ok, err := client.SetNX(ctx, q.dedupKey(cmd.CmdID), 1, q.cmdTTL).Result()
	if err != nil {
		return nil, err
	}
	if !ok {
		q.Duplicated++
		return nil, fmt.Errorf("duplicate command %s", cmd.CmdID)
	}
	if err := client.RPush(ctx, q.cmdKey(deviceID), data).Err(); err != nil {
		return nil, err
	}
	// 队列整体TTL，防止长期离线设备堆积
	client.Expire(ctx, q.cmdKey(deviceID), q.cmdTTL)
	q.Enqueued++
	return cmd, nil
}

// EnqueueWithID 使用指定指令ID入队（用于在线直发触发的指令，保持幂等）
func (q *Queue) EnqueueWithID(deviceID, cmdID, cmdType string, params map[string]interface{}) error {
	if !q.enabled {
		return nil
	}
	cmd := &Command{
		CmdID:     cmdID,
		DeviceID:  deviceID,
		Type:      cmdType,
		Params:    params,
		CreatedAt: time.Now().Unix(),
		TTL:       int64(q.cmdTTL / time.Second),
		Status:    StatusQueued,
	}
	data, _ := json.Marshal(cmd)

	if gredis.IsLocalMode() {
		q.mu.Lock()
		defer q.mu.Unlock()
		if _, dup := q.localDedup[cmdID]; dup {
			q.Duplicated++
			return fmt.Errorf("duplicate command %s", cmdID)
		}
		q.localDedup[cmdID] = cmd.CreatedAt
		q.local[deviceID] = append(q.local[deviceID], cmd)
		q.Enqueued++
		return nil
	}

	ctx := gredis.Ctx()
	client := gredis.GetClient()
	ok, err := client.SetNX(ctx, q.dedupKey(cmdID), 1, q.cmdTTL).Result()
	if err != nil {
		return err
	}
	if !ok {
		q.Duplicated++
		return fmt.Errorf("duplicate command %s", cmdID)
	}
	if err := client.RPush(ctx, q.cmdKey(deviceID), data).Err(); err != nil {
		return err
	}
	client.Expire(ctx, q.cmdKey(deviceID), q.cmdTTL)
	q.Enqueued++
	return nil
}

// Fetch 桩拉取待执行指令（不移除；CMDACK 确认后 MarkDone 才移除，失败可重拉）
func (q *Queue) Fetch(deviceID string, limit int) ([]*Command, error) {
	if !q.enabled {
		return nil, nil
	}
	if limit <= 0 {
		limit = q.maxBatch
	}
	if gredis.IsLocalMode() {
		q.mu.Lock()
		defer q.mu.Unlock()
		list := q.local[deviceID]
		var out []*Command
		for _, c := range list {
			if c.Status == StatusQueued {
				out = append(out, c)
				if len(out) >= limit {
					break
				}
			}
		}
		q.Fetched += int64(len(out))
		return out, nil
	}
	ctx := gredis.Ctx()
	client := gredis.GetClient()
	vals, err := client.LRange(ctx, q.cmdKey(deviceID), 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}
	var out []*Command
	for _, v := range vals {
		var c Command
		if err := json.Unmarshal([]byte(v), &c); err != nil {
			continue
		}
		if c.Status == StatusQueued {
			out = append(out, &c)
		}
	}
	q.Fetched += int64(len(out))
	return out, nil
}

// MarkDone 桩确认执行完成：移除队列中的指令
func (q *Queue) MarkDone(deviceID, cmdID string) error {
	if !q.enabled {
		return nil
	}
	if gredis.IsLocalMode() {
		q.mu.Lock()
		defer q.mu.Unlock()
		list := q.local[deviceID]
		for i, c := range list {
			if c.CmdID == cmdID {
				q.local[deviceID] = append(list[:i], list[i+1:]...)
				q.Done++
				return nil
			}
		}
		return nil
	}
	ctx := gredis.Ctx()
	client := gredis.GetClient()
	vals, err := client.LRange(ctx, q.cmdKey(deviceID), 0, -1).Result()
	if err != nil {
		return err
	}
	for _, v := range vals {
		var c Command
		if err := json.Unmarshal([]byte(v), &c); err != nil {
			continue
		}
		if c.CmdID == cmdID {
			if err := client.LRem(ctx, q.cmdKey(deviceID), 1, v).Err(); err != nil {
				return err
			}
			q.Done++
			return nil
		}
	}
	return nil
}

// PendingCount 待执行指令数
func (q *Queue) PendingCount(deviceID string) (int64, error) {
	if !q.enabled {
		return 0, nil
	}
	if gredis.IsLocalMode() {
		q.mu.Lock()
		defer q.mu.Unlock()
		var n int64
		for _, c := range q.local[deviceID] {
			if c.Status == StatusQueued {
				n++
			}
		}
		return n, nil
	}
	ctx := gredis.Ctx()
	n, err := gredis.GetClient().LLen(ctx, q.cmdKey(deviceID)).Result()
	return n, err
}

// CleanupExpired 清理过期指令（本地模式；Redis模式依赖key TTL自动过期）
func (q *Queue) CleanupExpired() int {
	if gredis.IsLocalMode() {
		q.mu.Lock()
		defer q.mu.Unlock()
		now := time.Now().Unix()
		var removed int
		for dev, list := range q.local {
			var keep []*Command
			for _, c := range list {
				if now-c.CreatedAt > int64(q.cmdTTL/time.Second) {
					removed++
				} else {
					keep = append(keep, c)
				}
			}
			q.local[dev] = keep
			if len(keep) == 0 {
				delete(q.local, dev)
			}
		}
		// 清理dedup表（仅清理已过期且无队列引用的）
		for id, ts := range q.localDedup {
			if now-ts > int64(q.cmdTTL/time.Second) {
				delete(q.localDedup, id)
			}
		}
		q.Expired += int64(removed)
		return removed
	}
	return 0
}

// Stats 统计
func (q *Queue) Stats() map[string]int64 {
	return map[string]int64{
		"cmd_enqueued":   q.Enqueued,
		"cmd_duplicated": q.Duplicated,
		"cmd_fetched":    q.Fetched,
		"cmd_done":       q.Done,
		"cmd_expired":    q.Expired,
	}
}

// ParseCmdIDFromPayload 从指令负载中提取指令ID（工具函数）
func ParseCmdIDFromPayload(payload string) string {
	if strings.HasPrefix(payload, "{") {
		var c Command
		if err := json.Unmarshal([]byte(payload), &c); err == nil {
			return c.CmdID
		}
	}
	return ""
}
