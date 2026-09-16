// Package shadow 实现设备影子（Device Shadow）：为每台 IoT 设备维护一份权威状态副本。
// 桩是权威源，影子是投影：桩上报状态驱动影子变更；C 端查询/预占走影子，绝不直连弱网桩。
package shadow

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"

	"dual-gateway/internal/config"
	gredis "dual-gateway/internal/redis"
)

// 影子状态（业务态）
const (
	StateIdle      = "idle"      // 空闲：可被抢占
	StatePreoccupy = "preoccupy" // 预占/启动中：已锁定，等待桩确认
	StateCharging  = "charging"  // 充电中：业务进行态
	StateFault     = "fault"     // 故障：不可下发指令
	StateOffline   = "offline"   // 离线：心跳TTL过期（网关判定）或桩上报
)

// TLV TypeStatus 数值 → 影子状态（0x04 字段）
var StateFromTLV = map[uint8]string{
	0: StateIdle,
	1: StateCharging,
	2: StateFault,
	3: StateOffline,
}

// 影子状态 → TLV 数值
var StateToTLV = map[string]uint8{
	StateIdle:      0,
	StateCharging:  1,
	StateFault:     2,
	StateOffline:   3,
	StatePreoccupy: 4,
}

// 影子数据结构（Redis Hash 与本地降级共用）
type Shadow struct {
	DeviceID  string `json:"device_id"`
	State     string `json:"state"`
	Version   int64  `json:"version"`
	UpdatedAt int64  `json:"updated_at"`
	GatewayID string `json:"gateway_id,omitempty"`
	Online    bool   `json:"online"`
	LastSeen  int64  `json:"last_seen,omitempty"`
}

// 状态迁移合法性表（防脏状态：充电中/故障不可被抢占等）
var transitions = map[string]map[string]bool{
	StateIdle: {
		StatePreoccupy: true, // C端指令·原子抢占
		StateCharging:  true, // 桩上报（容错）
		StateFault:     true, // 桩上报
		StateOffline:   true, // 失联判定
	},
	StatePreoccupy: {
		StateIdle:     true, // 超时回滚 / 桩拒绝
		StateCharging: true, // 桩确认·充电开始
		StateFault:    true, // 桩上报故障
		StateOffline:  true, // 失联判定
	},
	StateCharging: {
		StateIdle:    true, // 桩上报·充电结束
		StateFault:   true, // 桩上报故障
		StateOffline: true, // 失联判定
	},
	StateFault: {
		StateIdle:   true, // 恢复上报 / 运维确认
		StateFault:  true, // 保持
		StateOffline: true,
	},
	StateOffline: {
		StateIdle:      true, // 重连成功·上报状态
		StatePreoccupy: true, // 离线排队预占（C端指令）
		StateFault:     true, // 上报
		StateOffline:   true, // 保持
	},
}

// CanTransition 校验状态迁移是否合法
func CanTransition(from, to string) bool {
	if m, ok := transitions[from]; ok {
		return m[to]
	}
	return from == to
}

// 事件消息（状态变更 → Pub/Sub → C端推送）
type ShadowEvent struct {
	DeviceID  string `json:"device_id"`
	State     string `json:"state"`
	Version   int64  `json:"version"`
	Timestamp int64  `json:"timestamp"`
	GatewayID string `json:"gateway_id,omitempty"`
}

// 指令触发消息（C端指令 → Pub/Sub → IoT网关在线直发）
type CmdTrigger struct {
	DeviceID string `json:"device_id"`
	CmdID    string `json:"cmd_id"`
	CmdType  string `json:"cmd_type"`
}

// 事件频道
const (
	EventChannel  = "shadow:events"   // 状态变更
	CmdChannel    = "shadow:cmds"     // 指令触发（在线直发加速）
	CmdResultChan = "shadow:cmdresults" // 指令执行结果
)

// 管理器
type Manager struct {
	Enabled           bool
	offlineTTL        time.Duration
	preoccupyTimeout  time.Duration
	onlineKeyPrefix   string
	shadowKeyPrefix   string

	// 统计
	EventsPublished  atomic.Int64
	PreoccupyOK      atomic.Int64
	PreoccupyReject  atomic.Int64
	TransitionOK     atomic.Int64
	TransitionReject atomic.Int64
	RollbackCount    atomic.Int64
	OfflineCount     atomic.Int64

	// 本地降级模式（Redis不可用时）
	mu       sync.RWMutex
	local    map[string]*Shadow
	localTTL map[string]int64 // deviceID -> lastSeen

	stopCh chan struct{}
	once   sync.Once
}

// 创建影子管理器
func NewManager(cfg *config.ShadowConfig) *Manager {
	m := &Manager{
		Enabled:          true,
		offlineTTL:       time.Duration(cfg.OfflineTTLSeconds) * time.Second,
		preoccupyTimeout: time.Duration(cfg.PreoccupyTimeoutSeconds) * time.Second,
		onlineKeyPrefix:  "shadow:online:",
		shadowKeyPrefix:  "shadow:dev:",
		local:            make(map[string]*Shadow),
		localTTL:         make(map[string]int64),
		stopCh:           make(chan struct{}),
	}
	if !cfg.Enabled {
		m.Enabled = false
	}
	if m.offlineTTL <= 0 {
		m.offlineTTL = 120 * time.Second
	}
	if m.preoccupyTimeout <= 0 {
		m.preoccupyTimeout = 60 * time.Second
	}
	return m
}

// 启动后台任务（预占超时回滚 + 本地在线过期）
func (m *Manager) Start() {
	m.once.Do(func() {
		interval := 15 * time.Second
		go func() {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					m.recoverExpiredPreoccupy()
				case <-m.stopCh:
					return
				}
			}
		}()
	})
}

// 停止后台任务
func (m *Manager) Stop() {
	close(m.stopCh)
}

// 影子Key
func (m *Manager) shadowKey(deviceID string) string { return m.shadowKeyPrefix + deviceID }
func (m *Manager) onlineKey(deviceID string) string { return m.onlineKeyPrefix + deviceID }

// ---------- Redis 模式 ----------

var luaPreoccupy = redis.NewScript(`
-- 原子抢占：仅 idle/offline 可抢占
local state = redis.call('HGET', KEYS[1], 'state') or 'offline'
if state == 'idle' or state == 'offline' then
  local v = tonumber(redis.call('HGET', KEYS[1], 'version') or '0')
  redis.call('HMSET', KEYS[1], 'state', 'preoccupy', 'version', v + 1, 'updated_at', ARGV[1])
  return {1, v + 1}
end
return {0, state}
`)

var luaTransition = redis.NewScript(`
-- 原子状态迁移（含状态机校验与版本递增）
-- KEYS[1]=shadow hash  ARGV[1]=from  ARGV[2]=to  ARGV[3]=now
local state = redis.call('HGET', KEYS[1], 'state') or 'offline'
local from = ARGV[1]
local to = ARGV[2]
local legal = 0
if to == state then
  legal = 1
elseif from == '*' then
  -- 任意状态：仅故障/离线允许无条件进入（桩上报故障、网关判离线）
  if to == 'fault' or to == 'offline' then legal = 1 end
else
  if state == from then legal = 1 end
end
if legal == 1 then
  local v = tonumber(redis.call('HGET', KEYS[1], 'version') or '0')
  redis.call('HMSET', KEYS[1], 'state', to, 'version', v + 1, 'updated_at', ARGV[3])
  return {1, v + 1}
end
return {0, state}
`)

var luaHeartbeat = redis.NewScript(`
-- 心跳：刷新在线TTL + 可选状态上报（桩上报的迁移在白名单内）
-- KEYS[1]=shadow hash  KEYS[2]=online key
-- ARGV[1]=now  ARGV[2]=ttl  ARGV[3]=state(可空)  ARGV[4]=gateway_id
redis.call('SET', KEYS[2], ARGV[1], 'EX', ARGV[2])
if ARGV[4] ~= '' then
  redis.call('HSET', KEYS[1], 'gateway_id', ARGV[4])
end
if ARGV[3] ~= '' then
  local cur = redis.call('HGET', KEYS[1], 'state') or 'offline'
  local to = ARGV[3]
  local legal = 0
  if to == cur then legal = 1
  elseif to == 'idle' and (cur == 'preoccupy' or cur == 'charging' or cur == 'offline' or cur == 'fault') then legal = 1
  elseif to == 'charging' and (cur == 'preoccupy' or cur == 'idle') then legal = 1
  elseif to == 'fault' then legal = 1
  elseif to == 'offline' then legal = 1
  end
  if legal == 1 then
    local v = tonumber(redis.call('HGET', KEYS[1], 'version') or '0')
    redis.call('HMSET', KEYS[1], 'state', to, 'version', v + 1, 'updated_at', ARGV[1])
    return {1, v + 1, to}
  end
  return {0, cur, to}
end
return {1, -1, ''}
`)

var luaTouchHeartbeat = redis.NewScript(`
-- 上线/心跳合并：刷新在线TTL + 初始化/更新网关归属（单次往返）
-- KEYS[1]=shadow hash  KEYS[2]=online key
-- ARGV[1]=now  ARGV[2]=ttl  ARGV[3]=gateway_id
redis.call('SET', KEYS[2], ARGV[1], 'EX', ARGV[2])
if ARGV[3] ~= '' then
  redis.call('HSET', KEYS[1], 'gateway_id', ARGV[3])
  redis.call('EXPIRE', KEYS[1], 604800)
end
return 1
`)

// 初始化影子（设备首次上线/重连时调用：保留旧状态，仅刷新在线与网关归属）
func (m *Manager) Touch(deviceID, gatewayID string) error {
	if !m.Enabled {
		return nil
	}
	now := time.Now().Unix()
	if gredis.IsLocalMode() {
		m.mu.Lock()
		s, ok := m.local[deviceID]
		if !ok {
			s = &Shadow{DeviceID: deviceID, State: StateIdle, Version: 0, UpdatedAt: now}
			m.local[deviceID] = s
		}
		s.GatewayID = gatewayID
		m.localTTL[deviceID] = now
		m.mu.Unlock()
		return nil
	}
	ctx := gredis.Ctx()
	return luaTouchHeartbeat.Run(ctx, gredis.GetClient(),
		[]string{m.shadowKey(deviceID), m.onlineKey(deviceID)},
		now, int64(m.offlineTTL/time.Second), gatewayID).Err()
}

// TryPreoccupy 原子抢占：仅 idle/offline 可抢占为 preoccupy
func (m *Manager) TryPreoccupy(deviceID string) (*Shadow, error) {
	s, err := m.tryPreoccupy(deviceID)
	if err == nil {
		if s != nil {
			m.PreoccupyOK.Add(1)
			// 抢占成功即发布事件（订阅者实时看到"占用中"，防双人抢桩）
			m.publishEvent(deviceID, s.State, s.Version, s.GatewayID)
		} else {
			m.PreoccupyReject.Add(1)
		}
	}
	return s, err
}

func (m *Manager) tryPreoccupy(deviceID string) (*Shadow, error) {
	if !m.Enabled {
		return nil, nil
	}
	now := time.Now().Unix()
	if gredis.IsLocalMode() {
		m.mu.Lock()
		defer m.mu.Unlock()
		s, ok := m.local[deviceID]
		if !ok {
			return nil, fmt.Errorf("shadow not found: %s", deviceID)
		}
		if s.State != StateIdle && s.State != StateOffline {
			return cloneShadow(s), fmt.Errorf("device %s state=%s cannot preoccupy", deviceID, s.State)
		}
		s.State = StatePreoccupy
		s.Version++
		s.UpdatedAt = now
		m.localTTL[deviceID] = now
		return cloneShadow(s), nil
	}
	ctx := gredis.Ctx()
	res, err := luaPreoccupy.Run(ctx, gredis.GetClient(), []string{m.shadowKey(deviceID)}, now).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("shadow not found: %s", deviceID)
		}
		return nil, err
	}
	arr, ok := res.([]interface{})
	if !ok || len(arr) < 2 {
		return nil, fmt.Errorf("unexpected lua result")
	}
	if arr[0].(int64) == 1 {
		return &Shadow{DeviceID: deviceID, State: StatePreoccupy, Version: arr[1].(int64), UpdatedAt: now}, nil
	}
	return nil, fmt.Errorf("device %s state=%v cannot preoccupy", deviceID, arr[1])
}

// Transition 原子状态迁移（带状态机校验）
func (m *Manager) Transition(deviceID, from, to, gatewayID string) (*Shadow, error) {
	if !CanTransition(from, to) && from != "*" {
		m.TransitionReject.Add(1)
		return nil, fmt.Errorf("invalid transition %s->%s", from, to)
	}
	s, err := m.transition(deviceID, from, to, gatewayID)
	if err == nil && s != nil {
		m.TransitionOK.Add(1)
		m.publishEvent(deviceID, s.State, s.Version, gatewayID)
	}
	return s, err
}

func (m *Manager) transition(deviceID, from, to, gatewayID string) (*Shadow, error) {
	if !m.Enabled {
		return nil, nil
	}
	now := time.Now().Unix()
	if gredis.IsLocalMode() {
		m.mu.Lock()
		defer m.mu.Unlock()
		s, ok := m.local[deviceID]
		if !ok {
			return nil, fmt.Errorf("shadow not found: %s", deviceID)
		}
		if from != "*" && s.State != from {
			return nil, fmt.Errorf("state mismatch: have=%s want=%s", s.State, from)
		}
		s.State = to
		s.Version++
		s.UpdatedAt = now
		if gatewayID != "" {
			s.GatewayID = gatewayID
		}
		return cloneShadow(s), nil
	}
	ctx := gredis.Ctx()
	res, err := luaTransition.Run(ctx, gredis.GetClient(), []string{m.shadowKey(deviceID)}, from, to, now).Result()
	if err != nil {
		return nil, err
	}
	arr, ok := res.([]interface{})
	if !ok || len(arr) < 2 {
		return nil, fmt.Errorf("unexpected lua result")
	}
	if arr[0].(int64) == 1 {
		return &Shadow{DeviceID: deviceID, State: to, Version: arr[1].(int64), UpdatedAt: now}, nil
	}
	return nil, fmt.Errorf("transition rejected: %v", arr[1])
}

// ReportHeartbeat 桩心跳：刷新在线TTL；state 非空时同时上报状态（桩权威）
func (m *Manager) ReportHeartbeat(deviceID, state string) error {
	return m.ReportHeartbeatWithGateway(deviceID, state, "")
}

// ReportHeartbeatWithGateway 带网关归属的心跳
func (m *Manager) ReportHeartbeatWithGateway(deviceID, state, gatewayID string) error {
	if !m.Enabled {
		return nil
	}
	now := time.Now().Unix()
	ttl := int64(m.offlineTTL / time.Second)

	if gredis.IsLocalMode() {
		m.mu.Lock()
		defer m.mu.Unlock()
		s, ok := m.local[deviceID]
		if !ok {
			s = &Shadow{DeviceID: deviceID, State: StateIdle, Version: 0, UpdatedAt: now}
			m.local[deviceID] = s
		}
		if gatewayID != "" {
			s.GatewayID = gatewayID
		}
		m.localTTL[deviceID] = now
		if state != "" && state != s.State {
			// 桩上报：校验合法迁移
			if legalReport(s.State, state) {
				s.State = state
				s.Version++
				s.UpdatedAt = now
				m.TransitionOK.Add(1)
				m.publishEvent(deviceID, state, s.Version, gatewayID)
			} else {
				m.TransitionReject.Add(1)
			}
		}
		return nil
	}

	ctx := gredis.Ctx()
	res, err := luaHeartbeat.Run(ctx, gredis.GetClient(),
		[]string{m.shadowKey(deviceID), m.onlineKey(deviceID)},
		now, ttl, state, gatewayID).Result()
	if err != nil {
		return err
	}
	arr, ok := res.([]interface{})
	if !ok || len(arr) < 3 {
		return fmt.Errorf("unexpected lua result")
	}
	if arr[0].(int64) == 1 && arr[1].(int64) > 0 {
		m.TransitionOK.Add(1)
		m.publishEvent(deviceID, arr[2].(string), arr[1].(int64), gatewayID)
	} else if arr[0].(int64) == 0 {
		m.TransitionReject.Add(1)
	}
	return nil
}

// ReportState 桩主动上报状态（变更事件 → 推送）
func (m *Manager) ReportState(deviceID, state, gatewayID string) error {
	return m.ReportHeartbeatWithGateway(deviceID, state, gatewayID)
}

// MarkOffline 失联判定（会话清理触发）
func (m *Manager) MarkOffline(deviceID string) error {
	m.OfflineCount.Add(1)
	if _, err := m.Transition(deviceID, "*", StateOffline, ""); err != nil {
		return err
	}
	return nil
}

// RollbackPreoccupy 预占超时回滚：preoccupy → idle
func (m *Manager) RollbackPreoccupy(deviceID string) error {
	m.RollbackCount.Add(1)
	_, err := m.Transition(deviceID, StatePreoccupy, StateIdle, "")
	return err
}

// Get 查询影子（C端查询入口：仅影子，不直连桩）
func (m *Manager) Get(deviceID string) (*Shadow, error) {
	if !m.Enabled {
		return &Shadow{DeviceID: deviceID, State: StateOffline, Online: false}, nil
	}
	if gredis.IsLocalMode() {
		m.mu.RLock()
		defer m.mu.RUnlock()
		s, ok := m.local[deviceID]
		if !ok {
			return &Shadow{DeviceID: deviceID, State: StateOffline, Online: false}, nil
		}
		lastSeen := m.localTTL[deviceID]
		online := time.Now().Unix()-lastSeen <= int64(m.offlineTTL/time.Second)
		out := cloneShadow(s)
		out.Online = online
		out.LastSeen = lastSeen
		return out, nil
	}
	ctx := gredis.Ctx()
	client := gredis.GetClient()
	pipe := client.Pipeline()
	hashCmd := pipe.HGetAll(ctx, m.shadowKey(deviceID))
	onlineCmd := pipe.Get(ctx, m.onlineKey(deviceID))
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}
	fields, err := hashCmd.Result()
	if err != nil {
		return &Shadow{DeviceID: deviceID, State: StateOffline, Online: false}, nil
	}
	if len(fields) == 0 {
		return &Shadow{DeviceID: deviceID, State: StateOffline, Online: false}, nil
	}
	s := &Shadow{DeviceID: deviceID}
	s.State = fields["state"]
	if s.State == "" {
		s.State = StateOffline
	}
	fmt.Sscanf(fields["version"], "%d", &s.Version)
	fmt.Sscanf(fields["updated_at"], "%d", &s.UpdatedAt)
	s.GatewayID = fields["gateway_id"]
	onlineStr, _ := onlineCmd.Result()
	s.Online = onlineStr != ""
	if t, err := onlineCmd.Int64(); err == nil {
		s.LastSeen = t
	}
	return s, nil
}

// IsOnline 是否在线（心跳TTL内）
func (m *Manager) IsOnline(deviceID string) bool {
	s, err := m.Get(deviceID)
	if err != nil {
		return false
	}
	return s.Online
}

// 预占超时回滚扫描
func (m *Manager) recoverExpiredPreoccupy() {
	now := time.Now().Unix()
	if gredis.IsLocalMode() {
		m.mu.Lock()
		defer m.mu.Unlock()
		for id, s := range m.local {
			if s.State == StatePreoccupy && now-s.UpdatedAt > int64(m.preoccupyTimeout/time.Second) {
				s.State = StateIdle
				s.Version++
				s.UpdatedAt = now
				m.RollbackCount.Add(1)
				m.publishEvent(id, StateIdle, s.Version, "")
			}
		}
		return
	}
	// Redis 模式：SCAN 所有影子，找到超时 preoccupy 回滚
	ctx := gredis.Ctx()
	client := gredis.GetClient()
	iter := client.Scan(ctx, 0, m.shadowKeyPrefix+"*", 500).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		deviceID := strings.TrimPrefix(key, m.shadowKeyPrefix)
		vals, err := client.HMGet(ctx, key, "state", "updated_at").Result()
		if err != nil || len(vals) < 2 {
			continue
		}
		state, _ := vals[0].(string)
		updated, _ := vals[1].(string)
		var updatedAt int64
		fmt.Sscanf(updated, "%d", &updatedAt)
		if state == StatePreoccupy && now-updatedAt > int64(m.preoccupyTimeout/time.Second) {
			m.RollbackPreoccupy(deviceID)
		}
	}
	if err := iter.Err(); err != nil {
		log.Printf("shadow scan error: %v", err)
	}
}

// 发布状态变更事件
func (m *Manager) publishEvent(deviceID, state string, version int64, gatewayID string) {
	m.EventsPublished.Add(1)
	evt := ShadowEvent{
		DeviceID:  deviceID,
		State:     state,
		Version:   version,
		Timestamp: time.Now().UnixMilli(),
		GatewayID: gatewayID,
	}
	data, _ := json.Marshal(evt)
	if err := gredis.PublishMessage(EventChannel, data); err != nil {
		// 本地降级模式：无订阅者，静默
	}
}

// 统计（health 用）
func (m *Manager) Stats() map[string]int64 {
	return map[string]int64{
		"shadow_events":      m.EventsPublished.Load(),
		"shadow_preoccupy_ok": m.PreoccupyOK.Load(),
		"shadow_preoccupy_reject": m.PreoccupyReject.Load(),
		"shadow_transition_ok": m.TransitionOK.Load(),
		"shadow_transition_reject": m.TransitionReject.Load(),
		"shadow_rollback":     m.RollbackCount.Load(),
		"shadow_offline":      m.OfflineCount.Load(),
	}
}

// 桩上报状态迁移白名单（Go侧校验，双保险）
func legalReport(cur, to string) bool {
	switch to {
	case StateIdle:
		return cur == StatePreoccupy || cur == StateCharging || cur == StateOffline || cur == StateFault
	case StateCharging:
		return cur == StatePreoccupy || cur == StateIdle
	case StateFault, StateOffline:
		return true
	}
	return cur == to
}

func cloneShadow(s *Shadow) *Shadow {
	c := *s
	return &c
}
