// Package ratelimit 实现分层接入限流：
//  1. CONNECT 令牌桶：限制每秒接受的握手数（超限回 CONNACK 0x03 + Retry-After）
//  2. UDP 源 IP 限流：防伪造源地址的洪峰（每个源 IP 独立令牌桶）
//  3. TCP accept 节流：C 端 WebSocket 升级入口限速
// 自研令牌桶（无外部依赖），并发安全。
package ratelimit

import (
	"sync"
	"sync/atomic"
	"time"
)

// 令牌桶
type TokenBucket struct {
	mu       sync.Mutex
	rate     float64 // 每秒补充速率
	burst    float64 // 桶容量
	tokens   float64
	last     time.Time
}

// NewTokenBucket 创建令牌桶
func NewTokenBucket(rate, burst float64) *TokenBucket {
	if burst <= 0 {
		burst = rate
	}
	return &TokenBucket{
		rate:   rate,
		burst:  burst,
		tokens: burst,
		last:   time.Now(),
	}
}

// Allow 尝试取一个令牌
func (t *TokenBucket) Allow() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(t.last).Seconds()
	t.last = now
	t.tokens += elapsed * t.rate
	if t.tokens > t.burst {
		t.tokens = t.burst
	}
	if t.tokens >= 1 {
		t.tokens--
		return true
	}
	return false
}

// AllowN 尝试取 n 个令牌
func (t *TokenBucket) AllowN(n float64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(t.last).Seconds()
	t.last = now
	t.tokens += elapsed * t.rate
	if t.tokens > t.burst {
		t.tokens = t.burst
	}
	if t.tokens >= n {
		t.tokens -= n
		return true
	}
	return false
}

// ConnectLimiter CONNECT 接入限流（IoT MQTT-SN/DTLS 握手）
type ConnectLimiter struct {
	Enabled  bool
	bucket   *TokenBucket
	Accepted atomic.Int64
	Rejected atomic.Int64
}

// NewConnectLimiter 创建连接限流器
func NewConnectLimiter(rate, burst int, enabled bool) *ConnectLimiter {
	return &ConnectLimiter{
		Enabled: enabled,
		bucket:  NewTokenBucket(float64(rate), float64(burst)),
	}
}

// Allow 是否允许本次握手
func (l *ConnectLimiter) Allow() bool {
	if !l.Enabled {
		l.Accepted.Add(1)
		return true
	}
	if l.bucket.Allow() {
		l.Accepted.Add(1)
		return true
	}
	l.Rejected.Add(1)
	return false
}

// Stats 限流统计
func (l *ConnectLimiter) Stats() (int64, int64) {
	return l.Accepted.Load(), l.Rejected.Load()
}

// IPLimiter 源 IP 限流（UDP 无连接，按源 IP 维度限流防伪造洪峰）
type IPLimiter struct {
	Enabled bool
	rate    float64
	burst   float64
	mu      sync.Mutex
	buckets map[string]*TokenBucket
	CleanupInterval time.Duration
}

// NewIPLimiter 创建源IP限流器
func NewIPLimiter(rate, burst int, enabled bool) *IPLimiter {
	return &IPLimiter{
		Enabled:         enabled,
		rate:            float64(rate),
		burst:           float64(burst),
		buckets:         make(map[string]*TokenBucket),
		CleanupInterval: 5 * time.Minute,
	}
}

// Allow 指定源IP是否允许
func (l *IPLimiter) Allow(ip string) bool {
	if !l.Enabled {
		return true
	}
	l.mu.Lock()
	b, ok := l.buckets[ip]
	if !ok {
		b = NewTokenBucket(l.rate, l.burst)
		l.buckets[ip] = b
	}
	l.mu.Unlock()
	return b.Allow()
}

// Cleanup 清理长时间无活动的IP桶（防内存膨胀）
func (l *IPLimiter) Cleanup() {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	for ip, b := range l.buckets {
		b.mu.Lock()
		idle := now.Sub(b.last)
		b.mu.Unlock()
		if idle > l.CleanupInterval {
			delete(l.buckets, ip)
		}
	}
}

// ActiveIPCount 当前追踪的源IP数（监控用）
func (l *IPLimiter) ActiveIPCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.buckets)
}

// AcceptLimiter TCP/WS accept 节流（C端升级入口，防 SYN/握手洪峰）
type AcceptLimiter struct {
	Enabled  bool
	bucket   *TokenBucket
	Rejected atomic.Int64
}

// NewAcceptLimiter 创建accept限流器
func NewAcceptLimiter(rate int, enabled bool) *AcceptLimiter {
	return &AcceptLimiter{
		Enabled: enabled,
		bucket:  NewTokenBucket(float64(rate), float64(rate)),
	}
}

// Allow 是否允许接受新连接
func (l *AcceptLimiter) Allow() bool {
	if !l.Enabled {
		return true
	}
	if l.bucket.Allow() {
		return true
	}
	l.Rejected.Add(1)
	return false
}

// RejectedCount 拒绝计数
func (l *AcceptLimiter) RejectedCount() int64 {
	return l.Rejected.Load()
}
