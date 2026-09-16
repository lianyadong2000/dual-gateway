package ratelimit

import (
	"sync"
	"testing"
	"time"
)

// 令牌桶基本速率控制
func TestTokenBucketRate(t *testing.T) {
	b := NewTokenBucket(10, 10) // 每秒10个
	// 桶满：立即可以取10个
	for i := 0; i < 10; i++ {
		if !b.Allow() {
			t.Fatalf("token %d should be allowed", i)
		}
	}
	// 桶空：立即拒绝
	if b.Allow() {
		t.Fatal("empty bucket should reject")
	}
	// 等待0.5s → 补充约5个
	time.Sleep(550 * time.Millisecond)
	for i := 0; i < 5; i++ {
		if !b.Allow() {
			t.Fatalf("refill token %d should be allowed", i)
		}
	}
	if b.Allow() {
		t.Fatal("should exhaust again")
	}
}

// 并发安全：多goroutine同时取令牌，总数不超过 burst
func TestTokenBucketConcurrent(t *testing.T) {
	b := NewTokenBucket(1e6, 1000) // 高速率、容量1000
	var wg sync.WaitGroup
	var okCount int64
	var mu sync.Mutex
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if b.Allow() {
					mu.Lock()
					okCount++
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	if okCount > 1000 {
		t.Errorf("concurrent allows=%d > burst 1000", okCount)
	}
}

// 禁用时限流放行
func TestConnectLimiterDisabled(t *testing.T) {
	l := NewConnectLimiter(1, 1, false)
	for i := 0; i < 10; i++ {
		if !l.Allow() {
			t.Fatal("disabled limiter should allow all")
		}
	}
	acc, rej := l.Stats()
	if acc != 10 || rej != 0 {
		t.Errorf("stats acc=%d rej=%d", acc, rej)
	}
}

// 启用后超限拒绝
func TestConnectLimiterEnabled(t *testing.T) {
	l := NewConnectLimiter(5, 5, true)
	for i := 0; i < 5; i++ {
		if !l.Allow() {
			t.Fatal("should allow within burst")
		}
	}
	if l.Allow() {
		t.Fatal("should reject beyond burst")
	}
	_, rej := l.Stats()
	if rej != 1 {
		t.Errorf("rejected=%d want 1", rej)
	}
}

// 源IP限流：独立桶
func TestIPLimiterPerIP(t *testing.T) {
	l := NewIPLimiter(2, 2, true)
	if !l.Allow("1.1.1.1") || !l.Allow("1.1.1.1") {
		t.Fatal("two allows for same ip")
	}
	if l.Allow("1.1.1.1") {
		t.Fatal("third should reject")
	}
	if !l.Allow("2.2.2.2") {
		t.Fatal("different ip independent bucket")
	}
	if l.ActiveIPCount() != 2 {
		t.Errorf("active ips=%d want 2", l.ActiveIPCount())
	}
}

// AcceptLimiter 超限拒绝计数
func TestAcceptLimiter(t *testing.T) {
	l := NewAcceptLimiter(3, true)
	for i := 0; i < 3; i++ {
		if !l.Allow() {
			t.Fatal("within burst allow")
		}
	}
	if l.Allow() {
		t.Fatal("over burst reject")
	}
	if l.RejectedCount() != 1 {
		t.Errorf("rejected=%d want 1", l.RejectedCount())
	}
}
