package utils

import (
	"bytes"
	"sync"
	"time"
)

// 字节缓冲池
var bufferPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

// 获取缓冲区
func GetBuffer() *bytes.Buffer {
	return bufferPool.Get().(*bytes.Buffer)
}

// 释放缓冲区
func PutBuffer(buf *bytes.Buffer) {
	buf.Reset()
	bufferPool.Put(buf)
}

// 字节切片池
type ByteSlicePool struct {
	pool   sync.Pool
	size   int
}

// 创建字节切片池
func NewByteSlicePool(size int) *ByteSlicePool {
	return &ByteSlicePool{
		pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, size)
			},
		},
		size: size,
	}
}

// 获取字节切片
func (p *ByteSlicePool) Get() []byte {
	return p.pool.Get().([]byte)
}

// 释放字节切片
func (p *ByteSlicePool) Put(b []byte) {
	if cap(b) >= p.size {
		p.pool.Put(b[:p.size])
	}
}

// 通用对象池
type ObjectPool struct {
	pool    sync.Pool
	factory func() interface{}
	reset   func(interface{})
}

// 创建对象池
func NewObjectPool(factory func() interface{}, reset func(interface{})) *ObjectPool {
	return &ObjectPool{
		pool: sync.Pool{
			New: factory,
		},
		factory: factory,
		reset:   reset,
	}
}

// 获取对象
func (p *ObjectPool) Get() interface{} {
	return p.pool.Get()
}

// 释放对象
func (p *ObjectPool) Put(obj interface{}) {
	if p.reset != nil {
		p.reset(obj)
	}
	p.pool.Put(obj)
}

// 消息对象池
type MessagePool struct {
	pool sync.Pool
}

// 创建消息池
func NewMessagePool() *MessagePool {
	return &MessagePool{
		pool: sync.Pool{
			New: func() interface{} {
				return &Message{}
			},
		},
	}
}

// 消息结构
type Message struct {
	ID        string
	Type      string
	From      string
	To        string
	Content   []byte
	Timestamp int64
	Metadata  map[string]string
}

// 获取消息
func (p *MessagePool) Get() *Message {
	return p.pool.Get().(*Message)
}

// 释放消息
func (p *MessagePool) Put(msg *Message) {
	msg.ID = ""
	msg.Type = ""
	msg.From = ""
	msg.To = ""
	msg.Content = nil
	msg.Timestamp = 0
	msg.Metadata = nil
	p.pool.Put(msg)
}

// 连接对象池
type ConnectionPool struct {
	pool sync.Pool
}

// 创建连接池
func NewConnectionPool() *ConnectionPool {
	return &ConnectionPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &Connection{}
			},
		},
	}
}

// 连接结构
type Connection struct {
	ID         string
	UserID     string
	DeviceID   string
	RemoteAddr string
	CreatedAt  int64
	LastActive int64
}

// 获取连接
func (p *ConnectionPool) Get() *Connection {
	return p.pool.Get().(*Connection)
}

// 释放连接
func (p *ConnectionPool) Put(conn *Connection) {
	conn.ID = ""
	conn.UserID = ""
	conn.DeviceID = ""
	conn.RemoteAddr = ""
	conn.CreatedAt = 0
	conn.LastActive = 0
	p.pool.Put(conn)
}

// 定时清理池
type TickerPool struct {
	mu      sync.Mutex
	items   map[string]interface{}
	maxSize int
	ttl     int64
}

// 创建定时清理池
func NewTickerPool(maxSize int, ttl int64) *TickerPool {
	return &TickerPool{
		items:   make(map[string]interface{}),
		maxSize: maxSize,
		ttl:     ttl,
	}
}

// 添加项
func (p *TickerPool) Add(key string, value interface{}) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if len(p.items) >= p.maxSize {
		return false
	}
	
	p.items[key] = value
	return true
}

// 获取项
func (p *TickerPool) Get(key string) (interface{}, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	value, ok := p.items[key]
	return value, ok
}

// 删除项
func (p *TickerPool) Remove(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.items, key)
}

// 清理过期项
func (p *TickerPool) Cleanup() {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	now := time.Now().Unix()
	for key, value := range p.items {
		// 检查是否过期
		if item, ok := value.(interface{ GetTimestamp() int64 }); ok {
			if now-item.GetTimestamp() > p.ttl {
				delete(p.items, key)
			}
		}
	}
}

// 获取池大小
func (p *TickerPool) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.items)
}