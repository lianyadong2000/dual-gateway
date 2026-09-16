package utils

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math/big"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// ID生成器接口
type IDGenerator interface {
	Generate() string
	GenerateWithPrefix(prefix string) string
}

// 雪花算法ID生成器
type SnowflakeGenerator struct {
	mu       sync.Mutex
	nodeID   int64
	sequence int64
	lastTime int64
	epoch    int64
}

// 创建雪花算法生成器
func NewSnowflakeGenerator(nodeID int64) *SnowflakeGenerator {
	return &SnowflakeGenerator{
		nodeID:   nodeID,
		epoch:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano() / 1e6,
		sequence: 0,
		lastTime: 0,
	}
}

// 生成ID
func (g *SnowflakeGenerator) Generate() string {
	g.mu.Lock()
	defer g.mu.Unlock()

	currentTime := time.Now().UnixNano() / 1e6 // 毫秒

	if currentTime < g.lastTime {
		currentTime = g.lastTime
	}

	if currentTime == g.lastTime {
		g.sequence = (g.sequence + 1) & 0xFFF
		if g.sequence == 0 {
			currentTime = g.waitNextMillis(g.lastTime)
		}
	} else {
		g.sequence = 0
	}

	g.lastTime = currentTime

	id := (currentTime-g.epoch)<<22 | (g.nodeID&0x3FF)<<12 | g.sequence
	return fmt.Sprintf("%d", id)
}

// 生成带前缀的ID
func (g *SnowflakeGenerator) GenerateWithPrefix(prefix string) string {
	return prefix + "_" + g.Generate()
}

// 等待下一毫秒
func (g *SnowflakeGenerator) waitNextMillis(lastTime int64) int64 {
	currentTime := time.Now().UnixNano() / 1e6
	for currentTime <= lastTime {
		time.Sleep(100 * time.Microsecond)
		currentTime = time.Now().UnixNano() / 1e6
	}
	return currentTime
}

// UUID生成器
type UUIDGenerator struct {
	counter atomic.Uint64
}

// 创建UUID生成器
func NewUUIDGenerator() *UUIDGenerator {
	return &UUIDGenerator{}
}

// 生成UUID
func (g *UUIDGenerator) Generate() string {
	b := make([]byte, 16)
	rand.Read(b)

	// 设置版本和变体
	b[6] = (b[6] & 0x0F) | 0x40 // Version 4
	b[8] = (b[8] & 0x3F) | 0x80 // Variant 10

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// 生成带前缀的UUID
func (g *UUIDGenerator) GenerateWithPrefix(prefix string) string {
	return prefix + "_" + g.Generate()
}

// 基于MD5的ID生成器
type MD5Generator struct{}

// 创建MD5生成器
func NewMD5Generator() *MD5Generator {
	return &MD5Generator{}
}

// 生成MD5 ID
func (g *MD5Generator) Generate() string {
	randomValue, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		randomValue = big.NewInt(time.Now().UnixNano())
	}

	data := fmt.Appendf(nil, "%d-%s", time.Now().UnixNano(), randomValue.String())
	hash := md5.Sum(data)
	return hex.EncodeToString(hash[:])
}

// 生成带前缀的MD5 ID
func (g *MD5Generator) GenerateWithPrefix(prefix string) string {
	return prefix + "_" + g.Generate()
}

// 获取本机MAC地址作为节点ID
func GetMacAddress() (string, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}

	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		if len(iface.HardwareAddr) > 0 {
			return iface.HardwareAddr.String(), nil
		}
	}

	return "", fmt.Errorf("no valid MAC address found")
}

// 获取主机名
func GetHostname() (string, error) {
	return os.Hostname()
}

// 生成节点ID
func GenerateNodeID() int64 {
	mac, err := GetMacAddress()
	if err != nil {
		mac = "00:00:00:00:00:00"
	}

	hash := md5.Sum([]byte(mac))
	return int64(hash[0])<<8 | int64(hash[1])
}

// 全局ID生成器
var (
	globalGenerator IDGenerator = NewSnowflakeGenerator(GenerateNodeID())
	globalMu        sync.RWMutex
)

// 设置全局ID生成器
func SetGlobalGenerator(generator IDGenerator) {
	globalMu.Lock()
	defer globalMu.Unlock()
	globalGenerator = generator
}

// 获取全局ID生成器
func GetGlobalGenerator() IDGenerator {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return globalGenerator
}

// 生成全局ID
func GenerateID() string {
	return GetGlobalGenerator().Generate()
}

// 生成带前缀的全局ID
func GenerateIDWithPrefix(prefix string) string {
	return GetGlobalGenerator().GenerateWithPrefix(prefix)
}

// 生成连接ID
func GenerateConnectionID() string {
	return GenerateIDWithPrefix("conn")
}

// 生成消息ID
func GenerateMessageID() string {
	return GenerateIDWithPrefix("msg")
}

// 生成会话ID
func GenerateSessionID() string {
	return GenerateIDWithPrefix("sess")
}

// 生成设备ID
func GenerateDeviceID() string {
	return GenerateIDWithPrefix("dev")
}

// 生成追踪ID
func GenerateTraceID() string {
	return GenerateIDWithPrefix("trace")
}

// 生成请求ID
func GenerateRequestID() string {
	return GenerateIDWithPrefix("req")
}

// 生成指令ID（全局唯一，用于指令幂等）
func GenerateCommandID() string {
	return GenerateIDWithPrefix("cmd")
}

// 生成请求ID
func GenerateServerID() string {
	return GenerateIDWithPrefix("server")
}
