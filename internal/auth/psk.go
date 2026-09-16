package auth

import (
    "crypto/sha256"
    "encoding/hex"
    "sync"
    "time"
)

// PSK设备信息
type PSKDevice struct {
    DeviceID  string
    PSK       string
    CreatedAt time.Time
    UpdatedAt time.Time
    Active    bool
}

// PSK管理器
type PSKManager struct {
    devices sync.Map // deviceID -> *PSKDevice
}

// 创建PSK管理器
func NewPSKManager() *PSKManager {
    return &PSKManager{}
}

// 注册设备
func (m *PSKManager) RegisterDevice(deviceID, psk string) error {
    device := &PSKDevice{
        DeviceID:  deviceID,
        PSK:       psk,
        CreatedAt: time.Now(),
        UpdatedAt: time.Now(),
        Active:    true,
    }
    
    m.devices.Store(deviceID, device)
    return nil
}

// 验证设备
func (m *PSKManager) ValidateDevice(deviceID, pskHash string) bool {
    device, ok := m.devices.Load(deviceID)
    if !ok {
        return false
    }
    
    dev := device.(*PSKDevice)
    if !dev.Active {
        return false
    }
    
    // 计算PSK的哈希
    expectedHash := hashPSK(dev.PSK)
    return pskHash == expectedHash
}

// 获取设备PSK
func (m *PSKManager) GetPSK(deviceID string) (string, bool) {
    device, ok := m.devices.Load(deviceID)
    if !ok {
        return "", false
    }
    
    dev := device.(*PSKDevice)
    if !dev.Active {
        return "", false
    }
    
    return dev.PSK, true
}

// 计算PSK哈希
func hashPSK(psk string) string {
    hash := sha256.Sum256([]byte(psk))
    return hex.EncodeToString(hash[:])
}

// 全局PSK管理器
var globalPSKManager = NewPSKManager()

// 初始化PSK设备
func InitPSKDevices(devices map[string]string) {
    for deviceID, psk := range devices {
        globalPSKManager.RegisterDevice(deviceID, psk)
    }
}

// 验证PSK设备
func ValidatePSKDevice(deviceID string) bool {
    _, ok := globalPSKManager.GetPSK(deviceID)
    return ok
}