package security

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/pion/dtls/v2"
	"github.com/pion/dtls/v2/pkg/crypto/selfsign"
)

// DTLS配置管理器
type DTLSManager struct {
	mu          sync.RWMutex
	config      *dtls.Config
	certificate tls.Certificate
	pskStore    *PSKStore
	listeners   map[string]net.Listener
	ctx         context.Context
	cancel      context.CancelFunc
}

// PSK存储
type PSKStore struct {
	mu    sync.RWMutex
	keys  map[string][]byte // deviceID -> PSK
	hints map[string][]byte // deviceID -> hint
}

// 创建PSK存储
func NewPSKStore() *PSKStore {
	return &PSKStore{
		keys:  make(map[string][]byte),
		hints: make(map[string][]byte),
	}
}

// 添加PSK
func (s *PSKStore) AddPSK(deviceID string, psk []byte, hint []byte) error {
	if len(psk) < 16 {
		return errors.New("PSK must be at least 16 bytes")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.keys[deviceID] = psk
	s.hints[deviceID] = hint
	return nil
}

// 获取PSK
func (s *PSKStore) GetPSK(deviceID string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	psk, ok := s.keys[deviceID]
	return psk, ok
}

// 获取PSK Hint
func (s *PSKStore) GetHint(deviceID string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	hint, ok := s.hints[deviceID]
	return hint, ok
}

// 删除PSK
func (s *PSKStore) RemovePSK(deviceID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.keys, deviceID)
	delete(s.hints, deviceID)
}

// 创建DTLS管理器
func NewDTLSManager(pskStore *PSKStore) (*DTLSManager, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// 生成自签名证书
	certificate, err := selfsign.GenerateSelfSigned()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to generate certificate: %w", err)
	}

	// 配置DTLS
	config := &dtls.Config{
		Certificates: []tls.Certificate{certificate},

		// PSK回调
		PSK: func(hint []byte) ([]byte, error) {
			deviceID := string(hint)
			if psk, ok := pskStore.GetPSK(deviceID); ok {
				return psk, nil
			}
			return nil, errors.New("PSK not found for device")
		},

		PSKIdentityHint: []byte("gateway"),

		// 支持的加密套件
		CipherSuites: []dtls.CipherSuiteID{
			dtls.TLS_PSK_WITH_AES_128_CCM,
			dtls.TLS_PSK_WITH_AES_128_CCM_8,
			dtls.TLS_PSK_WITH_AES_128_GCM_SHA256,
		},

		// 握手上下文
		ConnectContextMaker: func() (context.Context, func()) {
			return context.WithTimeout(ctx, 30*time.Second)
		},

		// 握手配置
		FlightInterval: 100 * time.Millisecond,

		// MTU
		MTU: 1200,

		// 扩展握手
		ExtendedMasterSecret: dtls.RequireExtendedMasterSecret,
	}

	return &DTLSManager{
		config:      config,
		certificate: certificate,
		pskStore:    pskStore,
		listeners:   make(map[string]net.Listener),
		ctx:         ctx,
		cancel:      cancel,
	}, nil
}

// 获取DTLS配置
func (m *DTLSManager) GetConfig() *dtls.Config {
	return m.config
}

// 启动DTLS监听
func (m *DTLSManager) Listen(network, addr string) (net.Listener, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	udpAddr, err := net.ResolveUDPAddr(network, addr)
	if err != nil {
		return nil, err
	}

	listener, err := dtls.Listen(network, udpAddr, m.config)
	if err != nil {
		return nil, err
	}

	m.listeners[addr] = listener
	return listener, nil
}

// 停止所有监听
func (m *DTLSManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for addr, listener := range m.listeners {
		listener.Close()
		delete(m.listeners, addr)
	}
	m.cancel()
}

// 生成随机PSK
func GeneratePSK(length int) ([]byte, error) {
	if length < 16 || length > 64 {
		return nil, errors.New("PSK length must be between 16 and 64 bytes")
	}

	psk := make([]byte, length)
	if _, err := rand.Read(psk); err != nil {
		return nil, fmt.Errorf("failed to generate PSK: %w", err)
	}

	return psk, nil
}
