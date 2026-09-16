package auth

import (
	"strings"
	"testing"
	"time"
)

func TestGenerateAndValidateToken(t *testing.T) {
	privPEM, pubPEM, err := GenerateRSAKeyPair(2048)
	if err != nil {
		t.Fatalf("GenerateRSAKeyPair failed: %v", err)
	}

	manager, err := NewJWTManager(privPEM, pubPEM, "dual-gateway", 2*time.Hour)
	if err != nil {
		t.Fatalf("NewJWTManager failed: %v", err)
	}

	// 生成token
	token, err := manager.GenerateToken("user_001", "dev_001")
	if err != nil {
		t.Fatalf("GenerateToken failed: %v", err)
	}
	if token == "" {
		t.Fatal("empty token")
	}

	// 验证token
	claims, err := manager.ValidateToken(token)
	if err != nil {
		t.Fatalf("ValidateToken failed: %v", err)
	}

	if claims.UserID != "user_001" {
		t.Errorf("user id = %q, want %q", claims.UserID, "user_001")
	}
	if claims.DeviceID != "dev_001" {
		t.Errorf("device id = %q, want %q", claims.DeviceID, "dev_001")
	}
	if claims.Issuer != "dual-gateway" {
		t.Errorf("issuer = %q, want %q", claims.Issuer, "dual-gateway")
	}
}

func TestValidateInvalidToken(t *testing.T) {
	privPEM, pubPEM, _ := GenerateRSAKeyPair(2048)
	manager, _ := NewJWTManager(privPEM, pubPEM, "dual-gateway", time.Hour)

	if _, err := manager.ValidateToken("invalid-token"); err == nil {
		t.Error("expected error for invalid token")
	}
	if _, err := manager.ValidateToken(""); err == nil {
		t.Error("expected error for empty token")
	}
}

func TestExpiredToken(t *testing.T) {
	privPEM, pubPEM, _ := GenerateRSAKeyPair(2048)
	manager, _ := NewJWTManager(privPEM, pubPEM, "dual-gateway", 1*time.Second)

	token, err := manager.GenerateToken("user_001", "dev_001")
	if err != nil {
		t.Fatalf("GenerateToken failed: %v", err)
	}

	// 等待过期
	time.Sleep(1500 * time.Millisecond)

	if _, err := manager.ValidateToken(token); err == nil {
		t.Error("expected error for expired token")
	}
}

func TestEnsureRSAKeys(t *testing.T) {
	priv := ""
	pub := ""

	manager, err := EnsureRSAKeys(&priv, &pub, "dual-gateway")
	if err != nil {
		t.Fatalf("EnsureRSAKeys failed: %v", err)
	}
	if manager == nil {
		t.Fatal("manager is nil")
	}
	if priv == "" || pub == "" {
		t.Fatal("keys not populated")
	}

	// 再次调用应复用已有密钥
	priv2 := priv
	pub2 := pub
	if _, err := EnsureRSAKeys(&priv2, &pub2, "dual-gateway"); err != nil {
		t.Fatalf("EnsureRSAKeys (reuse) failed: %v", err)
	}
	if priv2 != priv {
		t.Error("keys regenerated instead of reused")
	}
}

func TestGenerateRSAKeyPair(t *testing.T) {
	privPEM, pubPEM, err := GenerateRSAKeyPair(2048)
	if err != nil {
		t.Fatalf("GenerateRSAKeyPair failed: %v", err)
	}

	if !strings.Contains(string(privPEM), "PRIVATE KEY") {
		t.Error("private key PEM missing header")
	}
	if !strings.Contains(string(pubPEM), "PUBLIC KEY") {
		t.Error("public key PEM missing header")
	}
}

func TestPEMRoundTrip(t *testing.T) {
	privPEM, pubPEM, _ := GenerateRSAKeyPair(2048)

	// 用PEM创建manager（模拟配置加载）
	manager, err := NewJWTManager(privPEM, pubPEM, "issuer", time.Hour)
	if err != nil {
		t.Fatalf("NewJWTManager with PEM failed: %v", err)
	}

	token, _ := manager.GenerateToken("u1", "d1")
	claims, err := manager.ValidateToken(token)
	if err != nil {
		t.Fatalf("ValidateToken failed: %v", err)
	}
	if claims.UserID != "u1" {
		t.Errorf("user id = %q, want u1", claims.UserID)
	}
}
