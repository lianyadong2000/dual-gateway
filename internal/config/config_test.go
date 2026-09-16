package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadDefault(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Cend.Port != 8080 {
		t.Errorf("default cend port = %d, want 8080", cfg.Cend.Port)
	}
	if cfg.IoT.Port != 5683 {
		t.Errorf("default iot port = %d, want 5683", cfg.IoT.Port)
	}
	if cfg.IoT.DTLSPort != 5684 {
		t.Errorf("default dtls port = %d, want 5684", cfg.IoT.DTLSPort)
	}
	if cfg.Auth.JWTTTLSeconds != 7200 {
		t.Errorf("default jwt ttl = %d, want 7200", cfg.Auth.JWTTTLSeconds)
	}
}

func TestJWTTTLDuration(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Auth.JWTTTLSeconds = 7200

	// 关键回归：7200秒必须转换为2小时，而非7200纳秒
	ttl := cfg.Auth.JWTTTL()
	if ttl != 2*time.Hour {
		t.Errorf("JWTTTL() = %v, want %v", ttl, 2*time.Hour)
	}

	cfg.Auth.JWTTTLSeconds = 60
	if ttl := cfg.Auth.JWTTTL(); ttl != time.Minute {
		t.Errorf("JWTTTL() = %v, want 1m", ttl)
	}

	// 非正值回退默认
	cfg.Auth.JWTTTLSeconds = 0
	if ttl := cfg.Auth.JWTTTL(); ttl != 2*time.Hour {
		t.Errorf("JWTTTL() = %v, want default 2h", ttl)
	}
}

func TestLoadFromYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yml")

	yaml := `
cend:
  port: 9999
  max_connections: 50000
iot:
  port: 6000
  dtls_port: 6001
auth:
  jwt_ttl: 3600
  psk_devices:
    dev1: "psk1"
`
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Cend.Port != 9999 {
		t.Errorf("cend port = %d, want 9999", cfg.Cend.Port)
	}
	if cfg.Cend.MaxConnections != 50000 {
		t.Errorf("max connections = %d, want 50000", cfg.Cend.MaxConnections)
	}
	if cfg.IoT.DTLSPort != 6001 {
		t.Errorf("dtls port = %d, want 6001", cfg.IoT.DTLSPort)
	}
	if cfg.Auth.JWTTTL() != time.Hour {
		t.Errorf("jwt ttl = %v, want 1h", cfg.Auth.JWTTTL())
	}
	if _, ok := cfg.Auth.PSKDevices["dev1"]; !ok {
		t.Error("psk device dev1 missing")
	}
}

func TestLoadJWTKeyFiles(t *testing.T) {
	dir := t.TempDir()
	privPath := filepath.Join(dir, "private.pem")
	pubPath := filepath.Join(dir, "public.pem")

	os.WriteFile(privPath, []byte("PRIVATE-KEY-CONTENT"), 0600)
	os.WriteFile(pubPath, []byte("PUBLIC-KEY-CONTENT"), 0644)

	cfg := DefaultConfig()
	cfg.Auth.JWTPrivateKeyFile = privPath
	cfg.Auth.JWTPublicKeyFile = pubPath

	if err := cfg.LoadJWTKeyFiles(); err != nil {
		t.Fatalf("LoadJWTKeyFiles failed: %v", err)
	}

	if cfg.Auth.JWTPrivateKey != "PRIVATE-KEY-CONTENT" {
		t.Error("private key not loaded from file")
	}
	if cfg.Auth.JWTPublicKey != "PUBLIC-KEY-CONTENT" {
		t.Error("public key not loaded from file")
	}
}

func TestValidate(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Errorf("default config should be valid: %v", err)
	}

	bad := DefaultConfig()
	bad.Cend.Port = 0
	if err := bad.Validate(); err == nil {
		t.Error("expected error for invalid cend port")
	}

	bad2 := DefaultConfig()
	bad2.IoT.DTLSPort = -1
	if err := bad2.Validate(); err == nil {
		t.Error("expected error for invalid dtls port")
	}
}
