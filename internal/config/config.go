package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// 配置结构
type Config struct {
	Runtime   RuntimeConfig   `yaml:"runtime" json:"runtime"`
	Log       LogConfig       `yaml:"log" json:"log"`
	Cend      CendConfig      `yaml:"cend" json:"cend"`
	IoT       IoTConfig       `yaml:"iot" json:"iot"`
	Redis     RedisConfig     `yaml:"redis" json:"redis"`
	Kafka     KafkaConfig     `yaml:"kafka" json:"kafka"`
	Auth      AuthConfig      `yaml:"auth" json:"auth"`
	Monitor   MonitorConfig   `yaml:"monitor" json:"monitor"`
	Shadow    ShadowConfig    `yaml:"shadow" json:"shadow"`
	RateLimit RateLimitConfig `yaml:"rate_limit" json:"rate_limit"`
	Admin     AdminConfig     `yaml:"admin" json:"admin"`
}

// 管理后台配置
type AdminConfig struct {
	Enabled  bool   `yaml:"enabled" json:"enabled"`    // 是否挂载管理后台（默认 true）
	BuildDir string `yaml:"build_dir" json:"build_dir"` // 压测二进制目录（默认 build）
}

// 运行时配置
type RuntimeConfig struct {
	MaxProcs int `yaml:"max_procs" json:"max_procs"`
}

// 日志配置
type LogConfig struct {
	Level  string `yaml:"level" json:"level"`
	Format string `yaml:"format" json:"format"`
	Output string `yaml:"output" json:"output"`
}

// C端配置
type CendConfig struct {
	Host              string `yaml:"host" json:"host"`
	Port              int    `yaml:"port" json:"port"`
	MaxConnections    int64  `yaml:"max_connections" json:"max_connections"`
	HeartbeatTimeout  int64  `yaml:"heartbeat_timeout" json:"heartbeat_timeout"`
	ReadBufferSize    int    `yaml:"read_buffer_size" json:"read_buffer_size"`
	WriteBufferSize   int    `yaml:"write_buffer_size" json:"write_buffer_size"`
	EnableCompression bool   `yaml:"enable_compression" json:"enable_compression"`
	MaxMessageSize    int    `yaml:"max_message_size" json:"max_message_size"`
}

// IoT配置
type IoTConfig struct {
	Host           string `yaml:"host" json:"host"`
	Port           int    `yaml:"port" json:"port"`
	DTLSPort       int    `yaml:"dtls_port" json:"dtls_port"` // DTLS独立端口，0表示禁用DTLS
	PSK            string `yaml:"psk" json:"psk"`
	DTLSEnabled    bool   `yaml:"dtls_enabled" json:"dtls_enabled"`
	MaxPacketSize  int    `yaml:"max_packet_size" json:"max_packet_size"`
	ReadTimeout    int    `yaml:"read_timeout" json:"read_timeout"`
	WriteTimeout   int    `yaml:"write_timeout" json:"write_timeout"`
	SessionTimeout int    `yaml:"session_timeout" json:"session_timeout"`
	WorkerCount    int    `yaml:"worker_count" json:"worker_count"`

	// MQTT-SN over QUIC（三路协议并存，独立开关；启用时 quic_port 必须有效）
	QUICEnabled bool   `yaml:"quic_enabled" json:"quic_enabled"`
	QUICPort    int    `yaml:"quic_port" json:"quic_port"`       // QUIC 监听端口，0 表示禁用
	QUICCert    string `yaml:"quic_cert" json:"quic_cert"`       // TLS 证书（PEM）；不存在时自动生成自签证书
	QUICKey     string `yaml:"quic_key" json:"quic_key"`         // TLS 私钥（PEM）
	QUICIdleSec int    `yaml:"quic_idle_sec" json:"quic_idle_sec"` // QUIC 连接空闲超时（秒），默认 300
}

// Redis配置
type RedisConfig struct {
	Addr         string `yaml:"addr" json:"addr"`
	Password     string `yaml:"password" json:"password"`
	DB           int    `yaml:"db" json:"db"`
	PoolSize     int    `yaml:"pool_size" json:"pool_size"`
	MinIdleConns int    `yaml:"min_idle_conns" json:"min_idle_conns"`
	MaxRetries   int    `yaml:"max_retries" json:"max_retries"`
}

// Kafka配置
type KafkaConfig struct {
	Brokers   []string `yaml:"brokers" json:"brokers"`
	Topic     string   `yaml:"topic" json:"topic"`
	GroupID   string   `yaml:"group_id" json:"group_id"`
	BatchSize int      `yaml:"batch_size" json:"batch_size"`
}

// 鉴权配置
type AuthConfig struct {
	JWTPrivateKey     string            `yaml:"jwt_private_key" json:"jwt_private_key"`
	JWTPublicKey      string            `yaml:"jwt_public_key" json:"jwt_public_key"`
	JWTPrivateKeyFile string            `yaml:"jwt_private_key_file" json:"jwt_private_key_file"`
	JWTPublicKeyFile  string            `yaml:"jwt_public_key_file" json:"jwt_public_key_file"`
	JWTIssuer         string            `yaml:"jwt_issuer" json:"jwt_issuer"`
	JWTTTLSeconds     int64             `yaml:"jwt_ttl" json:"jwt_ttl"` // Token有效期（秒）
	PSKDevices        map[string]string `yaml:"psk_devices" json:"psk_devices"`
}

// Token有效期（time.Duration形式）
func (a *AuthConfig) JWTTTL() time.Duration {
	if a.JWTTTLSeconds <= 0 {
		return 2 * time.Hour
	}
	return time.Duration(a.JWTTTLSeconds) * time.Second
}

// 监控配置
type MonitorConfig struct {
	Host string `yaml:"host" json:"host"`
	Port int    `yaml:"port" json:"port"`
}

// 设备影子配置
type ShadowConfig struct {
	Enabled                bool  `yaml:"enabled" json:"enabled"`
	OfflineTTLSeconds      int   `yaml:"offline_ttl_seconds" json:"offline_ttl_seconds"`
	PreoccupyTimeoutSeconds int  `yaml:"preoccupy_timeout_seconds" json:"preoccupy_timeout_seconds"`
	CmdTTLSeconds          int   `yaml:"cmd_ttl_seconds" json:"cmd_ttl_seconds"`
	MaxCmdBatch            int   `yaml:"max_cmd_batch" json:"max_cmd_batch"`
	PushEnabled            bool  `yaml:"push_enabled" json:"push_enabled"`
	GeoEnabled             bool  `yaml:"geo_enabled" json:"geo_enabled"`
	OfflineScanSeconds     int   `yaml:"offline_scan_seconds" json:"offline_scan_seconds"`
}

// 接入限流配置
type RateLimitConfig struct {
	Enabled        bool `yaml:"enabled" json:"enabled"`
	ConnectRate    int  `yaml:"connect_rate" json:"connect_rate"`
	ConnectBurst   int  `yaml:"connect_burst" json:"connect_burst"`
	UDPPerIPRate   int  `yaml:"udp_per_ip_rate" json:"udp_per_ip_rate"`
	UDPPerIPBurst  int  `yaml:"udp_per_ip_burst" json:"udp_per_ip_burst"`
	CendAcceptRate int  `yaml:"cend_accept_rate" json:"cend_accept_rate"`
}

// 默认配置
func DefaultConfig() *Config {
	return &Config{
		Runtime: RuntimeConfig{
			MaxProcs: 4,
		},
		Log: LogConfig{
			Level:  "info",
			Format: "json",
			Output: "stdout",
		},
		Cend: CendConfig{
			Host:              "0.0.0.0",
			Port:              8080,
			MaxConnections:    100000,
			HeartbeatTimeout:  120,
			ReadBufferSize:    65536,
			WriteBufferSize:   65536,
			EnableCompression: true,
			MaxMessageSize:    1048576,
		},
		IoT: IoTConfig{
			Host:           "0.0.0.0",
			Port:           5683,
			DTLSPort:       5684,
			DTLSEnabled:    true,
			MaxPacketSize:  65535,
			ReadTimeout:    30,
			WriteTimeout:   10,
			SessionTimeout: 300,
			WorkerCount:    2048,
			QUICEnabled:    false,
			QUICPort:       5685,
			QUICCert:       "config/keys/quic_cert.pem",
			QUICKey:        "config/keys/quic_key.pem",
			QUICIdleSec:    300,
		},
		Redis: RedisConfig{
			Addr:         "localhost:6379",
			DB:           0,
			PoolSize:     300,
			MinIdleConns: 10,
			MaxRetries:   3,
		},
		Kafka: KafkaConfig{
			Brokers:   []string{"localhost:9092"},
			Topic:     "gateway_messages",
			GroupID:   "gateway_group",
			BatchSize: 100,
		},
		Auth: AuthConfig{
			JWTIssuer:     "dual-gateway",
			JWTTTLSeconds: 7200,
		},
		Monitor: MonitorConfig{
			Host: "0.0.0.0",
			Port: 9090,
		},
		Shadow: ShadowConfig{
			Enabled:                 true,
			OfflineTTLSeconds:       120,
			PreoccupyTimeoutSeconds: 60,
			CmdTTLSeconds:           86400,
			MaxCmdBatch:             10,
			PushEnabled:             true,
			GeoEnabled:              true,
			OfflineScanSeconds:      15,
		},
		Admin: AdminConfig{
			Enabled:  true,
			BuildDir: "build",
		},
		RateLimit: RateLimitConfig{
			Enabled:       true,
			ConnectRate:   10000,
			ConnectBurst:  10000,
			UDPPerIPRate:  10000,
			UDPPerIPBurst: 20000,
			CendAcceptRate: 5000,
		},
	}
}

// 加载配置
func Load(path string) (*Config, error) {
	config := DefaultConfig()

	// 读取配置文件
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// 根据文件扩展名解析
	switch {
	case hasSuffix(path, ".yaml", ".yml"):
		if err := yaml.Unmarshal(data, config); err != nil {
			return nil, fmt.Errorf("failed to parse YAML config: %w", err)
		}
	case hasSuffix(path, ".json"):
		if err := json.Unmarshal(data, config); err != nil {
			return nil, fmt.Errorf("failed to parse JSON config: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported config file format: %s", path)
	}

	// 验证配置
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// 应用环境变量覆盖
	config.ApplyEnvOverrides()

	// 从文件加载JWT密钥（若配置了文件路径）
	if err := config.LoadJWTKeyFiles(); err != nil {
		return nil, fmt.Errorf("failed to load JWT key files: %w", err)
	}

	return config, nil
}

// LoadJWTKeyFiles 从文件加载JWT密钥（文件路径优先于内嵌内容）
func (c *Config) LoadJWTKeyFiles() error {
	if c.Auth.JWTPrivateKeyFile != "" {
		data, err := os.ReadFile(c.Auth.JWTPrivateKeyFile)
		if err != nil {
			return fmt.Errorf("failed to read private key file: %w", err)
		}
		c.Auth.JWTPrivateKey = string(data)
	}

	if c.Auth.JWTPublicKeyFile != "" {
		data, err := os.ReadFile(c.Auth.JWTPublicKeyFile)
		if err != nil {
			return fmt.Errorf("failed to read public key file: %w", err)
		}
		c.Auth.JWTPublicKey = string(data)
	}

	return nil
}

// 验证配置
func (c *Config) Validate() error {
	if c.Runtime.MaxProcs <= 0 {
		return fmt.Errorf("max_procs must be positive")
	}

	if c.Cend.Port <= 0 || c.Cend.Port > 65535 {
		return fmt.Errorf("invalid C-end port: %d", c.Cend.Port)
	}

	if c.IoT.Port <= 0 || c.IoT.Port > 65535 {
		return fmt.Errorf("invalid IoT port: %d", c.IoT.Port)
	}

	if c.IoT.DTLSPort != 0 && (c.IoT.DTLSPort <= 0 || c.IoT.DTLSPort > 65535) {
		return fmt.Errorf("invalid IoT DTLS port: %d", c.IoT.DTLSPort)
	}
	if c.IoT.QUICEnabled {
		if c.IoT.QUICPort <= 0 || c.IoT.QUICPort > 65535 {
			return fmt.Errorf("invalid IoT QUIC port: %d", c.IoT.QUICPort)
		}
		if c.IoT.QUICPort == c.IoT.Port || c.IoT.QUICPort == c.IoT.DTLSPort {
			return fmt.Errorf("IoT QUIC port conflicts with UDP/DTLS port: %d", c.IoT.QUICPort)
		}
	}

	if c.Redis.PoolSize <= 0 {
		return fmt.Errorf("redis pool size must be positive")
	}

	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("kafka brokers cannot be empty")
	}

	return nil
}

// 应用环境变量覆盖
func (c *Config) ApplyEnvOverrides() {
	if v := os.Getenv("GATEWAY_ID"); v != "" {
		// 设置网关ID
	}

	if v := os.Getenv("REDIS_ADDR"); v != "" {
		c.Redis.Addr = v
	}

	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		// 解析逗号分隔的brokers
		c.Kafka.Brokers = splitAndTrim(v, ",")
	}

	if v := os.Getenv("LOG_LEVEL"); v != "" {
		c.Log.Level = v
	}
}

// 辅助函数
func hasSuffix(s string, suffixes ...string) bool {
	for _, suffix := range suffixes {
		if len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix {
			return true
		}
	}
	return false
}

func splitAndTrim(s, sep string) []string {
	var result []string
	start := 0
	for i := 0; i < len(s); i++ {
		if string(s[i]) == sep {
			if part := trimSpace(s[start:i]); part != "" {
				result = append(result, part)
			}
			start = i + 1
		}
	}
	if part := trimSpace(s[start:]); part != "" {
		result = append(result, part)
	}
	return result
}

func trimSpace(s string) string {
	// 简单实现
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}
