package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"dual-gateway/pkg/utils"
)

// JWT管理器
type JWTManager struct {
	privateKey *rsa.PrivateKey
	publicKey  *rsa.PublicKey
	issuer     string
	ttl        time.Duration
}

// JWT Claims
type Claims struct {
	UserID   string `json:"user_id"`
	DeviceID string `json:"device_id"`
	jwt.RegisteredClaims
}

// 创建JWT管理器
func NewJWTManager(privateKeyPEM, publicKeyPEM []byte, issuer string, ttl time.Duration) (*JWTManager, error) {
	// 解析私钥
	privateBlock, _ := pem.Decode(privateKeyPEM)
	if privateBlock == nil {
		return nil, errors.New("failed to parse private key PEM")
	}

	// 尝试解析 PKCS1 格式
	privateKey, err := x509.ParsePKCS1PrivateKey(privateBlock.Bytes)
	if err != nil {
		// 尝试解析 PKCS8 格式
		parsedKey, err2 := x509.ParsePKCS8PrivateKey(privateBlock.Bytes)
		if err2 != nil {
			return nil, fmt.Errorf("failed to parse private key: %v (PKCS1) / %v (PKCS8)", err, err2)
		}
		var ok bool
		privateKey, ok = parsedKey.(*rsa.PrivateKey)
		if !ok {
			return nil, errors.New("private key is not RSA type")
		}
	}

	// 解析公钥
	publicBlock, _ := pem.Decode(publicKeyPEM)
	if publicBlock == nil {
		return nil, errors.New("failed to parse public key PEM")
	}

	// 尝试解析 PKIX 格式
	publicKeyInterface, err := x509.ParsePKIXPublicKey(publicBlock.Bytes)
	if err != nil {
		// 尝试解析 PKCS1 格式
		parsedPub, err2 := x509.ParsePKCS1PublicKey(publicBlock.Bytes)
		if err2 != nil {
			return nil, fmt.Errorf("failed to parse public key: %v (PKIX) / %v (PKCS1)", err, err2)
		}
		publicKeyInterface = parsedPub
	}

	publicKey, ok := publicKeyInterface.(*rsa.PublicKey)
	if !ok {
		return nil, errors.New("public key is not RSA type")
	}

	return &JWTManager{
		privateKey: privateKey,
		publicKey:  publicKey,
		issuer:     issuer,
		ttl:        ttl,
	}, nil
}

// 生成Token
func (m *JWTManager) GenerateToken(userID, deviceID string) (string, error) {
	now := time.Now()

	claims := &Claims{
		UserID:   userID,
		DeviceID: deviceID,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    m.issuer,
			Subject:   userID,
			Audience:  jwt.ClaimStrings{"dual-gateway"},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(m.ttl)),
			NotBefore: jwt.NewNumericDate(now),
			ID:        utils.GenerateTraceID(), // 使用 utils 包中的工具
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	return token.SignedString(m.privateKey)
}

// 验证Token
func (m *JWTManager) ValidateToken(tokenString string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		// 验证签名方法
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return m.publicKey, nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to parse token: %w", err)
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, errors.New("invalid token")
	}

	// 验证issuer
	if claims.Issuer != m.issuer {
		return nil, errors.New("invalid issuer")
	}

	// 验证过期时间
	if claims.ExpiresAt != nil && claims.ExpiresAt.Time.Before(time.Now()) {
		return nil, errors.New("token expired")
	}

	return claims, nil
}

// 刷新Token
func (m *JWTManager) RefreshToken(tokenString string) (string, error) {
	claims, err := m.ValidateToken(tokenString)
	if err != nil {
		return "", err
	}

	// 检查是否在刷新窗口内（过期前5分钟）
	if claims.ExpiresAt != nil {
		timeUntilExpiry := time.Until(claims.ExpiresAt.Time)
		if timeUntilExpiry > 5*time.Minute {
			return "", errors.New("token not ready for refresh")
		}
	}

	return m.GenerateToken(claims.UserID, claims.DeviceID)
}

// 生成RSA密钥对（用于测试或初始化）
func GenerateRSAKeyPair(bits int) (privateKeyPEM, publicKeyPEM []byte, err error) {
	if bits < 2048 {
		return nil, nil, errors.New("RSA key size must be at least 2048 bits")
	}

	privateKey, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate RSA key: %w", err)
	}

	// 编码私钥
	privateKeyBytes := x509.MarshalPKCS1PrivateKey(privateKey)
	privateKeyPEM = pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: privateKeyBytes,
	})

	// 编码公钥
	publicKeyBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal public key: %w", err)
	}
	publicKeyPEM = pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: publicKeyBytes,
	})

	return privateKeyPEM, publicKeyPEM, nil
}

// 从文件加载RSA密钥
func LoadRSAKeys(privateKeyPath, publicKeyPath string) (privateKeyPEM, publicKeyPEM []byte, err error) {
	privateKeyPEM, err = os.ReadFile(privateKeyPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read private key: %w", err)
	}

	publicKeyPEM, err = os.ReadFile(publicKeyPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read public key: %w", err)
	}

	return privateKeyPEM, publicKeyPEM, nil
}


// EnsureRSAKeys 确保JWT密钥有效；无效时自动生成RSA密钥对并回填配置（开发/单机模式）
func EnsureRSAKeys(priv, pub *string, issuer string) (*JWTManager, error) {
	if *priv != "" && *pub != "" {
		if m, err := NewJWTManager([]byte(*priv), []byte(*pub), issuer, 2*time.Hour); err == nil {
			return m, nil
		}
	}

	// 生成新的RSA密钥对
	privPEM, pubPEM, err := GenerateRSAKeyPair(2048)
	if err != nil {
		return nil, fmt.Errorf("failed to generate RSA key pair: %w", err)
	}

	*priv = string(privPEM)
	*pub = string(pubPEM)

	m, err := NewJWTManager(privPEM, pubPEM, issuer, 2*time.Hour)
	if err != nil {
		return nil, fmt.Errorf("failed to create JWT manager with generated keys: %w", err)
	}
	return m, nil
}
