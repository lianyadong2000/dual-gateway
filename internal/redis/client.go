package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	client *redis.Client
	ctx    = context.Background()

	// 本地降级模式（Redis不可用时启用，单机可运行）
	localMode     bool
	localMu       sync.RWMutex
	localGateways = make(map[string]string)
	localOnline   = make(map[string]string)
	localIoTDev   = make(map[string]string)
)

// 连接信息
type ConnectionInfo struct {
	UserID       string `json:"user_id"`
	ConnectionID string `json:"connection_id"`
	GatewayID    string `json:"gateway_id"`
	GatewayType  string `json:"gateway_type"`
	Timestamp    int64  `json:"timestamp"`
}

// IoT设备信息
type IoTDeviceInfo struct {
	DeviceID  string `json:"device_id"`
	GatewayID string `json:"gateway_id"`
	Address   string `json:"address"`
	LastSeen  int64  `json:"last_seen"`
	Status    string `json:"status"` // online, offline, sleep
}

// 初始化Redis客户端；失败时自动降级为本地内存模式
func Init(addr, password string, db int, poolSize int) error {
	c := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		PoolSize:     poolSize,
		MinIdleConns: poolSize / 10,
		MaxRetries:   3,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
		IdleTimeout:  60 * time.Second,
	})

	// 测试连接
	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := c.Ping(pingCtx).Err(); err != nil {
		log.Printf("Redis unavailable (%v), falling back to local mode", err)
		localMode = true
		client = nil
		return nil
	}

	client = c
	localMode = false
	log.Printf("Connected to Redis: %s", addr)
	return nil
}

// 当前是否为本地降级模式
func IsLocalMode() bool {
	return localMode
}

// 注册网关
func RegisterGateway(gatewayID, gatewayType, addr string) error {
	data := map[string]interface{}{
		"id":   gatewayID,
		"type": gatewayType,
		"addr": addr,
		"time": time.Now().Unix(),
	}

	jsonData, _ := json.Marshal(data)

	if client == nil {
		localMu.Lock()
		localGateways[gatewayID] = string(jsonData)
		localMu.Unlock()
		return nil
	}
	return client.HSet(ctx, "gateways", gatewayID, jsonData).Err()
}

// 注销网关
func UnregisterGateway(gatewayID string) error {
	if client == nil {
		localMu.Lock()
		delete(localGateways, gatewayID)
		localMu.Unlock()
		return nil
	}
	return client.HDel(ctx, "gateways", gatewayID).Err()
}

// 获取网关信息
func GetGatewayInfo(gatewayID string) (map[string]interface{}, error) {
	var data string
	var err error

	if client == nil {
		localMu.RLock()
		data = localGateways[gatewayID]
		localMu.RUnlock()
		if data == "" {
			return nil, fmt.Errorf("gateway %s not found", gatewayID)
		}
	} else {
		data, err = client.HGet(ctx, "gateways", gatewayID).Result()
		if err != nil {
			return nil, err
		}
	}

	var info map[string]interface{}
	if err := json.Unmarshal([]byte(data), &info); err != nil {
		return nil, err
	}

	return info, nil
}

// 添加在线用户
func AddOnlineUser(userID, connectionID, gatewayID string) error {
	info := &ConnectionInfo{
		UserID:       userID,
		ConnectionID: connectionID,
		GatewayID:    gatewayID,
		GatewayType:  "cend",
		Timestamp:    time.Now().Unix(),
	}

	data, _ := json.Marshal(info)

	if client == nil {
		localMu.Lock()
		localOnline[userID] = string(data)
		localMu.Unlock()
		return nil
	}
	return client.HSet(ctx, "online_users", userID, data).Err()
}

// 移除在线用户
func RemoveOnlineUser(userID string) error {
	if client == nil {
		localMu.Lock()
		delete(localOnline, userID)
		localMu.Unlock()
		return nil
	}
	return client.HDel(ctx, "online_users", userID).Err()
}

// 获取用户连接信息
func GetUserConnection(userID string) (*ConnectionInfo, error) {
	var data string
	var err error

	if client == nil {
		localMu.RLock()
		data = localOnline[userID]
		localMu.RUnlock()
		if data == "" {
			return nil, fmt.Errorf("user %s not online", userID)
		}
	} else {
		data, err = client.HGet(ctx, "online_users", userID).Result()
		if err != nil {
			return nil, err
		}
	}

	var info ConnectionInfo
	if err := json.Unmarshal([]byte(data), &info); err != nil {
		return nil, err
	}

	return &info, nil
}

// 注册IoT设备
func RegisterIoTDevice(deviceID, gatewayID, address string) error {
	info := &IoTDeviceInfo{
		DeviceID:  deviceID,
		GatewayID: gatewayID,
		Address:   address,
		LastSeen:  time.Now().Unix(),
		Status:    "online",
	}

	data, _ := json.Marshal(info)

	if client == nil {
		localMu.Lock()
		localIoTDev[deviceID] = string(data)
		localMu.Unlock()
		return nil
	}
	return client.HSet(ctx, "iot_devices", deviceID, data).Err()
}

// 移除IoT设备
func RemoveIoTDevice(deviceID string) error {
	if client == nil {
		localMu.Lock()
		delete(localIoTDev, deviceID)
		localMu.Unlock()
		return nil
	}
	return client.HDel(ctx, "iot_devices", deviceID).Err()
}

// 获取IoT设备信息
func GetIoTDeviceInfo(deviceID string) (*IoTDeviceInfo, error) {
	var data string
	var err error

	if client == nil {
		localMu.RLock()
		data = localIoTDev[deviceID]
		localMu.RUnlock()
		if data == "" {
			return nil, fmt.Errorf("device %s not found", deviceID)
		}
	} else {
		data, err = client.HGet(ctx, "iot_devices", deviceID).Result()
		if err != nil {
			return nil, err
		}
	}

	var info IoTDeviceInfo
	if err := json.Unmarshal([]byte(data), &info); err != nil {
		return nil, err
	}

	return &info, nil
}

// 更新IoT设备心跳（Lua单往返：保留GatewayID/Address字段）
var luaHeartbeat = redis.NewScript(`
local v = redis.call('HGET', KEYS[1], ARGV[1])
if v then
  local info = cjson.decode(v)
  info['last_seen'] = tonumber(ARGV[2])
  info['status'] = 'online'
  redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(info))
  return 1
end
return 0
`)

func UpdateIoTDeviceHeartbeat(deviceID string) error {
	if client == nil {
		localMu.Lock()
		if info, ok := localIoTDev[deviceID]; ok {
			var old IoTDeviceInfo
			if err := json.Unmarshal([]byte(info), &old); err == nil {
				old.LastSeen = time.Now().Unix()
				old.Status = "online"
				data, _ := json.Marshal(old)
				localIoTDev[deviceID] = string(data)
			}
		}
		localMu.Unlock()
		return nil
	}
	return luaHeartbeat.Run(ctx, client, []string{"iot_devices"}, deviceID, time.Now().Unix()).Err()
}

// 发布消息
func PublishMessage(channel string, message []byte) error {
	if client == nil {
		// 本地降级模式：无跨进程订阅者，静默丢弃
		return nil
	}
	return client.Publish(ctx, channel, message).Err()
}

// 订阅消息
func SubscribeMessage(channel string, handler func([]byte)) {
	if client == nil {
		log.Printf("Local mode: message subscription disabled for channel %s", channel)
		return
	}

	pubsub := client.Subscribe(ctx, channel)
	defer pubsub.Close()

	ch := pubsub.Channel()
	for msg := range ch {
		handler([]byte(msg.Payload))
	}
}

// 获取Redis客户端
func GetClient() *redis.Client {
	return client
}

// 获取全局上下文
func Ctx() context.Context {
	return ctx
}
