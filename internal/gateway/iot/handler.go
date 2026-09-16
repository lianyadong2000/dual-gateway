package iot

import (
	"encoding/binary"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"dual-gateway/internal/mq"
	"dual-gateway/internal/protocol/iot/mqttsn"
	"dual-gateway/internal/protocol/iot/tlv"
	"dual-gateway/internal/redis"
	"dual-gateway/internal/shadow"
	"dual-gateway/pkg/logger"
	"dual-gateway/pkg/utils"
)

// 消息处理器
type MessageHandler struct {
	Logger         logger.Logger
	DeviceManager  *DeviceManager
	SessionManager *SessionManager
	PacketPool     *utils.ByteSlicePool
	Metrics        *Metrics
}

// 设备管理器
type DeviceManager struct {
	mu      sync.RWMutex
	devices map[string]*DeviceInfo
}

// Get 按设备ID查询（nil=不在线）
func (m *DeviceManager) Get(deviceID string) *DeviceInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.devices[deviceID]
}

// 设备信息
type DeviceInfo struct {
	DeviceID  string
	GatewayID string
	Address   string
	LastSeen  time.Time
	Status    string
	PSK       []byte
	SessionID string
	Peer      *Peer // 传输端点（QUIC/DTLS 时用于在线直发回包；UDP 时为地址端点）
}

// 会话管理器
type SessionManager struct {
	mu       sync.RWMutex
	sessions map[string]*Session
	byDevice map[string]string // deviceID -> sessionID 索引，O(1)查找
}

// 会话
type Session struct {
	ID         string
	DeviceID   string
	CreatedAt  time.Time
	LastActive time.Time
	KeepAlive  uint16
	Clean      bool
}

// 指标
type Metrics struct {
	PacketsReceived atomic.Int64
	PacketsSent     atomic.Int64
	BytesReceived   atomic.Int64
	BytesSent       atomic.Int64
	ConnectCount    atomic.Int64
	DisconnectCount atomic.Int64
	PublishCount    atomic.Int64
	SubscribeCount  atomic.Int64
	ErrorCount      atomic.Int64
}

// 创建消息处理器
func NewMessageHandler(logger logger.Logger) *MessageHandler {
	return &MessageHandler{
		Logger: logger,
		DeviceManager: &DeviceManager{
			devices: make(map[string]*DeviceInfo),
		},
		SessionManager: &SessionManager{
			sessions: make(map[string]*Session),
			byDevice: make(map[string]string),
		},
		PacketPool: utils.NewByteSlicePool(65535),
		Metrics:    &Metrics{},
	}
}

// 处理数据包（Peer 统一 UDP/DTLS/QUIC 传输）
func (h *MessageHandler) HandlePacket(server *Server, data []byte, peer *Peer) {
	msgType, msgData, err := mqttsn.ParseMessage(data)
	if err != nil {
		// 无效包属于高频路径，降级为Debug日志避免日志风暴阻塞worker
		h.Logger.Debug("Invalid packet", "error", err, "from", peer.String())
		h.Metrics.ErrorCount.Add(1)
		return
	}

	h.Metrics.PacketsReceived.Add(1)
	h.Metrics.BytesReceived.Add(int64(len(data)))

	// 处理不同类型的消息
	switch msgType {
	case mqttsn.CONNECT:
		h.handleConnect(server, msgData, peer)

	case mqttsn.PUBLISH:
		h.handlePublish(server, msgData, peer)

	case mqttsn.PINGREQ:
		h.handlePingReq(server, msgData, peer)

	case mqttsn.DISCONNECT:
		h.handleDisconnect(server, msgData, peer)

	case mqttsn.SUBSCRIBE:
		h.handleSubscribe(server, msgData, peer)

	case mqttsn.UNSUBSCRIBE:
		h.handleUnsubscribe(server, msgData, peer)

	case mqttsn.REGISTER:
		h.handleRegister(server, msgData, peer)

	case mqttsn.STATUS:
		h.handleStatus(server, msgData, peer)

	case mqttsn.FETCH:
		h.handleFetch(server, msgData, peer)

	case mqttsn.CMDACK:
		h.handleCmdAck(server, msgData, peer)

	default:
		h.Logger.Debug("Unknown message type", "type", msgType)
	}
}

// 处理CONNECT消息
func (h *MessageHandler) handleConnect(server *Server, data []byte, peer *Peer) {
	connectMsg, err := mqttsn.DecodeConnect(data)
	if err != nil {
		h.Logger.Error("Failed to decode CONNECT", "error", err)
		h.Metrics.ErrorCount.Add(1)
		return
	}

	// 验证设备：优先查PSK库，兜底使用默认PSK（若配置）
	deviceID := connectMsg.ClientID
	if deviceID == "" {
		h.Logger.Warn("Empty device ID")
		h.sendConnack(server, peer, 0x02) // Rejected: Not authorized
		return
	}

	// 接入限流（防重连风暴/洪峰）：CONNECT 令牌桶 + 源IP限流
	// 超限回 CONNACK 0x03 (Server Unavailable) + RETRYINFO 重试等待
	if !server.ConnectLimiter.Allow() || !server.IPLimiter.Allow(peer.IP()) {
		h.Logger.Debug("Connect rate limited",
			"device_id", deviceID, "from", peer.String(),
			"accepted", server.ConnectLimiter.Accepted.Load(),
			"rejected", server.ConnectLimiter.Rejected.Load())
		h.sendConnack(server, peer, 0x03) // Server Unavailable
		h.sendRetryInfo(server, peer, 10) // 建议10秒后重试（客户端应指数退避+抖动）
		return
	}

	if _, ok := server.PSKStore.GetPSK(deviceID); !ok {
		if server.DefaultPSK == nil {
			h.Logger.Warn("Device not found", "device_id", deviceID)
			h.sendConnack(server, peer, 0x02) // Rejected: Not authorized
			return
		}
	}

	// 获取PSK（用于设备信息记录）
	var psk []byte
	if p, ok := server.PSKStore.GetPSK(deviceID); ok {
		psk = p
	} else {
		psk = server.DefaultPSK
	}

	// 创建设备信息
	deviceInfo := &DeviceInfo{
		DeviceID:  deviceID,
		GatewayID: server.ID,
		Address:   peer.String(),
		LastSeen:  time.Now(),
		Status:    "online",
		PSK:       psk,
		Peer:      peer,
	}

	// 注册设备
	h.DeviceManager.mu.Lock()
	h.DeviceManager.devices[deviceID] = deviceInfo
	h.DeviceManager.mu.Unlock()

	// 创建会话
	sessionID := utils.GenerateSessionID()
	session := &Session{
		ID:         sessionID,
		DeviceID:   deviceID,
		CreatedAt:  time.Now(),
		LastActive: time.Now(),
		KeepAlive:  connectMsg.Duration,
		Clean:      connectMsg.Flags&0x02 != 0,
	}

	// 同设备重连时清理旧会话（避免会话表膨胀）
	h.SessionManager.mu.Lock()
	if oldID, ok := h.SessionManager.byDevice[deviceID]; ok {
		delete(h.SessionManager.sessions, oldID)
	}
	h.SessionManager.sessions[sessionID] = session
	h.SessionManager.byDevice[deviceID] = sessionID
	h.SessionManager.mu.Unlock()

	h.Metrics.ConnectCount.Add(1)

	// 发送CONNACK（主路径纯本地，微秒级返回；5万并发握手不阻塞）
	h.sendConnack(server, peer, 0x00) // Accepted

	// Redis 侧工作异步化（Register影子归属 + Touch在线 + 唤醒补发指令）：
	// 不阻塞 CONNECT 主路径，避免 5 万并发打满 Redis 连接池拖垮 worker
	asyncRedisWork := func() {
		if err := redis.RegisterIoTDevice(deviceID, server.ID, peer.String()); err != nil {
			h.Logger.Debug("Failed to register device in Redis", "error", err)
		}
		if err := server.Shadow.Touch(deviceID, server.ID); err != nil {
			h.Logger.Debug("Shadow touch failed", "device_id", deviceID, "error", err)
		}
		h.dispatchPendingCommands(server, deviceID)
	}
	select {
	case server.AsyncRedis <- asyncRedisWork:
	default:
		// 异步队列满：降级为同步执行（兜底，保证语义不丢）
		asyncRedisWork()
	}

	// 高频路径，连接日志降为Debug避免高并发刷屏
	h.Logger.Debug("Device connected",
		"device_id", deviceID,
		"session_id", sessionID,
		"keepalive", connectMsg.Duration,
		"clean_session", session.Clean,
	)
}

// dispatchPendingCommands 拉取该设备待执行指令并下发（桩在线时；QUIC/DTLS 走连接流）
func (h *MessageHandler) dispatchPendingCommands(server *Server, deviceID string) {
	// 仅在设备在线时下发
	dev := h.DeviceManager.Get(deviceID)
	if dev == nil {
		h.Logger.Debug("dispatch: device not in local table", "device_id", deviceID)
		return
	}
	cmds, err := server.CmdQueue.Fetch(deviceID, 0) // limit 0 = 默认批量
	if err != nil {
		h.Logger.Debug("dispatch: fetch failed", "device_id", deviceID, "error", err)
		return
	}
	h.Logger.Debug("dispatch: fetched commands", "device_id", deviceID, "count", len(cmds))
	for _, c := range cmds {
		payload := []byte{}
		if len(c.Params) > 0 {
			if b, err := json.Marshal(c.Params); err == nil {
				payload = b
			}
		}
		cmdMsg := mqttsn.EncodeCmd(deviceID, c.CmdID, mqttsn.CmdTypeToByte(c.Type), payload)
		if err := server.SendToDevice(deviceID, cmdMsg); err != nil {
			h.Metrics.ErrorCount.Add(1)
			h.Logger.Error("Command send failed", "device_id", deviceID, "cmd_id", c.CmdID, "error", err)
			return
		}
		h.Metrics.PacketsSent.Add(1)
		h.Metrics.BytesSent.Add(int64(len(cmdMsg)))
		h.Logger.Debug("Command delivered", "device_id", deviceID, "cmd_id", c.CmdID, "type", c.Type)
	}
}

// 处理STATUS消息：桩主动状态上报（充电开始/结束/故障等，驱动影子）
func (h *MessageHandler) handleStatus(server *Server, data []byte, peer *Peer) {
	deviceID, stateByte, err := mqttsn.DecodeStatus(data)
	if err != nil {
		h.Metrics.ErrorCount.Add(1)
		return
	}

	// 设备必须已连接
	h.DeviceManager.mu.Lock()
	dev, ok := h.DeviceManager.devices[deviceID]
	if ok {
		dev.LastSeen = time.Now()
		dev.Status = "online"
	}
	h.DeviceManager.mu.Unlock()
	if !ok {
		h.Logger.Debug("STATUS from unknown device", "device_id", deviceID)
		return
	}

	stateStr := mqttsn.ByteToStateStr[stateByte]
	if stateStr == "" {
		h.Logger.Debug("Unknown status byte", "device_id", deviceID, "state", stateByte)
		return
	}

	// 更新影子（桩权威源：迁移白名单校验 + 版本递增 + 事件发布）
	if err := server.Shadow.ReportHeartbeatWithGateway(deviceID, stateStr, server.ID); err != nil {
		h.Logger.Debug("Shadow update failed", "device_id", deviceID, "state", stateStr, "error", err)
	}

	h.Logger.Debug("Device status report",
		"device_id", deviceID, "state", stateStr, "from", peer.String())
}

// 处理FETCH消息：桩唤醒后主动拉取待执行指令
func (h *MessageHandler) handleFetch(server *Server, data []byte, peer *Peer) {
	deviceID, err := mqttsn.DecodeFetch(data)
	if err != nil {
		h.Metrics.ErrorCount.Add(1)
		return
	}
	h.dispatchPendingCommands(server, deviceID)
}

// 处理CMDACK消息：桩指令执行确认（结果闭环 -> 影子 + C端推送）
func (h *MessageHandler) handleCmdAck(server *Server, data []byte, peer *Peer) {
	deviceID, cmdID, code, resultState, err := mqttsn.DecodeCmdAck(data)
	if err != nil {
		h.Metrics.ErrorCount.Add(1)
		return
	}

	// 从队列移除（执行完成）
	if err := server.CmdQueue.MarkDone(deviceID, cmdID); err != nil {
		h.Logger.Debug("CmdAck MarkDone failed", "cmd_id", cmdID, "error", err)
	}

	// 桩确认的状态若有效，同步影子（如 charge.start -> charging）
	if stateStr := mqttsn.ByteToStateStr[resultState]; stateStr != "" && stateStr != "offline" {
		server.Shadow.ReportHeartbeatWithGateway(deviceID, stateStr, server.ID)
	}

	h.Logger.Debug("Command ack received",
		"device_id", deviceID, "cmd_id", cmdID, "code", code, "result_state", resultState)

	// 发布执行结果（C端网关订阅后推送用户）
	result := map[string]interface{}{
		"cmd_id":       cmdID,
		"device_id":    deviceID,
		"code":         code,
		"result_state": mqttsn.ByteToStateStr[resultState],
		"timestamp":    time.Now().UnixMilli(),
	}
	if data, err := json.Marshal(result); err == nil {
		redis.PublishMessage(shadow.CmdResultChan, data)
	}
}

// sendRetryInfo 发送重试指示（限流拒绝后，告知桩等待秒数再重连）
func (h *MessageHandler) sendRetryInfo(server *Server, peer *Peer, retryAfterSec uint16) {
	msg := mqttsn.EncodeRetryInfo(retryAfterSec)
	if err := peer.Send(server, msg); err != nil {
		h.Metrics.ErrorCount.Add(1)
		return
	}
	h.Metrics.PacketsSent.Add(1)
	h.Metrics.BytesSent.Add(int64(len(msg)))
}

// 处理PUBLISH消息
func (h *MessageHandler) handlePublish(server *Server, data []byte, peer *Peer) {
	publishMsg, err := mqttsn.DecodePublish(data)
	if err != nil {
		h.Logger.Error("Failed to decode PUBLISH", "error", err)
		h.Metrics.ErrorCount.Add(1)
		return
	}

	// 解析TLV数据
	decoder := tlv.NewDecoder(publishMsg.Data)
	tlvs, err := decoder.DecodeAll()
	if err != nil {
		h.Logger.Error("Failed to decode TLV", "error", err)
		h.Metrics.ErrorCount.Add(1)
		return
	}

	// 提取设备ID和数据
	var deviceID string
	values := make(map[string]interface{})

	for _, tlvItem := range tlvs {
		switch tlvItem.Type {
		case tlv.TypeDeviceID:
			deviceID = tlvItem.GetString()

		case tlv.TypeTemperature:
			if temp, err := tlvItem.GetUint16(); err == nil {
				values["temperature"] = float32(temp) / 100
			}

		case tlv.TypeHumidity:
			if humidity, err := tlvItem.GetUint16(); err == nil {
				values["humidity"] = float32(humidity) / 100
			}

		case tlv.TypeVoltage:
			if voltage, err := tlvItem.GetUint16(); err == nil {
				values["voltage"] = float32(voltage) / 1000
			}

		case tlv.TypeBattery:
			if battery, err := tlvItem.GetUint8(); err == nil {
				values["battery"] = battery
			}

		case tlv.TypeSignal:
			if signal, err := tlvItem.GetUint8(); err == nil {
				values["signal"] = signal
			}

		case tlv.TypeStatus:
			if status, err := tlvItem.GetUint8(); err == nil {
				values["status"] = status
			}

		case tlv.TypeAlarm:
			if alarm, err := tlvItem.GetUint8(); err == nil {
				values["alarm"] = alarm
			}

		case tlv.TypeGPS:
			values["gps"] = tlvItem.Value
		}
	}

	if deviceID == "" {
		h.Logger.Warn("Device ID not found in TLV data")
		return
	}

	// 更新设备最后活跃时间
	h.DeviceManager.mu.Lock()
	if device, ok := h.DeviceManager.devices[deviceID]; ok {
		device.LastSeen = time.Now()
		device.Status = "online"
	}
	h.DeviceManager.mu.Unlock()

	// 影子：心跳刷新在线；TLV 携带状态字段时上报状态（桩权威源）
	var stateStr string
	if statusVal, ok := values["status"]; ok {
		if sv, ok2 := statusVal.(uint8); ok2 {
			if s, ok3 := shadow.StateFromTLV[sv]; ok3 {
				stateStr = s
			}
		}
	}
	if err := server.Shadow.ReportHeartbeatWithGateway(deviceID, stateStr, server.ID); err != nil {
		h.Logger.Debug("Shadow heartbeat failed", "device_id", deviceID, "error", err)
	}
	// 同步Redis设备心跳（保留字段）
	redis.UpdateIoTDeviceHeartbeat(deviceID)

	// 构造事件
	event := map[string]interface{}{
		"device_id":  deviceID,
		"topic_id":   publishMsg.TopicID,
		"msg_id":     publishMsg.MsgID,
		"qos":        (publishMsg.Flags & 0x60) >> 5,
		"retain":     publishMsg.Flags&0x10 != 0,
		"timestamp":  time.Now().Unix(),
		"values":     values,
		"gateway_id": server.ID,
	}

	// 发布到Kafka（失败降级，不阻塞协议响应）
	if err := mq.PublishIoTEvent(event); err != nil {
		h.Logger.Debug("Failed to publish event to Kafka", "error", err)
	}

	h.Metrics.PublishCount.Add(1)

	// 根据QoS发送确认
	qos := (publishMsg.Flags & 0x60) >> 5
	switch qos {
	case 0:
		// QoS 0: 无需确认

	case 1:
		// QoS 1: 发送PUBACK
		h.sendPuback(server, peer, publishMsg.TopicID, publishMsg.MsgID, 0x00)

	case 2:
		// QoS 2: 发送PUBREC（简化处理，直接发送PUBACK）
		h.sendPuback(server, peer, publishMsg.TopicID, publishMsg.MsgID, 0x00)
	}

	h.Logger.Debug("Message published",
		"device_id", deviceID,
		"topic_id", publishMsg.TopicID,
		"msg_id", publishMsg.MsgID,
		"qos", qos,
		"values", values,
	)
}

// 处理PINGREQ消息（MQTT-SN允许携带ClientId以更新会话活跃时间）
func (h *MessageHandler) handlePingReq(server *Server, data []byte, peer *Peer) {
	// 若携带ClientId，更新对应设备的活跃时间与在线状态
	if len(data) > 0 {
		deviceID := string(data)
		h.DeviceManager.mu.Lock()
		if device, ok := h.DeviceManager.devices[deviceID]; ok {
			device.LastSeen = time.Now()
			device.Status = "online"
		}
		h.DeviceManager.mu.Unlock()

		h.SessionManager.mu.Lock()
		if sessionID, ok := h.SessionManager.byDevice[deviceID]; ok {
			if session, ok2 := h.SessionManager.sessions[sessionID]; ok2 {
				session.LastActive = time.Now()
			}
		}
		h.SessionManager.mu.Unlock()

		// 影子：心跳刷新在线标记（不携带业务状态）
		if err := server.Shadow.ReportHeartbeatWithGateway(deviceID, "", server.ID); err != nil {
			h.Logger.Debug("Shadow heartbeat failed", "device_id", deviceID, "error", err)
		}
	}

	// 发送PINGRESP
	pingResp := []byte{0x02, mqttsn.PINGRESP}
	if err := peer.Send(server, pingResp); err != nil {
		h.Metrics.ErrorCount.Add(1)
		return
	}
	h.Metrics.PacketsSent.Add(1)
	h.Metrics.BytesSent.Add(int64(len(pingResp)))

	// 心跳捎带：若该设备有待执行指令，PINGRESP 后立即补发（弱在线桩唤醒通道）
	if len(data) > 0 {
		h.dispatchPendingCommands(server, string(data))
	}
}

// 处理DISCONNECT消息
func (h *MessageHandler) handleDisconnect(server *Server, data []byte, peer *Peer) {
	var deviceID string

	if len(data) > 0 {
		// 如果有ClientID
		deviceID = string(data)
	} else {
		// 从地址查找设备
		h.DeviceManager.mu.RLock()
		for id, device := range h.DeviceManager.devices {
			if device.Address == peer.String() {
				deviceID = id
				break
			}
		}
		h.DeviceManager.mu.RUnlock()
	}

	if deviceID != "" {
		h.dropDevice(server, deviceID)
		h.Logger.Debug("Device disconnected", "device_id", deviceID)
	}
}

// 处理SUBSCRIBE消息
func (h *MessageHandler) handleSubscribe(server *Server, data []byte, peer *Peer) {
	// 解析订阅消息（简化处理）
	if len(data) < 5 {
		return
	}

	msgID := binary.BigEndian.Uint16(data[0:2])
	topicID := binary.BigEndian.Uint16(data[2:4])
	qos := data[4] & 0x03
	_ = msgID

	// 发送SUBACK
	suback := []byte{
		0x08, // Length
		mqttsn.SUBACK,
		data[0], data[1], // MsgID
		0x00,          // TopicID MSB
		byte(topicID), // TopicID LSB
		0x00,          // Return Code
		qos,           // QoS
	}

	if err := peer.Send(server, suback); err != nil {
		h.Metrics.ErrorCount.Add(1)
		return
	}
	h.Metrics.SubscribeCount.Add(1)
}

// 处理UNSUBSCRIBE消息
func (h *MessageHandler) handleUnsubscribe(server *Server, data []byte, peer *Peer) {
	if len(data) < 4 {
		return
	}

	msgID := binary.BigEndian.Uint16(data[0:2])
	_ = msgID

	// 发送UNSUBACK
	unsuback := []byte{
		0x04, // Length
		mqttsn.UNSUBACK,
		data[0], data[1], // MsgID
	}

	if err := peer.Send(server, unsuback); err != nil {
		h.Metrics.ErrorCount.Add(1)
	}
}

// 处理REGISTER消息
func (h *MessageHandler) handleRegister(server *Server, data []byte, peer *Peer) {
	if len(data) < 6 {
		return
	}

	topicID := binary.BigEndian.Uint16(data[0:2])
	msgID := binary.BigEndian.Uint16(data[2:4])
	topicName := string(data[4:])
	_ = msgID

	// 发送REGACK
	regack := []byte{
		0x07, // Length
		mqttsn.REGACK,
		data[0], data[1], // TopicID MSB
		data[2], data[3], // TopicID LSB
		data[0], data[1], // MsgID MSB
		0x00, // Return Code
	}

	if err := peer.Send(server, regack); err != nil {
		h.Metrics.ErrorCount.Add(1)
		return
	}

	h.Logger.Debug("Topic registered",
		"topic_id", topicID,
		"topic_name", topicName,
	)
}

// 发送CONNACK
func (h *MessageHandler) sendConnack(server *Server, peer *Peer, returnCode uint8) {
	connack := []byte{
		0x03, // Length
		mqttsn.CONNACK,
		returnCode,
	}

	if err := peer.Send(server, connack); err != nil {
		h.Metrics.ErrorCount.Add(1)
		h.Logger.Warn("sendConnack failed", "to", peer.String(), "error", err)
		return
	}
	h.Metrics.PacketsSent.Add(1)
	h.Metrics.BytesSent.Add(int64(len(connack)))
}

// 发送PUBACK
func (h *MessageHandler) sendPuback(server *Server, peer *Peer, topicID, msgID uint16, returnCode uint8) {
	puback := []byte{
		0x07, // Length
		mqttsn.PUBACK,
		byte(topicID >> 8),
		byte(topicID),
		byte(msgID >> 8),
		byte(msgID),
		returnCode,
	}

	if err := peer.Send(server, puback); err != nil {
		h.Metrics.ErrorCount.Add(1)
		return
	}
	h.Metrics.PacketsSent.Add(1)
	h.Metrics.BytesSent.Add(int64(len(puback)))
}

// 获取指标
func (h *MessageHandler) GetMetrics() *Metrics {
	return h.Metrics
}

// 获取在线设备数
func (h *MessageHandler) GetOnlineDeviceCount() int {
	h.DeviceManager.mu.RLock()
	defer h.DeviceManager.mu.RUnlock()
	return len(h.DeviceManager.devices)
}

// 获取会话数
func (h *MessageHandler) GetSessionCount() int {
	h.SessionManager.mu.RLock()
	defer h.SessionManager.mu.RUnlock()
	return len(h.SessionManager.sessions)
}

// CleanupExpiredSessions 清理过期会话（失联判定），返回过期设备ID列表（供影子离线标记）
func (h *MessageHandler) CleanupExpiredSessions(timeout time.Duration) []string {
	now := time.Now()

	var expired []*Session

	h.SessionManager.mu.RLock()
	for _, session := range h.SessionManager.sessions {
		if now.Sub(session.LastActive) > timeout {
			expired = append(expired, session)
		}
	}
	h.SessionManager.mu.RUnlock()

	if len(expired) == 0 {
		return nil
	}

	expiredIDs := make([]string, 0, len(expired))

	h.SessionManager.mu.Lock()
	for _, session := range expired {
		id := session.ID
		if cur, ok := h.SessionManager.sessions[id]; !ok || cur != session {
			continue
		}
		delete(h.SessionManager.sessions, id)
		if cur, ok := h.SessionManager.byDevice[session.DeviceID]; ok && cur == id {
			delete(h.SessionManager.byDevice, session.DeviceID)
		}
		expiredIDs = append(expiredIDs, session.DeviceID)

		// 更新设备状态（离线）
		h.DeviceManager.mu.Lock()
		if device, ok := h.DeviceManager.devices[session.DeviceID]; ok {
			device.Status = "offline"
		}
		h.DeviceManager.mu.Unlock()
	}
	h.SessionManager.mu.Unlock()
	return expiredIDs
}

// GetAllDeviceIDs 返回本节点全部设备ID（网关停机时用于影子离线标记）
func (h *MessageHandler) GetAllDeviceIDs() []string {
	h.DeviceManager.mu.RLock()
	defer h.DeviceManager.mu.RUnlock()
	ids := make([]string, 0, len(h.DeviceManager.devices))
	for id := range h.DeviceManager.devices {
		ids = append(ids, id)
	}
	return ids
}

// dropDevice 设备下线清理：设备表/会话/Redis/影子（连接断开与DISCONNECT共用）
func (h *MessageHandler) dropDevice(server *Server, deviceID string) {
	h.DeviceManager.mu.Lock()
	delete(h.DeviceManager.devices, deviceID)
	h.DeviceManager.mu.Unlock()

	h.SessionManager.mu.Lock()
	if sessionID, ok := h.SessionManager.byDevice[deviceID]; ok {
		delete(h.SessionManager.sessions, sessionID)
		delete(h.SessionManager.byDevice, deviceID)
	}
	h.SessionManager.mu.Unlock()

	redis.RemoveIoTDevice(deviceID)
	server.Shadow.MarkOffline(deviceID)
	h.Metrics.DisconnectCount.Add(1)
	h.Logger.Debug("Device dropped", "device_id", deviceID)
}

// dropDeviceByConn 按流式连接（DTLS net.Conn / QUIC 连接）下线其设备
func (h *MessageHandler) dropDeviceByConn(server *Server, conn interface{}) {
	h.DeviceManager.mu.RLock()
	var target string
	for id, dev := range h.DeviceManager.devices {
		if dev.Peer == nil {
			continue
		}
		if dev.Peer.Conn != nil && dev.Peer.Conn == conn {
			target = id
			break
		}
		if dev.Peer.Quic != nil && dev.Peer.Quic == conn {
			target = id
			break
		}
	}
	h.DeviceManager.mu.RUnlock()
	if target != "" {
		h.dropDevice(server, target)
	}
}

// dropDeviceByStream 按 QUIC 流下线其设备
func (h *MessageHandler) dropDeviceByStream(server *Server, stream interface{}) {
	h.DeviceManager.mu.RLock()
	var target string
	for id, dev := range h.DeviceManager.devices {
		if dev.Peer != nil && dev.Peer.Stream != nil && dev.Peer.Stream == stream {
			target = id
			break
		}
	}
	h.DeviceManager.mu.RUnlock()
	if target != "" {
		h.dropDevice(server, target)
	}
}
