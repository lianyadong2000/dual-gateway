package cend

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"dual-gateway/internal/auth"
	"dual-gateway/internal/command"
	"dual-gateway/internal/config"
	"dual-gateway/internal/mq"
	"dual-gateway/internal/protocol/cend/protobuf"
	"dual-gateway/internal/ratelimit"
	"dual-gateway/internal/redis"
	"dual-gateway/internal/shadow"
	"dual-gateway/pkg/logger"
	"dual-gateway/pkg/utils"
)

// 处理器指标
type HandlerMetrics struct {
	AuthRequests     atomic.Int64
	AuthSuccess      atomic.Int64
	AuthFailures     atomic.Int64
	MessagesReceived atomic.Int64
	MessagesSent     atomic.Int64
	MessagesFailed   atomic.Int64
	Heartbeats       atomic.Int64
	ProcessingTime   atomic.Int64
	ActiveWorkers    atomic.Int64
	ShadowQueries    atomic.Int64
	ChargeCommands   atomic.Int64
	ChargeRejected   atomic.Int64
	Subscriptions    atomic.Int64
	PushesSent       atomic.Int64
}

// 消息处理器
type MessageHandler struct {
	Server      *Server
	Logger      logger.Logger
	JWTManager  *auth.JWTManager
	AuthConfig  *config.AuthConfig
	WorkerPool  chan struct{}
	MessagePool *utils.MessagePool
	Metrics     *HandlerMetrics

	// 设备影子 / 指令队列 / accept限流
	Shadow         *shadow.Manager
	CmdQueue       *command.Queue
	AcceptLimiter  *ratelimit.AcceptLimiter

	// 桩状态订阅表：deviceID -> set(userID)
	subMu        sync.RWMutex
	subscribers  map[string]map[string]bool
}

// 创建消息处理器
func NewMessageHandler(server *Server, log logger.Logger, authConfig *config.AuthConfig, shadowCfg *config.ShadowConfig, rateCfg *config.RateLimitConfig) *MessageHandler {
	jwtManager, err := auth.NewJWTManager(
		[]byte(authConfig.JWTPrivateKey),
		[]byte(authConfig.JWTPublicKey),
		authConfig.JWTIssuer,
		authConfig.JWTTTL(),
	)
	if err != nil {
		log.Error("Failed to create JWT manager", "error", err)
		jwtManager = &auth.JWTManager{}
	}

	return &MessageHandler{
		Server:      server,
		Logger:      log,
		JWTManager:  jwtManager,
		AuthConfig:  authConfig,
		WorkerPool:  make(chan struct{}, 10000),
		MessagePool: utils.NewMessagePool(),
		Metrics:     &HandlerMetrics{},
		Shadow:      shadow.NewManager(shadowCfg),
		CmdQueue:    command.NewQueue(shadowCfg),
		AcceptLimiter: ratelimit.NewAcceptLimiter(rateCfg.CendAcceptRate, rateCfg.Enabled),
		subscribers: make(map[string]map[string]bool),
	}
}

// 处理WebSocket消息
func (h *MessageHandler) HandleMessage(conn *Connection, data []byte) {
	select {
	case h.WorkerPool <- struct{}{}:
		defer func() { <-h.WorkerPool }()
		h.Metrics.ActiveWorkers.Add(1)
		defer h.Metrics.ActiveWorkers.Add(-1)
	default:
		h.Logger.Warn("Worker pool full, dropping message", "conn_id", conn.ID)
		h.Metrics.MessagesFailed.Add(1)
		return
	}

	startTime := time.Now()
	defer func() {
		elapsed := time.Since(startTime).Microseconds()
		h.Metrics.ProcessingTime.Add(elapsed)
	}()

	// 由于头部和消息体是分开序列化的，我们需要分别解析
	// 首先尝试解析头部
	header := &protobuf.Header{}
	headerBytes, bodyBytes, err := splitHeaderAndBody(data)
	if err != nil {
		h.Logger.Error("Failed to split message", "error", err, "conn_id", conn.ID)
		return
	}

	if err := proto.Unmarshal(headerBytes, header); err != nil {
		h.Logger.Error("Failed to unmarshal header", "error", err, "conn_id", conn.ID)
		return
	}

	// 验证魔数
	if header.Magic != 0xABCD {
		h.Logger.Warn("Invalid magic number", "magic", header.Magic, "conn_id", conn.ID)
		return
	}

	// 验证版本
	if header.Version != 1 {
		h.Logger.Warn("Unsupported version", "version", header.Version, "conn_id", conn.ID)
		h.sendError(conn, header.Seq, 400, "Unsupported protocol version")
		return
	}

	// 根据消息类型分发
	switch header.Type {
	case protobuf.MessageType_AUTH:
		h.handleAuth(conn, header, bodyBytes)

	case protobuf.MessageType_HEARTBEAT:
		h.handleHeartbeat(conn, header, bodyBytes)

	case protobuf.MessageType_MESSAGE:
		h.handleMessage(conn, header, bodyBytes)

	default:
		h.Logger.Warn("Unknown message type", "type", header.Type, "conn_id", conn.ID)
		h.sendError(conn, header.Seq, 400, "Unknown message type")
	}
}

// 分离头部和消息体
// 协议格式：[2B headerLen][headerBytes][bodyBytes]
// 兼容回退：旧格式（无长度前缀）使用有界扫描
func splitHeaderAndBody(data []byte) ([]byte, []byte, error) {
	// 优先尝试长度前缀协议
	if len(data) >= 4 {
		headerLen := int(data[0])<<8 | int(data[1])
		if headerLen >= 2 && 2+headerLen <= len(data) {
			header := &protobuf.Header{}
			if err := proto.Unmarshal(data[2:2+headerLen], header); err == nil {
				if header.Magic == 0xABCD && header.Version == 1 {
					return data[2 : 2+headerLen], data[2+headerLen:], nil
				}
			}
		}
	}

	// 兼容回退：有界扫描（旧协议）
	header := &protobuf.Header{}
	maxScan := len(data)
	if maxScan > 128 {
		maxScan = 128
	}

	for i := 1; i <= maxScan; i++ {
		header.Reset()
		if err := proto.Unmarshal(data[:i], header); err == nil {
			// 检查是否是有效的头部（需要完整解析到Magic、Version、Type字段）
			if header.Magic == 0xABCD && header.Version == 1 && header.Type != 0 {
				return data[:i], data[i:], nil
			}
		}
	}

	// 如果找不到头部，尝试将整个数据作为头部
	if err := proto.Unmarshal(data, header); err == nil {
		if header.Magic == 0xABCD {
			return data, nil, nil
		}
	}

	return nil, nil, fmt.Errorf("failed to split header and body")
}

// 处理鉴权
func (h *MessageHandler) handleAuth(conn *Connection, header *protobuf.Header, body []byte) {
	h.Metrics.AuthRequests.Add(1)

	if conn.Status.Load() == int32(StatusAuthed) {
		h.Logger.Warn("Connection already authenticated", "conn_id", conn.ID)
		h.sendAuthResponse(conn, header.Seq, 400, "Already authenticated", "", 0)
		return
	}

	authReq := &protobuf.AuthRequest{}
	if len(body) > 0 {
		if err := proto.Unmarshal(body, authReq); err != nil {
			h.Logger.Error("Failed to unmarshal auth request", "error", err)
			h.Metrics.AuthFailures.Add(1)
			h.sendAuthResponse(conn, header.Seq, 400, "Invalid auth request", "", 0)
			return
		}
	}

	// 验证Token
	claims, err := h.JWTManager.ValidateToken(authReq.Token)
	if err != nil {
		h.Logger.Warn("Invalid token", "error", err, "conn_id", conn.ID)
		h.Metrics.AuthFailures.Add(1)
		h.sendAuthResponse(conn, header.Seq, 401, "Invalid token", "", 0)
		return
	}

	// 更新连接状态
	conn.UserID = claims.UserID
	conn.DeviceID = claims.DeviceID
	conn.Status.Store(int32(StatusAuthed))

	// 生成会话ID
	sessionID := utils.GenerateSessionID()

	// 注册用户在线状态
	if err := redis.AddOnlineUser(claims.UserID, conn.ID, h.Server.ID); err != nil {
		h.Logger.Error("Failed to register online user", "error", err, "user_id", claims.UserID)
		h.Metrics.AuthFailures.Add(1)
		h.sendAuthResponse(conn, header.Seq, 500, "Internal server error", "", 0)
		return
	}

	h.Metrics.AuthSuccess.Add(1)

	// 发送鉴权成功响应
	expiresAt := uint64(0)
	if claims.ExpiresAt != nil {
		expiresAt = uint64(claims.ExpiresAt.Unix())
	}
	h.sendAuthResponse(conn, header.Seq, 200, "Authentication successful", sessionID, expiresAt)

	h.Logger.Info("User authenticated",
		"user_id", claims.UserID,
		"device_id", claims.DeviceID,
		"conn_id", conn.ID,
		"session_id", sessionID,
	)
}

// 处理心跳
func (h *MessageHandler) handleHeartbeat(conn *Connection, header *protobuf.Header, body []byte) {
	if conn.Status.Load() != int32(StatusAuthed) {
		return
	}

	h.Metrics.Heartbeats.Add(1)
	conn.LastActive.Store(time.Now().Unix())

	heartbeatAck := &protobuf.HeartbeatAck{
		ServerTime: uint64(time.Now().Unix()),
	}

	if err := conn.SendMessage(protobuf.MessageType_HEARTBEAT_ACK, header.Seq, heartbeatAck); err != nil {
		h.Logger.Debug("Failed to send heartbeat ack", "error", err)
	}
}

// 处理业务消息
func (h *MessageHandler) handleMessage(conn *Connection, header *protobuf.Header, body []byte) {
	if conn.Status.Load() != int32(StatusAuthed) {
		h.sendError(conn, header.Seq, 401, "Not authenticated")
		return
	}

	h.Metrics.MessagesReceived.Add(1)

	message := &protobuf.Message{}
	if len(body) > 0 {
		if err := proto.Unmarshal(body, message); err != nil {
			h.Logger.Error("Failed to unmarshal message", "error", err)
			h.Metrics.MessagesFailed.Add(1)
			h.sendMessageAck(conn, header.Seq, "", 400, "Invalid message format")
			return
		}
	}

	message.From = conn.UserID
	message.Timestamp = uint64(time.Now().UnixMilli())
	if message.RequestId == "" {
		message.RequestId = utils.GenerateRequestID()
	}

	// 影子/指令业务路由（充电桩场景）
	switch message.Type {
	case "shadow.query":
		h.handleShadowQuery(conn, header, message)

	case "charge.start", "charge.stop", "set.param", "query":
		h.handleDeviceCommand(conn, header, message)

	case "shadow.subscribe", "shadow.unsubscribe":
		h.handleShadowSubscribe(conn, header, message)

	default:
		// 转发到业务系统
		if err := h.forwardToBusinessSystem(message); err != nil {
			h.Logger.Error("Failed to forward message", "error", err)
			h.Metrics.MessagesFailed.Add(1)
			h.sendMessageAck(conn, header.Seq, message.RequestId, 500, "Failed to process message")
			return
		}

		h.Metrics.MessagesSent.Add(1)
		h.sendMessageAck(conn, header.Seq, message.RequestId, 200, "Message sent successfully")
	}

	h.Logger.Debug("Message processed",
		"request_id", message.RequestId,
		"from", message.From,
		"to", message.To,
		"type", message.Type,
	)
}

// handleShadowQuery 查询桩影子状态（C端一律走影子，绝不直连弱网桩）
func (h *MessageHandler) handleShadowQuery(conn *Connection, header *protobuf.Header, message *protobuf.Message) {
	h.Metrics.ShadowQueries.Add(1)
	deviceID := message.To
	if deviceID == "" {
		h.sendMessageAck(conn, header.Seq, message.RequestId, 400, "missing device id")
		return
	}
	s, err := h.Shadow.Get(deviceID)
	if err != nil {
		h.sendMessageAck(conn, header.Seq, message.RequestId, 500, "shadow unavailable")
		return
	}
	data, _ := json.Marshal(s)
	h.sendMessageAck(conn, header.Seq, message.RequestId, 200, string(data))
}

// handleDeviceCommand 充电指令：原子抢占（charge.start）→ 入队 → 触发在线直发
func (h *MessageHandler) handleDeviceCommand(conn *Connection, header *protobuf.Header, message *protobuf.Message) {
	deviceID := message.To
	if deviceID == "" {
		h.sendMessageAck(conn, header.Seq, message.RequestId, 400, "missing device id")
		return
	}

	// 解析参数
	params := map[string]interface{}{}
	if message.Content != "" {
		if err := json.Unmarshal([]byte(message.Content), &params); err != nil {
			params = map[string]interface{}{"content": message.Content}
		}
	}
	params["user_id"] = conn.UserID

	// charge.start 必须先原子抢占（防双人抢桩）
	if message.Type == "charge.start" {
		h.Metrics.ChargeCommands.Add(1)
		s, err := h.Shadow.TryPreoccupy(deviceID)
		if err != nil {
			h.Metrics.ChargeRejected.Add(1)
			h.sendMessageAck(conn, header.Seq, message.RequestId, 409, "device unavailable: "+err.Error())
			return
		}
		params["shadow_version"] = s.Version
	}

	// 入队（幂等：指令ID全局唯一）→ 队列是唯一真相，桩唤醒即拉取
	cmd, err := h.CmdQueue.Enqueue(deviceID, message.Type, params)
	if err != nil {
		h.sendMessageAck(conn, header.Seq, message.RequestId, 500, "enqueue failed: "+err.Error())
		return
	}
	if cmd == nil {
		// 影子/队列未启用：降级为仅ack（单机无影子模式）
		h.sendMessageAck(conn, header.Seq, message.RequestId, 200, `{"accepted":true,"queued":false}`)
		return
	}

	// 发布指令触发（IoT网关订阅后在线直发；桩离线则指令留在队列等唤醒）
	trig := shadow.CmdTrigger{DeviceID: deviceID, CmdID: cmd.CmdID, CmdType: cmd.Type}
	if data, err := json.Marshal(trig); err == nil {
		redis.PublishMessage(shadow.CmdChannel, data)
	}

	ack, _ := json.Marshal(map[string]interface{}{
		"accepted": true,
		"queued":   true,
		"cmd_id":   cmd.CmdID,
		"type":     cmd.Type,
	})
	h.sendMessageAck(conn, header.Seq, message.RequestId, 200, string(ack))
}

// handleShadowSubscribe 订阅桩状态（状态变更实时推送）
func (h *MessageHandler) handleShadowSubscribe(conn *Connection, header *protobuf.Header, message *protobuf.Message) {
	deviceID := message.To
	if deviceID == "" {
		h.sendMessageAck(conn, header.Seq, message.RequestId, 400, "missing device id")
		return
	}
	h.subMu.Lock()
	if message.Type == "shadow.subscribe" {
		set, ok := h.subscribers[deviceID]
		if !ok {
			set = make(map[string]bool)
			h.subscribers[deviceID] = set
		}
		if !set[conn.UserID] {
			set[conn.UserID] = true
			h.Metrics.Subscriptions.Add(1)
		}
	} else {
		if set, ok := h.subscribers[deviceID]; ok {
			delete(set, conn.UserID)
			if len(set) == 0 {
				delete(h.subscribers, deviceID)
			}
		}
	}
	h.subMu.Unlock()
	h.sendMessageAck(conn, header.Seq, message.RequestId, 200, `{"subscribed":true}`)
}

// PushShadowEvent 将桩状态变更/指令结果推送给订阅该桩的C端用户
func (h *MessageHandler) PushShadowEvent(deviceID string, eventJSON []byte) {
	h.subMu.RLock()
	users := make([]string, 0, len(h.subscribers[deviceID]))
	for uid := range h.subscribers[deviceID] {
		users = append(users, uid)
	}
	h.subMu.RUnlock()
	for _, uid := range users {
		if err := h.PushToUser(uid, eventJSON); err != nil {
			h.Logger.Debug("Shadow push failed", "user_id", uid, "error", err)
			continue
		}
		h.Metrics.PushesSent.Add(1)
	}
}

// 转发到业务系统
func (h *MessageHandler) forwardToBusinessSystem(message *protobuf.Message) error {
	msgData := map[string]interface{}{
		"request_id": message.RequestId,
		"from":       message.From,
		"to":         message.To,
		"content":    message.Content,
		"type":       message.Type,
		"timestamp":  message.Timestamp,
		"metadata":   message.Metadata,
	}
	return mq.PublishCendMessage(msgData)
}

// 推送给指定用户
func (h *MessageHandler) PushToUser(userID string, message []byte) error {
	connInfo, err := redis.GetUserConnection(userID)
	if err != nil {
		return fmt.Errorf("user %s not online: %w", userID, err)
	}

	if connInfo.GatewayID != h.Server.ID {
		return h.forwardToOtherGateway(connInfo.GatewayID, userID, message)
	}

	conn, ok := h.Server.Connections.Load(connInfo.ConnectionID)
	if !ok {
		return fmt.Errorf("connection %s not found", connInfo.ConnectionID)
	}

	pushMessage := &protobuf.PushMessage{
		MessageId: utils.GenerateMessageID(),
		Content:   string(message),
		Timestamp: uint64(time.Now().UnixMilli()),
	}

	return conn.(*Connection).SendMessage(protobuf.MessageType_PUSH, 0, pushMessage)
}

// 转发到其他网关
func (h *MessageHandler) forwardToOtherGateway(gatewayID, userID string, message []byte) error {
	forwardMsg := map[string]interface{}{
		"gateway_id": gatewayID,
		"user_id":    userID,
		"message":    string(message),
		"timestamp":  time.Now().Unix(),
	}
	data, _ := json.Marshal(forwardMsg)
	return redis.PublishMessage("gateway:forward:channel", data)
}

// 发送鉴权响应
func (h *MessageHandler) sendAuthResponse(conn *Connection, seq uint64, code int32, message string, sessionID string, expiresAt uint64) {
	resp := &protobuf.AuthResponse{
		Code:      code,
		Message:   message,
		SessionId: sessionID,
		ExpiresAt: expiresAt,
	}
	if err := conn.SendMessage(protobuf.MessageType_AUTH_ACK, seq, resp); err != nil {
		h.Logger.Debug("Failed to send auth response", "error", err)
	}
}

// 发送消息确认
func (h *MessageHandler) sendMessageAck(conn *Connection, seq uint64, requestID string, code int32, message string) {
	ack := &protobuf.MessageAck{
		RequestId: requestID,
		Code:      code,
		Message:   message,
		Timestamp: uint64(time.Now().UnixMilli()),
	}
	if err := conn.SendMessage(protobuf.MessageType_MESSAGE_ACK, seq, ack); err != nil {
		h.Logger.Debug("Failed to send message ack", "error", err)
	}
}

// 发送错误响应
func (h *MessageHandler) sendError(conn *Connection, seq uint64, code int32, message string) {
	errResp := &protobuf.ErrorResponse{
		Code:    code,
		Message: message,
	}
	if err := conn.SendMessage(protobuf.MessageType_ERROR, seq, errResp); err != nil {
		h.Logger.Debug("Failed to send error response", "error", err)
	}
}

// 获取处理器指标
func (h *MessageHandler) GetMetrics() *HandlerMetrics {
	return h.Metrics
}
