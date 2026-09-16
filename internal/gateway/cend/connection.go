package cend

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	"dual-gateway/internal/protocol/cend/protobuf"
	"dual-gateway/internal/redis"
	"dual-gateway/pkg/logger"
	"dual-gateway/pkg/utils"
)

// 连接状态
type ConnectionStatus int32

const (
	StatusConnecting ConnectionStatus = 0
	StatusAuthed     ConnectionStatus = 1
	StatusClosed     ConnectionStatus = 2
)

// C端连接
type Connection struct {
	ID         string
	UserID     string
	DeviceID   string
	Conn       *websocket.Conn
	Status     atomic.Int32
	LastActive atomic.Int64
	SendChan   chan []byte
	CloseChan  chan struct{}
	Ctx        context.Context
	Cancel     context.CancelFunc
	GatewayID  string
	Logger     logger.Logger
	mu         sync.RWMutex
	closeOnce  sync.Once
}

// 创建连接
func NewConnection(conn *websocket.Conn, gatewayID string, log logger.Logger) *Connection {
	ctx, cancel := context.WithCancel(context.Background())

	c := &Connection{
		ID:        generateConnectionID(),
		Conn:      conn,
		SendChan:  make(chan []byte, 256),
		CloseChan: make(chan struct{}),
		Ctx:       ctx,
		Cancel:    cancel,
		GatewayID: gatewayID,
		Logger:    log,
	}

	c.Status.Store(int32(StatusConnecting))
	c.LastActive.Store(time.Now().Unix())

	return c
}

// 写循环
func (c *Connection) WriteLoop() {
	defer c.Close()

	ticker := time.NewTicker(30 * time.Second) // Ping间隔
	defer ticker.Stop()

	for {
		select {
		case data := <-c.SendChan:
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.Conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
				c.Logger.Debug("Write error", "id", c.ID, "error", err)
				return
			}

		case <-ticker.C:
			// 发送Ping
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				c.Logger.Debug("Ping error", "id", c.ID, "error", err)
				return
			}

		case <-c.CloseChan:
			return

		case <-c.Ctx.Done():
			return
		}
	}
}

// 读循环
func (c *Connection) ReadLoop(handler *MessageHandler) {
	defer c.Close()

	// 设置Pong处理器
	c.Conn.SetPongHandler(func(string) error {
		c.LastActive.Store(time.Now().Unix())
		return nil
	})

	for {
		c.Conn.SetReadDeadline(time.Now().Add(90 * time.Second))

		msgType, data, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err,
				websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				c.Logger.Debug("Connection closed", "id", c.ID, "error", err)
			}
			return
		}

		c.LastActive.Store(time.Now().Unix())

		if msgType != websocket.BinaryMessage {
			continue
		}

		// 处理消息（同步处理，避免每个消息创建goroutine）
		handler.HandleMessage(c, data)
	}
}

// 发送Protobuf消息
func (c *Connection) SendMessage(msgType protobuf.MessageType, seq uint64, msg proto.Message) error {
	// 构造消息头
	header := &protobuf.Header{
		Magic:     0xABCD,
		Version:   1,
		Type:      msgType,
		Seq:       seq,
		Timestamp: uint64(time.Now().UnixMilli()),
		TraceId:   generateTraceID(),
	}

	// 序列化消息体
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	// 序列化头部
	headerBytes, err := proto.Marshal(header)
	if err != nil {
		return err
	}

	// 组合完整消息：[2B headerLen][headerBytes][bodyBytes]
	// 长度前缀保证接收端可精确定位头部边界，避免protobuf无长度前缀导致的误判
	data := make([]byte, 0, 2+len(headerBytes)+len(body))
	data = append(data, byte(len(headerBytes)>>8), byte(len(headerBytes)))
	data = append(data, headerBytes...)
	data = append(data, body...)

	select {
	case c.SendChan <- data:
		return nil
	case <-c.CloseChan:
		return errors.New("connection closed")
	case <-time.After(5 * time.Second):
		return errors.New("send timeout")
	}
}

// 关闭连接（幂等，可安全并发调用）
func (c *Connection) Close() {
	c.closeOnce.Do(func() {
		if c.Status.Load() == int32(StatusClosed) {
			return
		}

		c.Status.Store(int32(StatusClosed))
		close(c.CloseChan)
		c.Cancel()
		c.Conn.Close()

		// 清除在线状态
		if c.UserID != "" {
			redis.RemoveOnlineUser(c.UserID)
		}
	})
}

// 生成连接ID
func generateConnectionID() string {
	return utils.GenerateTraceID()
}

// 生成追踪ID
func generateTraceID() string {
	return utils.GenerateTraceID()
}
