package cend

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"dual-gateway/internal/protocol/cend/protobuf"
	"dual-gateway/pkg/utils"
)

// 协议常量
const (
	ProtocolMagic   = 0xABCD
	ProtocolVersion = 1
	HeaderSize      = 12 // 固定头部大小
	MaxMessageSize  = 1024 * 1024 // 1MB
)

// 编解码器接口
type Codec interface {
	Encode(msg *Message) ([]byte, error)
	Decode(data []byte) (*Message, error)
	EncodeToWriter(w io.Writer, msg *Message) error
	DecodeFromReader(r io.Reader) (*Message, error)
}

// 消息结构
type Message struct {
	Header *protobuf.Header
	Body   proto.Message
}

// 默认编解码器
type DefaultCodec struct {
	bufferPool *utils.ByteSlicePool
}

// 创建默认编解码器
func NewDefaultCodec() *DefaultCodec {
	return &DefaultCodec{
		bufferPool: utils.NewByteSlicePool(MaxMessageSize),
	}
}

// 编码消息
func (c *DefaultCodec) Encode(msg *Message) ([]byte, error) {
	if msg == nil || msg.Header == nil {
		return nil, errors.New("invalid message")
	}

	// 序列化头部
	headerBytes, err := proto.Marshal(msg.Header)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal header: %w", err)
	}

	// 序列化消息体
	var bodyBytes []byte
	if msg.Body != nil {
		bodyBytes, err = proto.Marshal(msg.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal body: %w", err)
		}
	}

	// 计算总长度
	totalLength := HeaderSize + len(headerBytes) + len(bodyBytes)
	if totalLength > MaxMessageSize {
		return nil, fmt.Errorf("message too large: %d bytes", totalLength)
	}

	// 分配缓冲区
	buffer := make([]byte, totalLength)

	// 写入固定头部
	binary.BigEndian.PutUint16(buffer[0:2], ProtocolMagic)
	buffer[2] = ProtocolVersion
	buffer[3] = uint8(msg.Header.Type)
	binary.BigEndian.PutUint32(buffer[4:8], uint32(msg.Header.Seq))
	binary.BigEndian.PutUint32(buffer[8:12], uint32(len(headerBytes)+len(bodyBytes)))

	// 写入protobuf头部长度（2字节），随后是header+body
	copy(buffer[HeaderSize:], make([]byte, 2))
	binary.BigEndian.PutUint16(buffer[HeaderSize:], uint16(len(headerBytes)))
	copy(buffer[HeaderSize+2:], headerBytes)
	copy(buffer[HeaderSize+2+len(headerBytes):], bodyBytes)

	return buffer, nil
}

// 分离Protobuf头部与消息体
// 线上协议格式为 [protobuf Header][protobuf Body]，通过尝试性解析定位Header边界
func splitHeaderAndBodyBytes(data []byte) ([]byte, []byte, error) {
	header := &protobuf.Header{}

	// 限制扫描次数，避免O(n^2)退化
	maxScan := len(data)
	if maxScan > 128 {
		maxScan = 128
	}

	for i := 1; i <= maxScan && i <= len(data); i++ {
		header.Reset()
		if err := proto.Unmarshal(data[:i], header); err == nil {
			if header.Magic == uint32(ProtocolMagic) {
				return data[:i], data[i:], nil
			}
		}
	}

	// 兜底：尝试将整个数据作为头部
	header.Reset()
	if err := proto.Unmarshal(data, header); err == nil && header.Magic == uint32(ProtocolMagic) {
		return data, nil, nil
	}

	return nil, nil, fmt.Errorf("failed to split header and body")
}

// 解码消息
func (c *DefaultCodec) Decode(data []byte) (*Message, error) {
	if len(data) < HeaderSize {
		return nil, errors.New("data too short")
	}

	// 验证魔数
	magic := binary.BigEndian.Uint16(data[0:2])
	if magic != ProtocolMagic {
		return nil, fmt.Errorf("invalid magic number: 0x%X", magic)
	}

	// 验证版本
	version := data[2]
	if version != ProtocolVersion {
		return nil, fmt.Errorf("unsupported version: %d", version)
	}

	// 解析固定头部
	msgType := protobuf.MessageType(data[3])
	bodyLength := binary.BigEndian.Uint32(data[8:12])

	if bodyLength > MaxMessageSize {
		return nil, fmt.Errorf("message too large: %d bytes", bodyLength)
	}

	if int(bodyLength) > len(data)-HeaderSize {
		return nil, errors.New("incomplete message data")
	}

	// 分离protobuf头部与消息体（优先长度前缀，回退扫描）
	payload := data[HeaderSize : HeaderSize+int(bodyLength)]
	var headerBytes, bodyBytes []byte
	if len(payload) >= 2 {
		hl := int(payload[0])<<8 | int(payload[1])
		if hl >= 2 && 2+hl <= len(payload) {
			hdr := &protobuf.Header{}
			if err := proto.Unmarshal(payload[2:2+hl], hdr); err == nil && hdr.Magic == uint32(ProtocolMagic) {
				headerBytes = payload[2 : 2+hl]
				bodyBytes = payload[2+hl:]
			}
		}
	}
	if headerBytes == nil {
		var err error
		headerBytes, bodyBytes, err = splitHeaderAndBodyBytes(payload)
		if err != nil {
			return nil, err
		}
	}

	header := &protobuf.Header{}
	if err := proto.Unmarshal(headerBytes, header); err != nil {
		return nil, fmt.Errorf("failed to unmarshal header: %w", err)
	}

	// 创建消息
	msg := &Message{
		Header: header,
	}

	// 根据消息类型解析消息体
	if len(bodyBytes) > 0 {
		msg.Body = c.unmarshalBody(msgType, bodyBytes)
	}

	return msg, nil
}

// 编码到Writer
func (c *DefaultCodec) EncodeToWriter(w io.Writer, msg *Message) error {
	data, err := c.Encode(msg)
	if err != nil {
		return err
	}

	_, err = w.Write(data)
	return err
}

// 从Reader解码
func (c *DefaultCodec) DecodeFromReader(r io.Reader) (*Message, error) {
	// 读取固定头部
	headerBuf := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, headerBuf); err != nil {
		return nil, err
	}

	// 验证魔数
	magic := binary.BigEndian.Uint16(headerBuf[0:2])
	if magic != ProtocolMagic {
		return nil, fmt.Errorf("invalid magic number: 0x%X", magic)
	}

	// 读取消息体长度
	bodyLength := binary.BigEndian.Uint32(headerBuf[8:12])
	if bodyLength > MaxMessageSize {
		return nil, fmt.Errorf("message too large: %d bytes", bodyLength)
	}

	// 读取消息体
	bodyBuf := make([]byte, bodyLength)
	if _, err := io.ReadFull(r, bodyBuf); err != nil {
		return nil, err
	}

	// 组合完整消息
	fullData := append(headerBuf, bodyBuf...)
	return c.Decode(fullData)
}

// 解析消息体
func (c *DefaultCodec) unmarshalBody(msgType protobuf.MessageType, data []byte) proto.Message {
	var body proto.Message

	switch msgType {
	case protobuf.MessageType_AUTH:
		body = &protobuf.AuthRequest{}
	case protobuf.MessageType_AUTH_ACK:
		body = &protobuf.AuthResponse{}
	case protobuf.MessageType_HEARTBEAT:
		body = &protobuf.Heartbeat{}
	case protobuf.MessageType_HEARTBEAT_ACK:
		body = &protobuf.HeartbeatAck{}
	case protobuf.MessageType_MESSAGE:
		body = &protobuf.Message{}
	case protobuf.MessageType_MESSAGE_ACK:
		body = &protobuf.MessageAck{}
	case protobuf.MessageType_PUSH:
		body = &protobuf.PushMessage{}
	case protobuf.MessageType_ERROR:
		body = &protobuf.ErrorResponse{}
	default:
		return nil
	}

	if err := proto.Unmarshal(data, body); err != nil {
		return nil
	}

	return body
}

// 消息工厂
type MessageFactory struct {
	pool sync.Pool
}

// 创建消息工厂
func NewMessageFactory() *MessageFactory {
	return &MessageFactory{
		pool: sync.Pool{
			New: func() interface{} {
				return &Message{
					Header: &protobuf.Header{},
				}
			},
		},
	}
}

// 创建消息
func (f *MessageFactory) CreateMessage(msgType protobuf.MessageType, seq uint64) *Message {
	msg := f.pool.Get().(*Message)
	msg.Header = &protobuf.Header{
		Magic:     uint32(ProtocolMagic),
		Version:   uint32(ProtocolVersion),
		Type:      msgType,
		Seq:       seq,
		Timestamp: uint64(time.Now().UnixMilli()),
	}
	msg.Body = nil
	return msg
}

// 释放消息
func (f *MessageFactory) ReleaseMessage(msg *Message) {
	if msg != nil {
		msg.Header = nil
		msg.Body = nil
		f.pool.Put(msg)
	}
}

// 流式编解码器
type StreamCodec struct {
	codec  *DefaultCodec
	reader io.Reader
	writer io.Writer
	buffer *bytes.Buffer
	mu     sync.Mutex
}

// 创建流式编解码器
func NewStreamCodec(reader io.Reader, writer io.Writer) *StreamCodec {
	return &StreamCodec{
		codec:  NewDefaultCodec(),
		reader: reader,
		writer: writer,
		buffer: &bytes.Buffer{},
	}
}

// 写入消息
func (s *StreamCodec) WriteMessage(msg *Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.codec.EncodeToWriter(s.writer, msg)
}

// 读取消息
func (s *StreamCodec) ReadMessage() (*Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.codec.DecodeFromReader(s.reader)
}

// 批量编解码器
type BatchCodec struct {
	codec *DefaultCodec
}

// 创建批量编解码器
func NewBatchCodec() *BatchCodec {
	return &BatchCodec{
		codec: NewDefaultCodec(),
	}
}

// 批量编码
func (b *BatchCodec) EncodeBatch(messages []*Message) ([]byte, error) {
	var buffer bytes.Buffer

	// 写入消息数量
	count := uint32(len(messages))
	if err := binary.Write(&buffer, binary.BigEndian, count); err != nil {
		return nil, err
	}

	// 编码每个消息
	for _, msg := range messages {
		data, err := b.codec.Encode(msg)
		if err != nil {
			return nil, err
		}

		// 写入消息长度
		if err := binary.Write(&buffer, binary.BigEndian, uint32(len(data))); err != nil {
			return nil, err
		}

		// 写入消息数据
		buffer.Write(data)
	}

	return buffer.Bytes(), nil
}

// 批量解码
func (b *BatchCodec) DecodeBatch(data []byte) ([]*Message, error) {
	buffer := bytes.NewReader(data)

	// 读取消息数量
	var count uint32
	if err := binary.Read(buffer, binary.BigEndian, &count); err != nil {
		return nil, err
	}

	messages := make([]*Message, 0, count)

	// 解码每个消息
	for i := uint32(0); i < count; i++ {
		// 读取消息长度
		var msgLength uint32
		if err := binary.Read(buffer, binary.BigEndian, &msgLength); err != nil {
			return nil, err
		}

		// 读取消息数据
		msgData := make([]byte, msgLength)
		if _, err := io.ReadFull(buffer, msgData); err != nil {
			return nil, err
		}

		// 解码消息
		msg, err := b.codec.Decode(msgData)
		if err != nil {
			return nil, err
		}

		messages = append(messages, msg)
	}

	return messages, nil
}

// 压缩编解码器
type CompressedCodec struct {
	inner *DefaultCodec
	level int
}

// 创建压缩编解码器
func NewCompressedCodec(level int) *CompressedCodec {
	return &CompressedCodec{
		inner: NewDefaultCodec(),
		level: level,
	}
}

// 编码（带压缩）
func (c *CompressedCodec) Encode(msg *Message) ([]byte, error) {
	// 先使用内部编解码器编码
	data, err := c.inner.Encode(msg)
	if err != nil {
		return nil, err
	}

	// 如果数据较小，不压缩
	if len(data) < 256 {
		return data, nil
	}

	// 压缩数据
	var compressed bytes.Buffer
	writer, err := zlib.NewWriterLevel(&compressed, c.level)
	if err != nil {
		return nil, err
	}

	if _, err := writer.Write(data); err != nil {
		return nil, err
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	// 添加压缩标记
	result := []byte{0x01} // 0x01表示已压缩
	result = append(result, compressed.Bytes()...)

	return result, nil
}

// 解码（带解压）
func (c *CompressedCodec) Decode(data []byte) (*Message, error) {
	// 检查是否压缩
	if len(data) > 0 && data[0] == 0x01 {
		// 解压数据
		reader, err := zlib.NewReader(bytes.NewReader(data[1:]))
		if err != nil {
			return nil, err
		}
		defer reader.Close()

		decompressed, err := io.ReadAll(reader)
		if err != nil {
			return nil, err
		}

		data = decompressed
	}

	return c.inner.Decode(data)
}
