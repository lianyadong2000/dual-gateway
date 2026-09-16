package mqttsn

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// 编码器
type Encoder struct {
	buffer bytes.Buffer
}

// 创建编码器
func NewEncoder() *Encoder {
	return &Encoder{}
}

// 编码CONNECT消息
func (e *Encoder) EncodeConnect(msg *ConnectMessage) ([]byte, error) {
	e.buffer.Reset()
	
	// 计算消息长度
	clientIDLen := len(msg.ClientID)
	length := 6 + clientIDLen // 2(header) + 4(flags+protocol+duration) + clientID
	
	// 写入长度
	if err := e.writeLength(length); err != nil {
		return nil, err
	}
	
	// 写入消息类型
	e.buffer.WriteByte(CONNECT)
	
	// 写入Flags
	e.buffer.WriteByte(msg.Flags)
	
	// 写入ProtocolID
	e.buffer.WriteByte(msg.ProtocolID)
	
	// 写入Duration
	if err := binary.Write(&e.buffer, binary.BigEndian, msg.Duration); err != nil {
		return nil, err
	}
	
	// 写入ClientID
	e.buffer.WriteString(msg.ClientID)
	
	return e.buffer.Bytes(), nil
}

// 编码PUBLISH消息
func (e *Encoder) EncodePublish(msg *PublishMessage) ([]byte, error) {
	e.buffer.Reset()
	
	// 计算消息长度
	dataLen := len(msg.Data)
	length := 7 + dataLen // 2(header) + 5(flags+topicid+msgid) + data
	
	// 写入长度
	if err := e.writeLength(length); err != nil {
		return nil, err
	}
	
	// 写入消息类型
	e.buffer.WriteByte(PUBLISH)
	
	// 写入Flags
	e.buffer.WriteByte(msg.Flags)
	
	// 写入TopicID
	if err := binary.Write(&e.buffer, binary.BigEndian, msg.TopicID); err != nil {
		return nil, err
	}
	
	// 写入MsgID
	if err := binary.Write(&e.buffer, binary.BigEndian, msg.MsgID); err != nil {
		return nil, err
	}
	
	// 写入Data
	e.buffer.Write(msg.Data)
	
	return e.buffer.Bytes(), nil
}

// 编码SUBSCRIBE消息
func (e *Encoder) EncodeSubscribe(msgID uint16, topicName string, qos uint8) ([]byte, error) {
	e.buffer.Reset()
	
	// 计算长度
	length := 5 + len(topicName) // 2(header) + 3(msgid+qos) + topicname
	
	// 写入长度
	if err := e.writeLength(length); err != nil {
		return nil, err
	}
	
	// 写入消息类型
	e.buffer.WriteByte(SUBSCRIBE)
	
	// 写入MsgID
	if err := binary.Write(&e.buffer, binary.BigEndian, msgID); err != nil {
		return nil, err
	}
	
	// 写入TopicName
	e.buffer.WriteString(topicName)
	
	// 写入QoS
	e.buffer.WriteByte(qos)
	
	return e.buffer.Bytes(), nil
}

// 写入长度
func (e *Encoder) writeLength(length int) error {
	if length < 0 || length > 65535 {
		return errors.New("invalid message length")
	}
	
	if length <= 255 {
		// 1字节长度
		e.buffer.WriteByte(byte(length))
	} else {
		// 3字节长度
		e.buffer.WriteByte(0x01) // 指示使用3字节长度
		if err := binary.Write(&e.buffer, binary.BigEndian, uint16(length)); err != nil {
			return err
		}
	}
	
	return nil
}

// 解码器
type Decoder struct {
	reader io.Reader
}

// 创建解码器
func NewDecoder(reader io.Reader) *Decoder {
	return &Decoder{reader: reader}
}

// 解码消息
func (d *Decoder) Decode() (uint8, []byte, error) {
	// 读取长度
	length, err := d.readLength()
	if err != nil {
		return 0, nil, err
	}
	
	// 读取消息类型
	msgType, err := d.readByte()
	if err != nil {
		return 0, nil, err
	}
	
	// 读取消息体
	body := make([]byte, length-2)
	if _, err := io.ReadFull(d.reader, body); err != nil {
		return 0, nil, err
	}
	
	return msgType, body, nil
}

// 读取长度
func (d *Decoder) readLength() (int, error) {
	firstByte, err := d.readByte()
	if err != nil {
		return 0, err
	}
	
	if firstByte == 0x01 {
		// 3字节长度
		lengthBytes := make([]byte, 2)
		if _, err := io.ReadFull(d.reader, lengthBytes); err != nil {
			return 0, err
		}
		return int(binary.BigEndian.Uint16(lengthBytes)), nil
	}
	
	return int(firstByte), nil
}

// 读取字节
func (d *Decoder) readByte() (uint8, error) {
	b := make([]byte, 1)
	if _, err := io.ReadFull(d.reader, b); err != nil {
		return 0, err
	}
	return b[0], nil
}

// 解码CONNECT消息
func DecodeConnect(data []byte) (*ConnectMessage, error) {
	if len(data) < 4 {
		return nil, errors.New("invalid CONNECT message length")
	}
	
	msg := &ConnectMessage{}
	msg.Flags = data[0]
	msg.ProtocolID = data[1]
	msg.Duration = binary.BigEndian.Uint16(data[2:4])
	
	if len(data) > 4 {
		msg.ClientID = string(data[4:])
	}
	
	return msg, nil
}

// 解码PUBLISH消息
func DecodePublish(data []byte) (*PublishMessage, error) {
	if len(data) < 5 {
		return nil, errors.New("invalid PUBLISH message length")
	}
	
	msg := &PublishMessage{}
	msg.Flags = data[0]
	msg.TopicID = binary.BigEndian.Uint16(data[1:3])
	msg.MsgID = binary.BigEndian.Uint16(data[3:5])
	
	if len(data) > 5 {
		msg.Data = data[5:]
	}
	
	return msg, nil
}

// 解码SUBSCRIBE消息
func DecodeSubscribe(data []byte) (uint16, string, uint8, error) {
	if len(data) < 4 {
		return 0, "", 0, errors.New("invalid SUBSCRIBE message length")
	}
	
	msgID := binary.BigEndian.Uint16(data[0:2])
	topicName := string(data[2 : len(data)-1])
	qos := data[len(data)-1]
	
	return msgID, topicName, qos, nil
}

// 编码CONNACK消息
func EncodeConnack(returnCode uint8) []byte {
	return []byte{0x03, CONNACK, returnCode}
}

// 编码PUBACK消息
func EncodePuback(topicID, msgID uint16, returnCode uint8) []byte {
	return []byte{
		0x07, // Length
		PUBACK,
		byte(topicID >> 8),
		byte(topicID),
		byte(msgID >> 8),
		byte(msgID),
		returnCode,
	}
}

// 编码PINGREQ消息
func EncodePingreq() []byte {
	return []byte{0x02, PINGREQ}
}

// 编码PINGRESP消息
func EncodePingresp() []byte {
	return []byte{0x02, PINGRESP}
}

// 编码DISCONNECT消息
func EncodeDisconnect() []byte {
	return []byte{0x02, DISCONNECT}
}

// 验证消息
func ValidateMessage(msgType uint8, data []byte) error {
	switch msgType {
	case CONNECT:
		if len(data) < 4 {
			return errors.New("invalid CONNECT message")
		}
		
	case PUBLISH:
		if len(data) < 5 {
			return errors.New("invalid PUBLISH message")
		}
		
	case SUBSCRIBE:
		if len(data) < 4 {
			return errors.New("invalid SUBSCRIBE message")
		}
		
	case PINGREQ, PINGRESP, DISCONNECT:
		if len(data) != 0 {
			return errors.New("invalid message length")
		}
		
	default:
		return fmt.Errorf("unknown message type: %d", msgType)
	}
	
	return nil
}


// 包级便捷编码函数（兼容既有调用方）
func EncodeConnect(msg *ConnectMessage) ([]byte, error) {
	return NewEncoder().EncodeConnect(msg)
}

func EncodePublish(msg *PublishMessage) ([]byte, error) {
	return NewEncoder().EncodePublish(msg)
}

func EncodeSubscribe(msgID uint16, topicName string, qos uint8) ([]byte, error) {
	return NewEncoder().EncodeSubscribe(msgID, topicName, qos)
}
