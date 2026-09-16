package mqttsn

import (
	"encoding/binary"
	"errors"
)

// MQTT-SN消息类型
const (
	ADVERTISE     = 0x00
	SEARCHGW      = 0x01
	GWINFO        = 0x02
	CONNECT       = 0x04
	CONNACK       = 0x05
	WILLTOPICREQ  = 0x06
	WILLTOPIC     = 0x07
	WILLMSGREQ    = 0x08
	WILLMSG       = 0x09
	REGISTER      = 0x0A
	REGACK        = 0x0B
	PUBLISH       = 0x0C
	PUBACK        = 0x0D
	PUBCOMP       = 0x0E
	PUBREC        = 0x0F
	PUBREL        = 0x10
	SUBSCRIBE     = 0x12
	SUBACK        = 0x13
	UNSUBSCRIBE   = 0x14
	UNSUBACK      = 0x15
	PINGREQ       = 0x16
	PINGRESP      = 0x17
	DISCONNECT    = 0x18
	WILLTOPICUPD  = 0x1A
	WILLTOPICRESP = 0x1B
	WILLMSGUPD    = 0x1C
	WILLMSGRESP   = 0x1D
)

// MQTT-SN消息头
type MsgSNHeader struct {
	Length  uint8 // 消息长度（1字节或3字节）
	MsgType uint8 // 消息类型
}

// CONNECT消息
type ConnectMessage struct {
	Flags      uint8
	ProtocolID uint8
	Duration   uint16
	ClientID   string
}

// CONNACK消息
type ConnackMessage struct {
	ReturnCode uint8
}

// PUBLISH消息
type PublishMessage struct {
	Flags   uint8
	TopicID uint16
	MsgID   uint16
	Data    []byte
}

// PUBACK消息
type PubackMessage struct {
	TopicID    uint16
	MsgID      uint16
	ReturnCode uint8
}

// 解析消息头，返回(消息类型, 消息体, 错误)
// data 必须是完整MQTT-SN报文（含长度字段），支持1字节与3字节长度编码
func ParseMessage(data []byte) (uint8, []byte, error) {
	if len(data) < 2 {
		return 0, nil, errors.New("packet too short")
	}

	var msgLength int
	var msgTypeIndex int

	if data[0] == 0x01 {
		// 3字节长度：0x01 + 2字节长度
		if len(data) < 4 {
			return 0, nil, errors.New("packet too short for 3-byte length")
		}
		msgLength = int(binary.BigEndian.Uint16(data[1:3]))
		msgTypeIndex = 3
	} else {
		// 1字节长度
		msgLength = int(data[0])
		msgTypeIndex = 1
	}

	if msgLength < msgTypeIndex+1 {
		return 0, nil, errors.New("invalid packet length")
	}

	if msgLength > len(data) {
		return 0, nil, errors.New("packet length mismatch")
	}

	return data[msgTypeIndex], data[msgTypeIndex+1 : msgLength], nil
}
