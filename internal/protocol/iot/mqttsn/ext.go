// MQTT-SN 扩展消息（网关与弱在线桩之间的下行/状态通道）
// 使用 MQTT-SN 1.2 规范未定义的保留消息类型值（0x1E 之后）：
//
//	STATUS    0x1E  桩→网关  主动状态上报（充电开始/结束/故障等）
//	FETCH     0x1F  桩→网关  唤醒后拉取待执行指令
//	CMD       0x20  网关→桩  指令下发（含指令ID，桩侧幂等去重）
//	CMDACK    0x21  桩→网关  指令执行确认（成功/失败 + 结果状态）
//	RETRYINFO 0x22  网关→桩  重试指示（CONNACK 0x03 后附带重试等待秒数）
package mqttsn

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// 扩展消息类型
const (
	STATUS    = 0x1E
	FETCH     = 0x1F
	CMD       = 0x20
	CMDACK    = 0x21
	RETRYINFO = 0x22
)

// ---------- STATUS: 桩状态上报 ----------

// EncodeStatus 编码状态上报消息
// 格式: [length][0x1E][deviceID][state(1B)]
func EncodeStatus(deviceID string, state uint8) []byte {
	bodyLen := 1 + len(deviceID) + 1 // msgType + deviceID + state
	msg := make([]byte, 0, 1+bodyLen)
	msg = append(msg, byte(1+bodyLen)) // length = msgType + body
	msg = append(msg, STATUS)
	msg = append(msg, []byte(deviceID)...)
	msg = append(msg, state)
	return msg
}

// DecodeStatus 解码状态上报消息
func DecodeStatus(data []byte) (string, uint8, error) {
	if len(data) < 2 {
		return "", 0, errors.New("invalid STATUS message")
	}
	deviceID := string(data[:len(data)-1])
	state := data[len(data)-1]
	return deviceID, state, nil
}

// ---------- FETCH: 指令拉取 ----------

// EncodeFetch 编码指令拉取消息
// 格式: [length][0x1F][deviceID]
func EncodeFetch(deviceID string) []byte {
	msg := make([]byte, 0, 2+len(deviceID))
	msg = append(msg, byte(2+len(deviceID))) // length = msgType + deviceID
	msg = append(msg, FETCH)
	msg = append(msg, []byte(deviceID)...)
	return msg
}

// DecodeFetch 解码指令拉取消息
func DecodeFetch(data []byte) (string, error) {
	if len(data) < 1 {
		return "", errors.New("invalid FETCH message")
	}
	return string(data), nil
}

// ---------- CMD: 网关→桩 指令下发 ----------

// 指令类型（1字节，与业务类型映射）
const (
	CmdTypeChargeStart = 0x01 // 启动充电
	CmdTypeChargeStop  = 0x02 // 停止充电
	CmdTypeSetParam    = 0x03 // 参数设置
	CmdTypeQuery       = 0x04 // 状态查询
)

// CmdTypeNames 指令类型名映射
var CmdTypeNames = map[uint8]string{
	CmdTypeChargeStart: "charge.start",
	CmdTypeChargeStop:  "charge.stop",
	CmdTypeSetParam:    "set.param",
	CmdTypeQuery:       "query",
}

// EncodeCmd 编码指令下发消息
// 格式: [length][0x20][deviceIDLen(1B)][deviceID][cmdIDLen(1B)][cmdID][cmdType(1B)][payload...]
func EncodeCmd(deviceID, cmdID string, cmdType uint8, payload []byte) []byte {
	bodyLen := 1 + 1 + len(deviceID) + 1 + len(cmdID) + 1 + len(payload)
	msg := make([]byte, 0, 1+bodyLen)
	msg = append(msg, byte(1+bodyLen))
	msg = append(msg, CMD)
	msg = append(msg, byte(len(deviceID)))
	msg = append(msg, []byte(deviceID)...)
	msg = append(msg, byte(len(cmdID)))
	msg = append(msg, []byte(cmdID)...)
	msg = append(msg, cmdType)
	msg = append(msg, payload...)
	return msg
}

// DecodeCmd 解码指令下发消息
func DecodeCmd(data []byte) (deviceID, cmdID string, cmdType uint8, payload []byte, err error) {
	if len(data) < 3 {
		return "", "", 0, nil, errors.New("invalid CMD message")
	}
	devLen := int(data[0])
	if len(data) < 2+devLen+1 {
		return "", "", 0, nil, errors.New("invalid CMD message length")
	}
	deviceID = string(data[1 : 1+devLen])
	idLen := int(data[1+devLen])
	bodyStart := 2 + devLen
	if len(data) < bodyStart+idLen+1 {
		return "", "", 0, nil, errors.New("invalid CMD message length")
	}
	cmdID = string(data[bodyStart : bodyStart+idLen])
	cmdType = data[bodyStart+idLen]
	payload = data[bodyStart+idLen+1:]
	return deviceID, cmdID, cmdType, payload, nil
}

// ---------- CMDACK: 桩→网关 指令执行确认 ----------

// 指令执行结果码
const (
	CmdAckOK     = 0x00 // 执行成功
	CmdAckFail   = 0x01 // 执行失败
	CmdAckBusy   = 0x02 // 忙/拒绝
	CmdAckInvalid = 0x03 // 指令无效/未知
)

// EncodeCmdAck 编码指令确认消息
// 格式: [length][0x21][deviceIDLen(1B)][deviceID][cmdIDLen(1B)][cmdID][code(1B)][state(1B, 可选结果状态)]
func EncodeCmdAck(deviceID, cmdID string, code uint8, resultState uint8) []byte {
	bodyLen := 1 + 1 + len(deviceID) + 1 + len(cmdID) + 1 + 1
	msg := make([]byte, 0, 1+bodyLen)
	msg = append(msg, byte(1+bodyLen))
	msg = append(msg, CMDACK)
	msg = append(msg, byte(len(deviceID)))
	msg = append(msg, []byte(deviceID)...)
	msg = append(msg, byte(len(cmdID)))
	msg = append(msg, []byte(cmdID)...)
	msg = append(msg, code)
	msg = append(msg, resultState)
	return msg
}

// DecodeCmdAck 解码指令确认消息
func DecodeCmdAck(data []byte) (deviceID, cmdID string, code uint8, state uint8, err error) {
	if len(data) < 5 {
		return "", "", 0, 0, errors.New("invalid CMDACK message")
	}
	devLen := int(data[0])
	if len(data) < 2+devLen+1+1 {
		return "", "", 0, 0, errors.New("invalid CMDACK message length")
	}
	deviceID = string(data[1 : 1+devLen])
	idLen := int(data[1+devLen])
	bodyStart := 2 + devLen
	if len(data) < bodyStart+idLen+2 {
		return "", "", 0, 0, errors.New("invalid CMDACK message length")
	}
	cmdID = string(data[bodyStart : bodyStart+idLen])
	code = data[bodyStart+idLen]
	state = data[bodyStart+idLen+1]
	return deviceID, cmdID, code, state, nil
}

// ---------- RETRYINFO: 重试指示 ----------

// EncodeRetryInfo 编码重试指示消息
// 格式: [length][0x22][retryAfterSec(2B BE)]
func EncodeRetryInfo(retryAfterSec uint16) []byte {
	msg := make([]byte, 0, 5)
	msg = append(msg, 0x04) // length = msgType + 2
	msg = append(msg, RETRYINFO)
	msg = append(msg, byte(retryAfterSec>>8), byte(retryAfterSec))
	return msg
}

// DecodeRetryInfo 解码重试指示消息
func DecodeRetryInfo(data []byte) (uint16, error) {
	if len(data) < 2 {
		return 0, errors.New("invalid RETRYINFO message")
	}
	return binary.BigEndian.Uint16(data[:2]), nil
}

// ---------- 状态值映射 ----------

// 业务状态字符串 → TLV 状态数值（与 shadow 包对齐）
var StateStrToByte = map[string]uint8{
	"idle":      0,
	"charging":  1,
	"fault":     2,
	"offline":   3,
	"preoccupy": 4,
}

// ByteToStateStr TLV 状态数值 → 业务状态字符串
var ByteToStateStr = map[uint8]string{
	0: "idle",
	1: "charging",
	2: "fault",
	3: "offline",
	4: "preoccupy",
}

// CmdTypeToByte 业务指令类型 → 协议指令类型
func CmdTypeToByte(t string) uint8 {
	switch t {
	case "charge.start":
		return CmdTypeChargeStart
	case "charge.stop":
		return CmdTypeChargeStop
	case "set.param":
		return CmdTypeSetParam
	case "query":
		return CmdTypeQuery
	}
	return 0
}

// CmdTypeFromByte 协议指令类型 → 业务指令类型
func CmdTypeFromByte(t uint8) string {
	if name, ok := CmdTypeNames[t]; ok {
		return name
	}
	return fmt.Sprintf("unknown_%d", t)
}
