package mqttsn

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeConnect(t *testing.T) {
	msg := &ConnectMessage{
		Flags:      0x04,
		ProtocolID: 0x01,
		Duration:   120,
		ClientID:   "device_000123",
	}

	data, err := EncodeConnect(msg)
	if err != nil {
		t.Fatalf("EncodeConnect failed: %v", err)
	}

	// 验证长度字段
	if data[0] != byte(len(data)) {
		t.Errorf("length field = %d, want %d", data[0], len(data))
	}
	if data[1] != CONNECT {
		t.Errorf("msg type = %d, want %d", data[1], CONNECT)
	}

	decoded, err := DecodeConnect(data[2:])
	if err != nil {
		t.Fatalf("DecodeConnect failed: %v", err)
	}

	if decoded.Flags != msg.Flags {
		t.Errorf("flags = %d, want %d", decoded.Flags, msg.Flags)
	}
	if decoded.ProtocolID != msg.ProtocolID {
		t.Errorf("protocol id = %d, want %d", decoded.ProtocolID, msg.ProtocolID)
	}
	if decoded.Duration != msg.Duration {
		t.Errorf("duration = %d, want %d", decoded.Duration, msg.Duration)
	}
	if decoded.ClientID != msg.ClientID {
		t.Errorf("client id = %q, want %q", decoded.ClientID, msg.ClientID)
	}
}

func TestEncodeDecodePublish(t *testing.T) {
	msg := &PublishMessage{
		Flags:   0x20, // QoS1
		TopicID: 0x0001,
		MsgID:   0x1234,
		Data:    []byte{0x01, 0x0A, 'h', 'e', 'l', 'l', 'o'},
	}

	data, err := EncodePublish(msg)
	if err != nil {
		t.Fatalf("EncodePublish failed: %v", err)
	}

	if data[0] != byte(len(data)) {
		t.Errorf("length field = %d, want %d", data[0], len(data))
	}
	if data[1] != PUBLISH {
		t.Errorf("msg type = %d, want %d", data[1], PUBLISH)
	}

	decoded, err := DecodePublish(data[2:])
	if err != nil {
		t.Fatalf("DecodePublish failed: %v", err)
	}

	if decoded.Flags != msg.Flags {
		t.Errorf("flags = %d, want %d", decoded.Flags, msg.Flags)
	}
	if decoded.TopicID != msg.TopicID {
		t.Errorf("topic id = %d, want %d", decoded.TopicID, msg.TopicID)
	}
	if decoded.MsgID != msg.MsgID {
		t.Errorf("msg id = %d, want %d", decoded.MsgID, msg.MsgID)
	}
	if !bytes.Equal(decoded.Data, msg.Data) {
		t.Errorf("data mismatch: %v != %v", decoded.Data, msg.Data)
	}
}

func TestParseMessage(t *testing.T) {
	// 1字节长度
	ping := []byte{0x02, PINGREQ}
	msgType, body, err := ParseMessage(ping)
	if err != nil {
		t.Fatalf("ParseMessage failed: %v", err)
	}
	if msgType != PINGREQ {
		t.Errorf("msg type = %d, want %d", msgType, PINGREQ)
	}
	if len(body) != 0 {
		t.Errorf("body = %v, want empty", body)
	}

	// 3字节长度
	connMsg := &ConnectMessage{Flags: 0, ProtocolID: 1, Duration: 60, ClientID: "device-3byte-len-very-long-name"}
	data, _ := EncodeConnect(connMsg)
	if len(data) > 255 {
		// 构造3字节长度消息
		longMsg := append([]byte{0x01, byte(len(data) >> 8), byte(len(data))}, data[1:]...)
		msgType, body, err := ParseMessage(longMsg)
		if err != nil {
			t.Fatalf("ParseMessage (3-byte) failed: %v", err)
		}
		if msgType != CONNECT {
			t.Errorf("msg type = %d, want %d", msgType, CONNECT)
		}
		if len(body) != len(data)-2 {
			t.Errorf("body len = %d, want %d", len(body), len(data)-2)
		}
	}
}

func TestParseMessageErrors(t *testing.T) {
	// 过短
	if _, _, err := ParseMessage([]byte{0x02}); err == nil {
		t.Error("expected error for short packet")
	}

	// 长度不匹配
	if _, _, err := ParseMessage([]byte{0x05, PINGREQ}); err == nil {
		t.Error("expected error for length mismatch")
	}
}

func TestEncodeSubscribe(t *testing.T) {
	data, err := EncodeSubscribe(0x0001, "topic/test", 0)
	if err != nil {
		t.Fatalf("EncodeSubscribe failed: %v", err)
	}

	if data[1] != SUBSCRIBE {
		t.Errorf("msg type = %d, want %d", data[1], SUBSCRIBE)
	}
	if len(data) != int(data[0]) {
		t.Errorf("length field = %d, want %d", data[0], len(data))
	}
}
