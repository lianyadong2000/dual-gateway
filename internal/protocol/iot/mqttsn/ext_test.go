package mqttsn

import (
	"bytes"
	"testing"
)

// STATUS 编解码往返
func TestStatusRoundTrip(t *testing.T) {
	for _, state := range []uint8{0, 1, 2, 3} {
		msg := EncodeStatus("pile_007", state)
		// 消息 = [len][0x1E][deviceID][state]；解码输入 = [deviceID][state]（去掉len与msgType）
		if msg[0] != byte(len("pile_007")+3) { // length = msgType(1) + deviceID + state(1)
			t.Errorf("status length byte = %d", msg[0])
		}
		deviceID, st, err := DecodeStatus(msg[2:])
		if err != nil {
			t.Fatal(err)
		}
		if deviceID != "pile_007" || st != state {
			t.Errorf("round trip = %s,%d want pile_007,%d", deviceID, st, state)
		}
	}
}

// FETCH 编解码往返
func TestFetchRoundTrip(t *testing.T) {
	msg := EncodeFetch("pile_009")
	deviceID, err := DecodeFetch(msg[2:])
	if err != nil {
		t.Fatal(err)
	}
	if deviceID != "pile_009" {
		t.Errorf("fetch round trip = %s", deviceID)
	}
}

// CMD 编解码往返（含设备ID、指令ID与负载）
func TestCmdRoundTrip(t *testing.T) {
	payload := []byte(`{"user_id":"u1","current":32}`)
	msg := EncodeCmd("pile_007", "cmd_123456", CmdTypeChargeStart, payload)
	deviceID, cmdID, cmdType, pl, err := DecodeCmd(msg[2:])
	if err != nil {
		t.Fatal(err)
	}
	if deviceID != "pile_007" || cmdID != "cmd_123456" || cmdType != CmdTypeChargeStart {
		t.Errorf("cmd = %s,%s,%d", deviceID, cmdID, cmdType)
	}
	if !bytes.Equal(pl, payload) {
		t.Errorf("payload mismatch: %s", pl)
	}
}

// CMDACK 编解码往返（含 deviceID）
func TestCmdAckRoundTrip(t *testing.T) {
	msg := EncodeCmdAck("pile_007", "cmd_123456", CmdAckOK, 1) // charging
	deviceID, cmdID, code, state, err := DecodeCmdAck(msg[2:])
	if err != nil {
		t.Fatal(err)
	}
	if deviceID != "pile_007" || cmdID != "cmd_123456" || code != CmdAckOK || state != 1 {
		t.Errorf("cmdack = %s,%s,%d,%d", deviceID, cmdID, code, state)
	}
}

// RETRYINFO 编解码往返
func TestRetryInfoRoundTrip(t *testing.T) {
	msg := EncodeRetryInfo(15)
	sec, err := DecodeRetryInfo(msg[2:])
	if err != nil {
		t.Fatal(err)
	}
	if sec != 15 {
		t.Errorf("retry sec = %d, want 15", sec)
	}
}

// 状态映射双向一致
func TestStateMapping(t *testing.T) {
	for b, s := range ByteToStateStr {
		if StateStrToByte[s] != b {
			t.Errorf("StateStrToByte[%s]=%d, want %d", s, StateStrToByte[s], b)
		}
	}
}

// 指令类型映射
func TestCmdTypeMapping(t *testing.T) {
	cases := map[string]uint8{
		"charge.start": CmdTypeChargeStart,
		"charge.stop":  CmdTypeChargeStop,
		"set.param":    CmdTypeSetParam,
		"query":        CmdTypeQuery,
	}
	for typ, b := range cases {
		if CmdTypeToByte(typ) != b {
			t.Errorf("CmdTypeToByte(%s)=%d", typ, CmdTypeToByte(typ))
		}
		if CmdTypeFromByte(b) != typ {
			t.Errorf("CmdTypeFromByte(%d)=%s", b, CmdTypeFromByte(b))
		}
	}
}

// 非法消息拒绝
func TestDecodeInvalid(t *testing.T) {
	if _, _, err := DecodeStatus([]byte{0x1E}); err == nil {
		t.Error("short STATUS should fail")
	}
	if _, err := DecodeFetch(nil); err == nil {
		t.Error("empty FETCH should fail")
	}
	if _, _, _, _, err := DecodeCmd([]byte{0x05, 0x20}); err == nil {
		t.Error("short CMD should fail")
	}
	if _, err := DecodeRetryInfo([]byte{0x04}); err == nil {
		t.Error("short RETRYINFO should fail")
	}
}
