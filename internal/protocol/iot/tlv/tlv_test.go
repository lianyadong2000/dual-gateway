package tlv

import (
	"bytes"
	"io"
	"testing"
)

func TestEncodeDecodeBasicTypes(t *testing.T) {
	encoder := NewEncoder()
	if err := encoder.Add(TypeTemperature, uint16(2550)); err != nil {
		t.Fatalf("Add temperature: %v", err)
	}
	if err := encoder.Add(TypeDeviceID, "device-001"); err != nil {
		t.Fatalf("Add device id: %v", err)
	}
	if err := encoder.Add(TypeBattery, uint8(85)); err != nil {
		t.Fatalf("Add battery: %v", err)
	}

	data := encoder.Bytes()

	decoder := NewDecoder(data)
	tlvs, err := decoder.DecodeAll()
	if err != nil {
		t.Fatalf("DecodeAll failed: %v", err)
	}

	if len(tlvs) != 3 {
		t.Fatalf("decoded %d TLVs, want 3", len(tlvs))
	}

	// 温度
	if tlvs[0].Type != TypeTemperature {
		t.Errorf("type = %d, want %d", tlvs[0].Type, TypeTemperature)
	}
	temp, err := tlvs[0].GetUint16()
	if err != nil || temp != 2550 {
		t.Errorf("temperature = %d (err %v), want 2550", temp, err)
	}

	// 设备ID
	if tlvs[1].Type != TypeDeviceID {
		t.Errorf("type = %d, want %d", tlvs[1].Type, TypeDeviceID)
	}
	if id := tlvs[1].GetString(); id != "device-001" {
		t.Errorf("device id = %q, want %q", id, "device-001")
	}

	// 电量
	battery, err := tlvs[2].GetUint8()
	if err != nil || battery != 85 {
		t.Errorf("battery = %d (err %v), want 85", battery, err)
	}
}

func TestEncodeDecodeVariousTypes(t *testing.T) {
	encoder := NewEncoder()
	encoder.Add(TypeVoltage, uint16(3500)) // 3.5V
	encoder.Add(TypeHumidity, uint16(6000))
	encoder.Add(TypeStatus, uint8(0x01))
	encoder.Add(TypeAlarm, uint8(0x00))
	encoder.Add(TypeGPS, []byte{31, 230, 121, 33})
	encoder.Add(TypeTimestamp, uint32(1700000000))

	data := encoder.Bytes()
	decoder := NewDecoder(data)
	tlvs, err := decoder.DecodeAll()
	if err != nil {
		t.Fatalf("DecodeAll failed: %v", err)
	}

	if len(tlvs) != 6 {
		t.Fatalf("decoded %d TLVs, want 6", len(tlvs))
	}

	voltage, _ := tlvs[0].GetUint16()
	if voltage != 3500 {
		t.Errorf("voltage = %d, want 3500", voltage)
	}
	humidity, _ := tlvs[1].GetUint16()
	if humidity != 6000 {
		t.Errorf("humidity = %d, want 6000", humidity)
	}
	status, _ := tlvs[2].GetUint8()
	if status != 0x01 {
		t.Errorf("status = %d, want 1", status)
	}
	ts, _ := tlvs[5].GetUint32()
	if ts != 1700000000 {
		t.Errorf("timestamp = %d, want 1700000000", ts)
	}
}

func TestDecodeErrors(t *testing.T) {
	// 空数据
	decoder := NewDecoder(nil)
	if _, err := decoder.DecodeAll(); err != nil {
		t.Errorf("empty data should decode to empty, got %v", err)
	}

	// 数据截断
	decoder = NewDecoder([]byte{TypeTemperature, 0x00, 0x02, 0x01})
	if _, err := decoder.DecodeAll(); err != io.ErrUnexpectedEOF {
		t.Errorf("truncated data: expected ErrUnexpectedEOF, got %v", err)
	}
}

func TestGetString(t *testing.T) {
	encoder := NewEncoder()
	encoder.Add(TypeDeviceID, "abc-123")

	tlvs, err := NewDecoder(encoder.Bytes()).DecodeAll()
	if err != nil {
		t.Fatalf("DecodeAll failed: %v", err)
	}

	if got := tlvs[0].GetString(); got != "abc-123" {
		t.Errorf("GetString = %q, want %q", got, "abc-123")
	}
}

func TestLargeValue(t *testing.T) {
	encoder := NewEncoder()
	big := bytes.Repeat([]byte{0xAB}, 1000)
	if err := encoder.Add(TypeGPS, big); err != nil {
		t.Fatalf("Add large value: %v", err)
	}

	tlvs, err := NewDecoder(encoder.Bytes()).DecodeAll()
	if err != nil {
		t.Fatalf("DecodeAll failed: %v", err)
	}

	if len(tlvs[0].Value) != 1000 {
		t.Errorf("value len = %d, want 1000", len(tlvs[0].Value))
	}
	if !bytes.Equal(tlvs[0].Value, big) {
		t.Error("value mismatch")
	}
}
