package tlv

import (
    "bytes"
    "encoding/binary"
    "errors"
    "fmt"
    "io"
)

// TLV类型定义
const (
    TypeTemperature = 0x01 // 温度
    TypeHumidity    = 0x02 // 湿度
    TypeVoltage     = 0x03 // 电压
    TypeStatus      = 0x04 // 状态
    TypeAlarm       = 0x05 // 告警
    TypeDoorBell    = 0x06 // 门铃
    TypeGPS         = 0x07 // GPS位置
    TypeTimestamp   = 0x08 // 时间戳
    TypeDeviceID    = 0x09 // 设备ID
    TypeBattery     = 0x0A // 电池电量
    TypeSignal      = 0x0B // 信号强度
)

// TLV结构
type TLV struct {
    Type   uint8
    Length uint16
    Value  []byte
}

// TLV编码器
type Encoder struct {
    buffer bytes.Buffer
}

// 创建编码器
func NewEncoder() *Encoder {
    return &Encoder{}
}

// 添加TLV
func (e *Encoder) Add(tlvType uint8, value interface{}) error {
    var valueBytes []byte
    
    switch v := value.(type) {
    case uint8:
        valueBytes = []byte{v}
    case uint16:
        valueBytes = make([]byte, 2)
        binary.BigEndian.PutUint16(valueBytes, v)
    case uint32:
        valueBytes = make([]byte, 4)
        binary.BigEndian.PutUint32(valueBytes, v)
    case uint64:
        valueBytes = make([]byte, 8)
        binary.BigEndian.PutUint64(valueBytes, v)
    case int16:
        valueBytes = make([]byte, 2)
        binary.BigEndian.PutUint16(valueBytes, uint16(v))
    case int32:
        valueBytes = make([]byte, 4)
        binary.BigEndian.PutUint32(valueBytes, uint32(v))
    case float32:
        valueBytes = make([]byte, 4)
        binary.BigEndian.PutUint32(valueBytes, uint32(v*100)) // 保留2位小数
    case string:
        valueBytes = []byte(v)
    case []byte:
        valueBytes = v
    default:
        return fmt.Errorf("unsupported type: %T", value)
    }
    
    if len(valueBytes) > 65535 {
        return errors.New("value too long")
    }
    
    // 写入Type
    e.buffer.WriteByte(tlvType)
    
    // 写入Length
    lengthBytes := make([]byte, 2)
    binary.BigEndian.PutUint16(lengthBytes, uint16(len(valueBytes)))
    e.buffer.Write(lengthBytes)
    
    // 写入Value
    e.buffer.Write(valueBytes)
    
    return nil
}

// 获取编码后的字节
func (e *Encoder) Bytes() []byte {
    return e.buffer.Bytes()
}

// TLV解码器
type Decoder struct {
    data []byte
    pos  int
}

// 创建解码器
func NewDecoder(data []byte) *Decoder {
    return &Decoder{
        data: data,
        pos:  0,
    }
}

// 解码所有TLV
func (d *Decoder) DecodeAll() ([]TLV, error) {
    var tlvs []TLV
    
    for d.pos < len(d.data) {
        tlv, err := d.DecodeOne()
        if err != nil {
            return nil, err
        }
        tlvs = append(tlvs, tlv)
    }
    
    return tlvs, nil
}

// 解码单个TLV
func (d *Decoder) DecodeOne() (TLV, error) {
    if d.pos+3 > len(d.data) {
        return TLV{}, io.ErrUnexpectedEOF
    }
    
    tlv := TLV{}
    tlv.Type = d.data[d.pos]
    d.pos++
    
    tlv.Length = binary.BigEndian.Uint16(d.data[d.pos : d.pos+2])
    d.pos += 2
    
    if d.pos+int(tlv.Length) > len(d.data) {
        return TLV{}, io.ErrUnexpectedEOF
    }
    
    tlv.Value = make([]byte, tlv.Length)
    copy(tlv.Value, d.data[d.pos:d.pos+int(tlv.Length)])
    d.pos += int(tlv.Length)
    
    return tlv, nil
}

// 获取TLV值
func (t *TLV) GetUint8() (uint8, error) {
    if t.Length != 1 {
        return 0, errors.New("invalid uint8 length")
    }
    return t.Value[0], nil
}

func (t *TLV) GetUint16() (uint16, error) {
    if t.Length != 2 {
        return 0, errors.New("invalid uint16 length")
    }
    return binary.BigEndian.Uint16(t.Value), nil
}

func (t *TLV) GetUint32() (uint32, error) {
    if t.Length != 4 {
        return 0, errors.New("invalid uint32 length")
    }
    return binary.BigEndian.Uint32(t.Value), nil
}

func (t *TLV) GetString() string {
    return string(t.Value)
}