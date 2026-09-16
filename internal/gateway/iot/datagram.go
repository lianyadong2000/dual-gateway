package iot

import (
	"bytes"
	"compress/zlib"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"dual-gateway/pkg/logger"
	"dual-gateway/pkg/utils"
)

// 数据报类型
type DatagramType uint8

const (
	DatagramPlain     DatagramType = 0 // 普通UDP数据报
	DatagramDTLS      DatagramType = 1 // DTLS加密数据报
	DatagramMulticast DatagramType = 2 // 多播数据报
)

// 数据报结构
type Datagram struct {
	Type       DatagramType
	Data       []byte
	RemoteAddr *net.UDPAddr
	ReceivedAt time.Time
	Length     int
}

// 数据报池
type DatagramPool struct {
	pool sync.Pool
}

// 创建数据报池
func NewDatagramPool() *DatagramPool {
	return &DatagramPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &Datagram{}
			},
		},
	}
}

// 获取数据报
func (p *DatagramPool) Get() *Datagram {
	return p.pool.Get().(*Datagram)
}

// 释放数据报
func (p *DatagramPool) Put(d *Datagram) {
	d.Type = DatagramPlain
	d.Data = nil
	d.RemoteAddr = nil
	d.ReceivedAt = time.Time{}
	d.Length = 0
	p.pool.Put(d)
}

// 数据报处理器
type DatagramHandler struct {
	Logger        logger.Logger
	Handler       *MessageHandler
	Server        *Server
	Pool          *DatagramPool
	BufferPool    *utils.ByteSlicePool
	MaxPacketSize int
	Metrics       *DatagramMetrics
}

// 数据报指标
type DatagramMetrics struct {
	Received       atomic.Int64
	Processed      atomic.Int64
	Dropped        atomic.Int64
	Errors         atomic.Int64
	DTLSHandshakes atomic.Int64
	ProcessingTime atomic.Int64 // 总处理时间（微秒）
}

// 创建数据报处理器
func NewDatagramHandler(logger logger.Logger, handler *MessageHandler, maxPacketSize int) *DatagramHandler {
	return &DatagramHandler{
		Logger:        logger,
		Handler:       handler,
		Pool:          NewDatagramPool(),
		BufferPool:    utils.NewByteSlicePool(maxPacketSize),
		MaxPacketSize: maxPacketSize,
		Metrics:       &DatagramMetrics{},
	}
}

// 处理原始UDP数据
func (h *DatagramHandler) HandleRawData(data []byte, remoteAddr *net.UDPAddr) {
	h.Metrics.Received.Add(1)

	// 检查数据长度
	if len(data) > h.MaxPacketSize {
		h.Logger.Warn("Packet too large", "size", len(data), "max", h.MaxPacketSize)
		h.Metrics.Dropped.Add(1)
		return
	}

	if len(data) < 2 {
		h.Logger.Warn("Packet too small", "size", len(data))
		h.Metrics.Dropped.Add(1)
		return
	}

	// 获取数据报对象
	datagram := h.Pool.Get()
	defer h.Pool.Put(datagram)

	// 检测数据报类型
	datagram.Type = h.detectDatagramType(data)
	datagram.Data = data
	datagram.RemoteAddr = remoteAddr
	datagram.ReceivedAt = time.Now()
	datagram.Length = len(data)

	// 处理数据报
	h.processDatagram(datagram)
}

// 检测数据报类型
func (h *DatagramHandler) detectDatagramType(data []byte) DatagramType {
	// DTLS记录的第一个字节通常是0x16（握手）或0x17（应用数据）
	if len(data) > 0 {
		firstByte := data[0]
		if firstByte == 0x16 || firstByte == 0x17 {
			// 可能是DTLS数据
			return DatagramDTLS
		}
	}

	// 默认为普通UDP数据
	return DatagramPlain
}

// 处理数据报
func (h *DatagramHandler) processDatagram(d *Datagram) {
	startTime := time.Now()
	defer func() {
		elapsed := time.Since(startTime).Microseconds()
		h.Metrics.ProcessingTime.Add(elapsed)
	}()

	switch d.Type {
	case DatagramPlain:
		// 处理普通MQTT-SN消息
		h.Handler.HandlePacket(h.Server, d.Data, &Peer{Addr: d.RemoteAddr})
		h.Metrics.Processed.Add(1)

	case DatagramDTLS:
		// 处理DTLS消息
		h.handleDTLSDatagram(d)

	default:
		h.Logger.Warn("Unknown datagram type", "type", d.Type)
		h.Metrics.Dropped.Add(1)
	}
}

// 处理DTLS数据报
func (h *DatagramHandler) handleDTLSDatagram(d *Datagram) {
	// DTLS处理由专门的DTLS处理器完成
	// 这里只记录指标
	h.Metrics.DTLSHandshakes.Add(1)

	// 实际的DTLS处理在Server层完成
	h.Logger.Debug("DTLS datagram received",
		"remote", d.RemoteAddr.String(),
		"size", d.Length,
	)
}

// 创建发送数据报
func (h *DatagramHandler) CreateSendDatagram(data []byte, addr *net.UDPAddr) *Datagram {
	datagram := h.Pool.Get()
	datagram.Type = DatagramPlain
	datagram.Data = data
	datagram.RemoteAddr = addr
	datagram.ReceivedAt = time.Now()
	datagram.Length = len(data)
	return datagram
}

// 发送数据报
func (h *DatagramHandler) SendDatagram(server *Server, datagram *Datagram) error {
	_, err := server.UDPConn.WriteToUDP(datagram.Data, datagram.RemoteAddr)
	return err
}

// 批量处理数据报
func (h *DatagramHandler) ProcessBatch(datagrams []*Datagram) {
	var wg sync.WaitGroup

	for _, datagram := range datagrams {
		wg.Add(1)
		go func(d *Datagram) {
			defer wg.Done()
			h.processDatagram(d)
			h.Pool.Put(d)
		}(datagram)
	}

	wg.Wait()
}

// 获取数据报指标
func (h *DatagramHandler) GetMetrics() *DatagramMetrics {
	return h.Metrics
}

// 数据报分片器
type DatagramFragmenter struct {
	MaxFragmentSize int
}

// 创建数据报分片器
func NewDatagramFragmenter(maxFragmentSize int) *DatagramFragmenter {
	return &DatagramFragmenter{
		MaxFragmentSize: maxFragmentSize,
	}
}

// 分片数据
func (f *DatagramFragmenter) Fragment(data []byte) [][]byte {
	if len(data) <= f.MaxFragmentSize {
		return [][]byte{data}
	}

	var fragments [][]byte
	totalFragments := (len(data) + f.MaxFragmentSize - 1) / f.MaxFragmentSize

	for i := 0; i < totalFragments; i++ {
		start := i * f.MaxFragmentSize
		end := start + f.MaxFragmentSize
		if end > len(data) {
			end = len(data)
		}

		// 添加分片头
		fragmentHeader := []byte{
			byte(i + 1),          // 分片序号
			byte(totalFragments), // 总分片数
		}

		fragment := append(fragmentHeader, data[start:end]...)
		fragments = append(fragments, fragment)
	}

	return fragments
}

// 数据报重组器
type DatagramReassembler struct {
	mu          sync.Mutex
	fragments   map[string]map[int][]byte
	timeout     time.Duration
	lastCleanup time.Time
}

// 创建数据报重组器
func NewDatagramReassembler(timeout time.Duration) *DatagramReassembler {
	return &DatagramReassembler{
		fragments:   make(map[string]map[int][]byte),
		timeout:     timeout,
		lastCleanup: time.Now(),
	}
}

// 添加分片
func (r *DatagramReassembler) AddFragment(messageID string, fragmentNum, totalFragments int, data []byte) ([]byte, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 定期清理过期分片
	if time.Since(r.lastCleanup) > r.timeout {
		r.cleanup()
		r.lastCleanup = time.Now()
	}

	// 初始化分片存储
	if _, exists := r.fragments[messageID]; !exists {
		r.fragments[messageID] = make(map[int][]byte)
	}

	// 存储分片
	r.fragments[messageID][fragmentNum] = data

	// 检查是否收集齐所有分片
	if len(r.fragments[messageID]) == totalFragments {
		// 重组数据
		var completeData []byte
		for i := 1; i <= totalFragments; i++ {
			fragment, exists := r.fragments[messageID][i]
			if !exists {
				return nil, false
			}
			completeData = append(completeData, fragment...)
		}

		// 删除分片
		delete(r.fragments, messageID)

		return completeData, true
	}

	return nil, false
}

// 清理过期分片
func (r *DatagramReassembler) cleanup() {
	for messageID := range r.fragments {
		// 简单清理策略：删除所有超过timeout的分片
		delete(r.fragments, messageID)
	}
}

// 数据报压缩器
type DatagramCompressor struct {
	enabled bool
	level   int
}

// 创建数据报压缩器
func NewDatagramCompressor(enabled bool, level int) *DatagramCompressor {
	return &DatagramCompressor{
		enabled: enabled,
		level:   level,
	}
}

// 压缩数据
func (c *DatagramCompressor) Compress(data []byte) ([]byte, error) {
	if !c.enabled || len(data) < 64 {
		// 小数据不压缩
		return data, nil
	}

	// 使用zlib压缩
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

	return compressed.Bytes(), nil
}

// 解压数据
func (c *DatagramCompressor) Decompress(data []byte) ([]byte, error) {
	if !c.enabled || len(data) < 64 {
		// 小数据不解压
		return data, nil
	}

	reader, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return io.ReadAll(reader)
}
