package iot

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
)

// quicConns 统计：当前 QUIC 连接数
var quicConnCount atomic.Int64

// GetQUICConnCount 当前 QUIC 连接数（健康检查指标）
func GetQUICConnCount() int64 { return quicConnCount.Load() }

var connOrPingCnt atomic.Int64

// Peer 抽象传输端点：UDP / DTLS / QUIC
type Peer struct {
	Addr   *net.UDPAddr
	Conn   net.Conn // DTLS（及未来流式传输）连接
	Quic   *quic.Conn
	Stream *quic.Stream
}

// String 端点标识（日志/Redis 展示）
func (p *Peer) String() string {
	if p == nil || p.Addr == nil {
		return "unknown"
	}
	return p.Addr.String()
}

// IP 源IP（用于源IP限流）
func (p *Peer) IP() string {
	if p == nil || p.Addr == nil {
		return ""
	}
	return p.Addr.IP.String()
}

// Send 按传输类型发送：QUIC 写关联流；DTLS 写连接；否则走 UDP
// 流式写带 5s deadline：客户端不读（流控满）时失败而非阻塞 worker
func (p *Peer) Send(server *Server, data []byte) error {
	if p == nil {
		return errors.New("nil peer")
	}
	if p.Quic != nil {
		if p.Stream == nil {
			return errors.New("quic peer without stream")
		}
		p.Stream.SetWriteDeadline(time.Now().Add(5 * time.Second))
		_, err := p.Stream.Write(data)
		return err
	}
	if p.Conn != nil {
		p.Conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		_, err := p.Conn.Write(data)
		return err
	}
	return server.SendUDP(data, p.Addr)
}

// quicLoop QUIC 监听循环（EarlyListener：支持 0-RTT）
func (s *Server) quicLoop(addr string) {
	defer s.wg.Done()

	tlsConf, err := loadQUICTLSConfig(s.Config.QUICCert, s.Config.QUICKey)
	if err != nil {
		s.Logger.Error("Failed to load QUIC TLS config", "error", err)
		return
	}

	idle := time.Duration(s.Config.QUICIdleSec) * time.Second
	if idle <= 0 {
		idle = 300 * time.Second
	}
	qconf := &quic.Config{
		Allow0RTT:               true, // 0-RTT 重连加速（TLS1.3 session resumption）
		MaxIdleTimeout:          idle,
		KeepAlivePeriod:         idle / 3,
		MaxIncomingStreams:      8, // 每连接少量并发流（MQTT-SN 帧序列，默认单流）
		HandshakeIdleTimeout:    60 * time.Second, // 5万并发握手时队列积压，放宽默认10s超时
		InitialStreamReceiveWindow: 64 * 1024,
		MaxStreamReceiveWindow:     1 << 20,
	}

	// 自建 UDPConn + 大缓冲：5万连接 Initial 突发（~60MB）不丢包
	udpConn, err := net.ListenUDP("udp", quicAddrFor(addr))
	if err != nil {
		s.Logger.Error("Failed to bind QUIC UDP", "error", err, "addr", addr)
		return
	}
	if err := udpConn.SetReadBuffer(32 * 1024 * 1024); err != nil {
		s.Logger.Warn("Failed to set QUIC read buffer", "error", err)
	}
	if err := udpConn.SetWriteBuffer(32 * 1024 * 1024); err != nil {
		s.Logger.Warn("Failed to set QUIC write buffer", "error", err)
	}
	tr := &quic.Transport{Conn: udpConn}
	listener, err := tr.ListenEarly(tlsConf, qconf)
	if err != nil {
		udpConn.Close()
		s.Logger.Error("Failed to start QUIC listener", "error", err, "addr", addr)
		return
	}
	s.quicLis = listener
	s.quicTrans = tr
	s.Logger.Info("IoT QUIC gateway started", "addr", addr, "0rtt", true, "idle", idle.String())

	for {
		conn, err := listener.Accept(s.Ctx)
		if err != nil {
			select {
			case <-s.Ctx.Done():
				return
			default:
			}
			s.Logger.Error("QUIC accept error", "error", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		quicConnCount.Add(1)
		s.wg.Add(1)
		go s.handleQUICConn(conn)
	}
}

// handleQUICConn 处理单条 QUIC 连接：串行承接各流（MQTT-SN 帧序列在主流上）。
// 每连接仅 1 goroutine（流内读完再 Accept 下一流），5万连接场景下减少调度开销；
// 多流设备（如控制/数据流分离）可配置 QUICSingleStream=false 回退到每流独立 goroutine。
func (s *Server) handleQUICConn(conn *quic.Conn) {
	defer s.wg.Done()
	defer quicConnCount.Add(-1)
	defer s.Handler.dropDeviceByConn(s, conn) // 连接断开：该连接上的设备下线（影子 MarkOffline）

	for {
		stream, err := conn.AcceptStream(s.Ctx)
		if err != nil {
			return
		}
		s.handleQUICStream(conn, stream) // 同步读完整条流
	}
}

// handleQUICStream 单流读循环：拆 MQTT-SN 帧 → 统一入 worker 池处理
// 与 UDP/DTLS 一致走 worker 池：限制并发 Redis 往返（5万连接不打爆连接池）
func (s *Server) handleQUICStream(conn *quic.Conn, stream *quic.Stream) {
	defer stream.Close()

	peer := &Peer{
		Addr:   udpAddrOf(conn.RemoteAddr()),
		Quic:   conn,
		Stream: stream,
	}

	for {
		frame, err := readMQTTFrame(stream)
		if err != nil {
			if !isClosedConnErr(err) {
				s.Logger.Warn("QUIC stream read ended", "remote", peer.String(), "error", err)
			}
			// 流结束：该流上的设备下线（若存在）
			s.Handler.dropDeviceByStream(s, stream)
			return
		}
		if len(frame) >= 2 && frame[1] == 0x04 {
			// 0x04=CONNECT：仅调试统计用（前30条）
			n := connOrPingCnt.Add(1)
			if n <= 30 {
				s.Logger.Info("QUIC svr recv CONNECT", "stream", stream.StreamID())
			}
		}
		// 非阻塞入队 worker 池；队列满时直接丢弃（读循环永不被处理阻塞）
		select {
		case s.packetCh <- udpPacket{data: frame, peer: peer}:
		default:
			s.Dropped.Add(1)
		}
	}
}

// readMQTTFrame 从字节流读取一帧 MQTT-SN 消息（支持 1字节/3字节 length 前缀）
func readMQTTFrame(r io.Reader) ([]byte, error) {
	var hdr [3]byte
	if _, err := io.ReadFull(r, hdr[:1]); err != nil {
		return nil, err
	}
	prefixLen := 1
	var total int
	if hdr[0] == 0x01 {
		if _, err := io.ReadFull(r, hdr[1:3]); err != nil {
			return nil, err
		}
		prefixLen = 3
		total = int(binary.BigEndian.Uint16(hdr[1:3]))
	} else {
		total = int(hdr[0])
	}
	if total < prefixLen+1 || total > 65535 {
		return nil, fmt.Errorf("invalid MQTT-SN frame length %d", total)
	}
	buf := make([]byte, total)
	copy(buf, hdr[:prefixLen])
	if _, err := io.ReadFull(r, buf[prefixLen:]); err != nil {
		return nil, err
	}
	return buf, nil
}

// udpAddrOf 从任意 net.Addr 提取 *net.UDPAddr（QUIC RemoteAddr 为 *net.UDPAddr）
func udpAddrOf(addr net.Addr) *net.UDPAddr {
	if u, ok := addr.(*net.UDPAddr); ok {
		return u
	}
	host, port, err := net.SplitHostPort(addr.String())
	if err != nil {
		return &net.UDPAddr{IP: net.IPv4zero, Port: 0}
	}
	ip := net.ParseIP(host)
	if ip == nil {
		ip = net.IPv4zero
	}
	var p int
	fmt.Sscanf(port, "%d", &p)
	return &net.UDPAddr{IP: ip, Port: p}
}

// quicAddrFor 解析 "host:port" 为 UDP 地址
func quicAddrFor(addr string) *net.UDPAddr {
	ua, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return &net.UDPAddr{IP: net.IPv4zero, Port: 5685}
	}
	return ua
}

// isClosedConnErr 判定连接关闭类错误（不视为异常）
func isClosedConnErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
		return true
	}
	var appErr *quic.ApplicationError
	if errors.As(err, &appErr) {
		return true
	}
	var idleErr *quic.IdleTimeoutError
	if errors.As(err, &idleErr) {
		return true
	}
	var resetErr *quic.StatelessResetError
	if errors.As(err, &resetErr) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}
	return false
}

// SendToDevice 按设备传输类型发送（在线直发：QUIC 写连接流，UDP/DTLS 写端点）
func (s *Server) SendToDevice(deviceID string, data []byte) error {
	dev := s.Handler.DeviceManager.Get(deviceID)
	if dev == nil {
		return fmt.Errorf("device not online: %s", deviceID)
	}
	if dev.Peer == nil {
		return fmt.Errorf("device peer missing: %s", deviceID)
	}
	return dev.Peer.Send(s, data)
}
