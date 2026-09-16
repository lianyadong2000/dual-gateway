// iot_loadtest IoT MQTT-SN 压测客户端（支持 udp / dtls / quic 三路传输）。
// 每设备一条独立 UDP socket 或 QUIC 连接：CONNECT -> CONNACK -> 周期 PINGREQ -> 收 CMD 回 CMDACK。
// 用法见 admin/apiTestStart：-transport -addr -devices -concurrency -keep-alive -ping-interval ...
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/dtls/v2"
	"github.com/quic-go/quic-go"
)

const (
	mqCONNECT  = 0x04
	mqCONNACK  = 0x05
	mqPINGREQ  = 0x16
	mqPINGRESP = 0x17
	mqCMD      = 0x20
	mqCMDACK   = 0x21
)

var (
	connectSent atomic.Int64
	connack     atomic.Int64
	active      atomic.Int64
	errCount    atomic.Int64
	cmdCount    atomic.Int64
	cmdAck      atomic.Int64
	dbgPrinted  atomic.Int64
)

type link interface {
	write([]byte) error
	readFrame() ([]byte, error)
	setReadDeadline(time.Duration) error
	close()
}

// ---------- UDP link ----------
type udpLink struct{ c *net.UDPConn }

func (u *udpLink) write(b []byte) error { _, err := u.c.Write(b); return err }
func (u *udpLink) close()               { u.c.Close() }
func (u *udpLink) readFrame() ([]byte, error) {
	buf := make([]byte, 65535)
	n, err := u.c.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

// ---------- QUIC stream link ----------
type quicLink struct {
	c      *quic.Conn
	stream *quic.Stream
}

func (q *quicLink) write(b []byte) error {
	q.stream.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err := q.stream.Write(b)
	return err
}
func (q *quicLink) close() {
	q.stream.Close()
	q.c.CloseWithError(0, "done")
}
func (q *quicLink) readFrame() ([]byte, error) { return readMQTTFrame(q.stream) }

// ---------- DTLS-PSK link ----------
type dtlsLink struct{ c net.Conn }

func (d *dtlsLink) write(b []byte) error {
	d.c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err := d.c.Write(b)
	return err
}
func (d *dtlsLink) close()               { d.c.Close() }
func (d *dtlsLink) readFrame() ([]byte, error) {
	// pion/dtls 一次 Read 返回一个完整 record，buffer 必须能装下；不能用流式 ReadFull 渐进读
	buf := make([]byte, 2048)
	n, err := d.c.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}
func (d *dtlsLink) setReadDeadline(dur time.Duration) error {
	return d.c.SetReadDeadline(time.Now().Add(dur))
}

func readMQTTFrame(r io.Reader) ([]byte, error) {
	var hdr [3]byte
	if _, err := io.ReadFull(r, hdr[:1]); err != nil {
		return nil, err
	}
	prefix := 1
	var total int
	if hdr[0] == 0x01 {
		if _, err := io.ReadFull(r, hdr[1:3]); err != nil {
			return nil, err
		}
		prefix = 3
		total = int(hdr[1])<<8 | int(hdr[2])
	} else {
		total = int(hdr[0])
	}
	if total < prefix+1 || total > 65535 {
		return nil, fmt.Errorf("bad frame len %d", total)
	}
	buf := make([]byte, total)
	copy(buf[:prefix], hdr[:prefix])
	if _, err := io.ReadFull(r, buf[prefix:]); err != nil {
		return nil, err
	}
	return buf, nil
}

func connectFrame(cid string, keep uint16) []byte {
	b := []byte{byte(6 + len(cid)), mqCONNECT, 0x02, 0x01, byte(keep >> 8), byte(keep)}
	return append(b, []byte(cid)...)
}

func pingFrame(cid string) []byte {
	b := []byte{byte(2 + len(cid)), mqPINGREQ}
	return append(b, []byte(cid)...)
}

func cmdAckFrame(cid, cmdID string) []byte {
	bodyLen := 1 + 1 + len(cid) + 1 + len(cmdID) + 1 + 1
	b := []byte{byte(1 + bodyLen), mqCMDACK, byte(len(cid))}
	b = append(b, []byte(cid)...)
	b = append(b, byte(len(cmdID)))
	b = append(b, []byte(cmdID)...)
	b = append(b, 0x00, 0x01) // code=OK, state=charging
	return b
}

func dialLink(transport, addr, cid string) (link, error) {
	switch transport {
	case "dtls":
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		raddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			return nil, err
		}
		c, err := dtls.DialWithContext(ctx, "udp", raddr, &dtls.Config{
			PSK: func(hint []byte) ([]byte, error) {
				return []byte("iot-default-psk-2026"), nil
			},
			PSKIdentityHint: []byte(cid),
			CipherSuites: []dtls.CipherSuiteID{
				dtls.TLS_PSK_WITH_AES_128_CCM,
				dtls.TLS_PSK_WITH_AES_128_CCM_8,
				dtls.TLS_PSK_WITH_AES_128_GCM_SHA256,
			},
		})
		if err != nil {
			return nil, err
		}
		return &dtlsLink{c: c}, nil
	case "quic":
		tlsc := &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"mqtt-sn"}}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		c, err := quic.DialAddr(ctx, addr, tlsc, &quic.Config{
			MaxIdleTimeout:  120 * time.Second,
			KeepAlivePeriod: 15 * time.Second,
		})
		if err != nil {
			return nil, err
		}
		st, err := c.OpenStreamSync(ctx)
		if err != nil {
			c.CloseWithError(0, "stream fail")
			return nil, err
		}
		return &quicLink{c: c, stream: st}, nil
	default: // udp
		raddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			return nil, err
		}
		c, err := net.DialUDP("udp4", nil, raddr)
		if err != nil {
			return nil, err
		}
		return &udpLink{c: c}, nil
	}
}

func runDevice(lk link, cid string, pingInterval int, done <-chan struct{}) {
	defer lk.close()

	connectSent.Add(1)
	if err := lk.write(connectFrame(cid, 60)); err != nil {
		errCount.Add(1)
		return
	}
	// 读 CONNACK
	_ = lk.setReadDeadline(5 * time.Second)
	resp, err := lk.readFrame()
	if err != nil {
		if dbgPrinted.Add(1) <= 3 {
			fmt.Fprintf(os.Stderr, "read connack fail: %v\n", err)
		}
		errCount.Add(1)
		return
	}
	// 期望 [0x03, 0x05, rc]
	if len(resp) >= 3 && resp[1] == mqCONNACK && resp[2] == 0x00 {
		connack.Add(1)
		active.Add(1)
	} else {
		// CONNACK 非 0 或异常：拒绝（可能限流）
		errCount.Add(1)
		return
	}

	pingT := time.NewTicker(time.Duration(pingInterval) * time.Second)
	defer pingT.Stop()
	for {
		select {
		case <-done:
			return
		case <-pingT.C:
			if err := lk.write(pingFrame(cid)); err != nil {
				errCount.Add(1)
				return
			}
			_ = lk.setReadDeadline(3 * time.Second)
			frame, err := lk.readFrame()
			if err != nil {
				// 读超时/错误：忽略，下个周期重试
				continue
			}
			if len(frame) >= 2 && frame[1] == mqCMD {
				cmdCount.Add(1)
				// 解析 cmdID 并回 CMDACK
				devLen := int(frame[2])
				idLen := int(frame[2+1+devLen])
				cmdID := string(frame[2+1+devLen+1 : 2+1+devLen+1+idLen])
				if err := lk.write(cmdAckFrame(cid, cmdID)); err == nil {
					cmdAck.Add(1)
				}
			}
		}
	}
}

// 扩展 deadline 能力
type deadlineLink interface {
	setReadDeadline(d time.Duration) error
}

func (u *udpLink) setReadDeadline(d time.Duration) error { return u.c.SetReadDeadline(time.Now().Add(d)) }
func (q *quicLink) setReadDeadline(d time.Duration) error {
	q.stream.SetReadDeadline(time.Now().Add(d))
	return nil
}

func main() {
	transport := flag.String("transport", "udp", "udp / dtls / quic")
	addr := flag.String("addr", "127.0.0.1:5683", "网关地址")
	devices := flag.Int("devices", 1000, "设备数")
	concurrency := flag.Int("concurrency", 100, "并发建连")
	keepAlive := flag.Int("keep-alive", 60, "保持时长（秒）")
	pingInterval := flag.Int("ping-interval", 10, "PINGREQ 间隔（秒，0=不发心跳）")
	deviceOffset := flag.Int("device-offset", 0, "设备号偏移")
	flag.Parse()

	if *pingInterval <= 0 {
		*pingInterval = 30
	}

	start := time.Now()
	sem := make(chan struct{}, *concurrency)
	var wg sync.WaitGroup
	done := make(chan struct{})

	go func() {
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				fmt.Printf("[%.1fs] connect_sent=%d connack=%d active=%d errors=%d cmd=%d cmdack=%d\n",
					time.Since(start).Seconds(), connectSent.Load(), connack.Load(),
					active.Load(), errCount.Load(), cmdCount.Load(), cmdAck.Load())
			case <-done:
				return
			}
		}
	}()

	// 建连阶段受 concurrency 限流；建连完成即释放信号量，
	// 长生命周期的 ping 循环不再占用，避免主循环阻塞形成死锁。
	for i := 0; i < *devices; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sem <- struct{}{}
			cid := fmt.Sprintf("device_%06d", *deviceOffset+i)
			lk, err := dialLink(*transport, *addr, cid)
			<-sem
			if err != nil {
				if errCount.Add(1) <= 3 {
					fmt.Fprintf(os.Stderr, "dial %s fail: %v\n", cid, err)
				}
				return
			}
			runDevice(lk, cid, *pingInterval, done)
		}(i)
	}

	// 保持到 keepAlive 后再结束
	time.Sleep(time.Duration(*keepAlive) * time.Second)
	close(done)
	wg.Wait()

	fmt.Printf("[DONE] elapsed=%dms connect_sent=%d connack=%d active=%d errors=%d cmd=%d cmdack=%d\n",
		time.Since(start).Milliseconds(), connectSent.Load(), connack.Load(),
		active.Load(), errCount.Load(), cmdCount.Load(), cmdAck.Load())
}
