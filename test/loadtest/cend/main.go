// cend_loadtest C 端 WebSocket 长连接压测客户端。
// 协议：WS /ws，帧为 [2B headerLen BE][protobuf Header][protobuf Body]，binary。
// 用法见 admin/apiTestStart：-conns -concurrency -tokens -keep-alive [-biz -biz-target -device-prefix -device-offset]
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	pb "dual-gateway/internal/protocol/cend/protobuf"
)

var (
	dialed    atomic.Int64
	authOK    atomic.Int64
	connFail  atomic.Int64
	bizSent   atomic.Int64
	bizOK     atomic.Int64
	biz409    atomic.Int64
	bizFail   atomic.Int64
)

func frame(typ pb.MessageType, seq uint64, body proto.Message) []byte {
	hdr := &pb.Header{
		Magic:     0xABCD,
		Version:   1,
		Type:      typ,
		Seq:       seq,
		Timestamp: uint64(time.Now().UnixMilli()),
		TraceId:   "cend-lt",
	}
	hb, _ := proto.Marshal(hdr)
	var bb []byte
	if body != nil {
		bb, _ = proto.Marshal(body)
	}
	out := make([]byte, 0, 2+len(hb)+len(bb))
	out = append(out, byte(len(hb)>>8), byte(len(hb)))
	out = append(out, hb...)
	out = append(out, bb...)
	return out
}

// readFrame 读一条 binary 消息，返回 (msgType, bodyBytes)
func readFrame(c *websocket.Conn) (pb.MessageType, []byte, error) {
	_, data, err := c.ReadMessage()
	if err != nil {
		return 0, nil, err
	}
	if len(data) < 2 {
		return 0, nil, fmt.Errorf("short frame")
	}
	hl := int(data[0])<<8 | int(data[1])
	if 2+hl > len(data) {
		return 0, nil, fmt.Errorf("bad header len")
	}
	hdr := &pb.Header{}
	if err := proto.Unmarshal(data[2:2+hl], hdr); err != nil {
		return 0, nil, err
	}
	return hdr.Type, data[2+hl:], nil
}

func runOne(addr string, token, devicePrefix string, deviceOffset, idx int,
	biz bool, bizTarget int, wg *sync.WaitGroup) {
	defer wg.Done()

	hdr := http.Header{}
	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = 10 * time.Second
	conn, _, err := dialer.Dial(addr, hdr)
	if err != nil {
		connFail.Add(1)
		return
	}
	defer conn.Close()
	dialed.Add(1)
	_ = conn.SetReadDeadline(time.Now().Add(15 * time.Second))

	// AUTH
	if err := conn.WriteMessage(websocket.BinaryMessage,
		frame(pb.MessageType_AUTH, 1, &pb.AuthRequest{Token: token})); err != nil {
		connFail.Add(1)
		return
	}
	typ, body, err := readFrame(conn)
	if err != nil || typ != pb.MessageType_AUTH_ACK {
		connFail.Add(1)
		return
	}
	ack := &pb.AuthResponse{}
	proto.Unmarshal(body, ack)
	if ack.Code != 200 {
		connFail.Add(1)
		return
	}
	authOK.Add(1)

	// 业务指令（可选）
	if biz && idx < bizTarget {
		target := fmt.Sprintf("%s%d", devicePrefix, deviceOffset+idx)
		_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err := conn.WriteMessage(websocket.BinaryMessage,
			frame(pb.MessageType_MESSAGE, 2, &pb.Message{
				To:      target,
				Type:    "charge.start",
				Content: `{"power_kw":7}`,
			})); err == nil {
			bizSent.Add(1)
			_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			if typ2, body2, err2 := readFrame(conn); err2 == nil && typ2 == pb.MessageType_MESSAGE_ACK {
				mack := &pb.MessageAck{}
				proto.Unmarshal(body2, mack)
				switch mack.Code {
				case 200:
					bizOK.Add(1)
				case 409:
					biz409.Add(1)
				default:
					bizFail.Add(1)
				}
			} else {
				bizFail.Add(1)
			}
		}
	}
}

func main() {
	addr := flag.String("addr", "ws://127.0.0.1:8080/ws", "websocket 地址")
	conns := flag.Int("conns", 1000, "建立连接数")
	concurrency := flag.Int("concurrency", 50, "并发建连 goroutine")
	tokensPath := flag.String("tokens", "build/tokens_quic.txt", "token 文件（每行一个）")
	keepAlive := flag.Int("keep-alive", 30, "保持时长（秒）")
	biz := flag.Bool("biz", false, "是否下发业务指令")
	bizTarget := flag.Int("biz-target", 0, "下发业务指令的连接数（0=全部）")
	devicePrefix := flag.String("device-prefix", "device_", "目标设备前缀")
	deviceOffset := flag.Int("device-offset", 0, "目标设备号偏移")
	flag.Parse()

	if *bizTarget <= 0 {
		*bizTarget = *conns
	}

	data, err := os.ReadFile(*tokensPath)
	if err != nil {
		log.Fatalf("read tokens: %v", err)
	}
	var tokens []string
	for _, t := range splitLines(string(data)) {
		if t != "" {
			tokens = append(tokens, t)
		}
	}
	if len(tokens) == 0 {
		log.Fatalf("no tokens in %s", *tokensPath)
	}

	start := time.Now()
	sem := make(chan struct{}, *concurrency)
	var wg sync.WaitGroup

	go func() {
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				fmt.Printf("[%.1fs] dialed=%d auth_ok=%d conn_fail=%d biz_sent=%d biz_ok=%d biz_409=%d biz_fail=%d active=%d\n",
					time.Since(start).Seconds(), dialed.Load(), authOK.Load(), connFail.Load(),
					bizSent.Load(), bizOK.Load(), biz409.Load(), bizFail.Load(), authOK.Load())
			case <-time.After(time.Duration(*keepAlive+5) * time.Second):
				return
			}
		}
	}()

	for i := 0; i < *conns; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer func() { <-sem }()
			tok := tokens[i%len(tokens)]
			runOne(*addr, tok, *devicePrefix, *deviceOffset, i, *biz, *bizTarget, &wg)
		}(i)
	}
	wg.Wait()

	// 保持 keep-alive（已完成建连与鉴权，等待业务/后台观察）
	time.Sleep(time.Duration(*keepAlive) * time.Second)

	fmt.Printf("[DONE] elapsed=%dms dialed=%d auth_ok=%d conn_fail=%d biz_sent=%d biz_ok=%d biz_409=%d biz_fail=%d\n",
		time.Since(start).Milliseconds(), dialed.Load(), authOK.Load(), connFail.Load(),
		bizSent.Load(), bizOK.Load(), biz409.Load(), bizFail.Load())
}

func splitLines(s string) []string {
	var out []string
	cur := ""
	for _, r := range s {
		if r == '\n' || r == '\r' {
			if cur != "" {
				out = append(out, cur)
			}
			cur = ""
		} else {
			cur += string(r)
		}
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}
