// cmd/debug_ws/main.go —— 调试C端WebSocket协议
package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	"dual-gateway/internal/protocol/cend/protobuf"
)

func main() {
	// 读取第一个token
	file, err := os.Open("tokens.txt")
	if err != nil {
		fmt.Println("open tokens:", err)
		return
	}
	scanner := bufio.NewScanner(file)
	scanner.Scan()
	token := scanner.Text()
	file.Close()

	conn, _, err := websocket.DefaultDialer.Dial("ws://127.0.0.1:8080/ws", nil)
	if err != nil {
		fmt.Println("dial:", err)
		return
	}
	defer conn.Close()

	// 发送AUTH
	header := &protobuf.Header{
		Magic:     0xABCD,
		Version:   1,
		Type:      protobuf.MessageType_AUTH,
		Seq:       1,
		Timestamp: uint64(time.Now().UnixMilli()),
	}
	hBytes, _ := proto.Marshal(header)
	authReq := &protobuf.AuthRequest{Token: token, DeviceId: "debug-dev"}
	bBytes, _ := proto.Marshal(authReq)
	msg := make([]byte, 0, 2+len(hBytes)+len(bBytes))
	msg = append(msg, byte(len(hBytes)>>8), byte(len(hBytes)))
	msg = append(msg, hBytes...)
	msg = append(msg, bBytes...)
	conn.WriteMessage(websocket.BinaryMessage, msg)
	fmt.Printf("sent %d bytes, header len=%d\n", len(msg), len(hBytes))
	fmt.Println("header hex:", hex.EncodeToString(hBytes))

	// 读取响应
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, data, err := conn.ReadMessage()
	if err != nil {
		fmt.Println("read:", err)
		return
	}
	fmt.Printf("recv %d bytes: %s\n", len(data), hex.EncodeToString(data[:min(64, len(data))]))

	// 长度前缀解析
	if len(data) >= 4 {
		hl := int(data[0])<<8 | int(data[1])
		h := &protobuf.Header{}
		if err := proto.Unmarshal(data[2:2+hl], h); err == nil {
			fmt.Printf("headerLen=%d magic=0x%X version=%d type=%v seq=%d trace=%q\n",
				hl, h.Magic, h.Version, h.Type, h.Seq, h.TraceId)
			fmt.Println("body len:", len(data)-2-hl)
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
