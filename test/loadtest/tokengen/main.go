// tokengen 批量签发 C 端 JWT（RS256），供 cend_loadtest 使用。
// 用法: go run ./test/loadtest/tokengen -keys-dir config/keys -count 1000 -out build/tokens_quic.txt
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"dual-gateway/internal/auth"
)

func main() {
	keysDir := flag.String("keys-dir", "config/keys", "RSA keys 目录（含 jwt_private.pem/jwt_public.pem）")
	count := flag.Int("count", 1000, "生成 token 数量")
	out := flag.String("out", "build/tokens_quic.txt", "输出文件（每行一个 token）")
	prefix := flag.String("prefix", "device_", "设备号前缀（仅写入 token 的 device_id 声明）")
	flag.Parse()

	priv, err := os.ReadFile(filepath.Join(*keysDir, "jwt_private.pem"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "read private key:", err)
		os.Exit(1)
	}
	pub, err := os.ReadFile(filepath.Join(*keysDir, "jwt_public.pem"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "read public key:", err)
		os.Exit(1)
	}

	mgr, err := auth.NewJWTManager(priv, pub, "dual-gateway", 24*time.Hour)
	if err != nil {
		fmt.Fprintln(os.Stderr, "jwt manager:", err)
		os.Exit(1)
	}

	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "mkdir out:", err)
		os.Exit(1)
	}
	f, err := os.Create(*out)
	if err != nil {
		fmt.Fprintln(os.Stderr, "create out:", err)
		os.Exit(1)
	}
	defer f.Close()

	for i := 0; i < *count; i++ {
		uid := fmt.Sprintf("user_%06d", i)
		did := fmt.Sprintf("%s%06d", *prefix, i)
		tok, err := mgr.GenerateToken(uid, did)
		if err != nil {
			fmt.Fprintln(os.Stderr, "sign:", err)
			os.Exit(1)
		}
		fmt.Fprintln(f, tok)
	}
	fmt.Printf("generated %d tokens -> %s\n", *count, *out)
}
