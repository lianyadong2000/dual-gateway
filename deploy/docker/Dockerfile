# 双模网关多阶段构建（C端WS + IoT MQTT-SN/DTLS/QUIC 三路）
# 构建：docker build -t dual-gateway:latest -f deploy/docker/Dockerfile .
FROM golang:1.26-alpine AS builder

# 安装依赖
RUN apk add --no-cache git make

WORKDIR /app

# 复制依赖文件（利用缓存）
COPY go.mod go.sum ./
RUN go mod download

# 复制源码
COPY . .

# 编译（CGO 关闭，纯静态；quic-go 为纯 Go 实现）
# 主程序入口：main/gateway（注意不是 cmd/gateway）
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o gateway ./main/gateway

# 最终镜像
FROM alpine:3.20

# CA 证书 + 时区
RUN apk --no-cache add ca-certificates tzdata

# 创建非 root 用户
RUN addgroup -S app && adduser -S app -G app

WORKDIR /app

# 复制二进制
COPY --from=builder /app/gateway .

# 复制配置与密钥目录（config/keys 内含 JWT 密钥与 QUIC 证书；
# 若缺失，网关启动时 QUIC 自签证书可自动生成到该目录）
COPY config/ ./config/

# 赋予 config 目录写权限（QUIC 证书自动生成兜底；生产可用 Secret 挂载替换）
RUN chown -R app:app /app/config

# 非 root 运行
USER app

# 暴露端口：
#   8080/tcp  C 端 WebSocket（强在线、高实时）
#   5683/udp  IoT MQTT-SN 明文 UDP（弱在线设备）
#   5684/udp  IoT DTLS-PSK（加密 UDP，可配置禁用）
#   5685/udp  IoT MQTT-SN over QUIC（TLS1.3 + 0-RTT 重连 + 连接迁移）
#   9090/tcp  健康检查 / 指标
EXPOSE 8080 5683/udp 5684/udp 5685/udp 9090

# 健康检查
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD wget -qO- http://localhost:9090/health || exit 1

# 启动（配置文件名 config.yml；可通过 -config 覆盖，支持 yaml/yml/json）
CMD ["./gateway", "--config", "config/config.yml"]
