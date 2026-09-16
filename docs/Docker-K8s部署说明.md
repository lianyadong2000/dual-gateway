# Dual-Gateway 容器化部署说明（Docker / Compose / Kubernetes）

> 本文对 `deploy/docker/` 与 `deploy/k8s/` 下现有脚本进行了逐项校验，并给出操作步骤与注意事项。
> 裸机部署见《安装部署与运维.md》。

---

## 一、现有部署脚本校验结论

### 1. `deploy/docker/Dockerfile` — ✅ 校验通过

| 检查项 | 结论 |
|---|---|
| 多阶段构建（builder → alpine:3.20） | 正确，最终镜像精简 |
| 依赖缓存（先 COPY go.mod/go.sum 再 download） | 正确 |
| 编译入口 `./main/gateway` | 正确（代码注释特别强调，避免误写成 `cmd/gateway`） |
| `CGO_ENABLED=0` 纯静态、`-trimpath -ldflags="-s -w"` | 正确，quic-go 为纯 Go，无 glibc 依赖 |
| 非 root 用户 `app` 运行 | 正确 |
| `COPY config/ ./config/` + `chown app`（QUIC 证书可自生成兜底） | 正确 |
| `EXPOSE 8080 5683/udp 5684/udp 5685/udp 9090` | 与实际监听完全一致 |
| `HEALTHCHECK` 访问 `:9090/health` | 正确（alpine 自带 wget） |

### 2. `deploy/docker/docker-compose.yml` — ✅ 校验通过（2 处提示）

| 检查项 | 结论 |
|---|---|
| redis 7-alpine + AOF + maxmemory 2gb/allkeys-lru | 合理 |
| kafka/zookeeper 挂在 `profiles: ["kafka"]`，默认不启动 | 正确，单机可零 Kafka 运行 |
| gateway-1 端口映射（TCP/UDP/监控齐全） | 正确 |
| gateway-2 仅映射 C 端与监控，UDP 错开避免冲突 | 正确，符合注释 |
| `REDIS_ADDR=redis:6379`、`KAFKA_BROKERS=kafka:29092` | 与 compose 服务名一致 |
| 网络 `gateway-net` + 数据卷 `redis-data` | 正确 |

> 提示：
> - `version: '3.8'` 在 Compose v2 已废弃但会被忽略，无影响，可删。
> - 未启用 kafka profile 时，gateway 打印 `kafka_available=false` 属预期降级。

### 3. `deploy/k8s/deployment.yaml` — ⚠️ 主体正确，2 处需注意

| 检查项 | 结论 |
|---|---|
| Deployment 多副本、端口含 protocol | 正确 |
| resources limits `8Gi/8cpu`（注释：5 万 QUIC ~6.5GB） | 与实测资源模型一致 |
| liveness/readiness 探活 `/health:9090` | 正确 |
| config 用 ConfigMap subPath 挂载、keys 用 Secret 只读 0400 | 正确 |
| C 端 Service `sessionAffinity: ClientIP` | 长连接推荐做法，正确 |
| IoT Service 三路 UDP（含云 LB 提示） | 正确 |
| 内嵌 ConfigMap `config.yml` 与仓库 `config/config.yml` 对齐 | 正确 |
| redis Deployment + Service | 正确 |

**需注意 / 建议修正：**

1. **`kafka-service` 未定义**：Deployment env 写了 `KAFKA_BROKERS=kafka-service:9092`，但该 YAML 只定义了 `redis-service`，没有 Kafka 的 Deployment/Service。
   - 影响：网关启动后连不上 Kafka 自动降级（不阻塞业务），但业务事件不会进 Kafka。
   - 处理：生产应部署独立 Kafka 并补一个 `kafka-service` Service；或在 ConfigMap 把 brokers 指向外部/托管 Kafka。
2. **`GATEWAY_ID=$(POD_NAME)` 当前不生效**：`internal/config` 的 `ApplyEnvOverrides` 读取了 `GATEWAY_ID` 但未实际赋值（空实现）。网关 ID 仍由 `hostname+pid` 生成。
   - 影响：同一 Pod 内多次重启 ID 变化，但因 hostname 稳定仍可接受；跨节点路由依赖 Redis 注册，不影响基本功能。
   - 建议：后续补全该环境变量覆盖逻辑。
3. **ConfigMap 中 `runtime.max_procs: 4`** 与 `cpu limit: 8` 不一致：建议改为 `8`（与 limit 对齐）。
4. **可选增强**：补 `PodDisruptionBudget`、Pod 反亲和（anti-affinity）与 HPA，以支撑多副本高可用与自动扩缩。

---

## 二、Docker 单机部署

```bash
cd dual-gateway   # 仓库根（含 go.mod）

# 构建镜像
docker build -t dual-gateway:latest -f deploy/docker/Dockerfile .

# 运行（含 Redis；Kafka 可选）
docker run -d --name dual-gateway \
  -p 8080:8080 -p 5683:5683/udp -p 5684:5684/udp -p 5685:5685/udp -p 9090:9090 \
  -e REDIS_ADDR=host.docker.internal:6379 \
  dual-gateway:latest

# 验证
curl http://127.0.0.1:9090/health
```

## 三、Docker Compose 部署

```bash
cd dual-gateway

# 一键启动（含 Redis；gateway-1/gateway-2 双实例演示）
docker compose -f deploy/docker/docker-compose.yml up -d --build

# 需要 Kafka 时
docker compose -f deploy/docker/docker-compose.yml --profile kafka up -d

docker compose -f deploy/docker/docker-compose.yml ps
curl http://127.0.0.1:9090/health
docker compose -f deploy/docker/docker-compose.yml down
```

端口访问：gateway-1 → `8080/5683/5684/5685/9090`；gateway-2 → C 端 `8081`、监控 `9091`。

## 四、Kubernetes 部署

```bash
# 1) 构建并推送镜像
docker build -t dual-gateway:latest -f deploy/docker/Dockerfile .
docker tag  dual-gateway:latest <registry>/dual-gateway:latest
docker push <registry>/dual-gateway:latest
# 把 deployment.yaml 中 image 改为 <registry>/dual-gateway:latest

# 2) 创建密钥 Secret（JWT + QUIC 证书，4 个 PEM）
go run ./test/loadtest/tokengen -keys-dir config/keys -count 1000
kubectl create secret generic gateway-keys --from-file=config/keys/

# 3) 部署
kubectl apply -f deploy/k8s/deployment.yaml

# 4) 验证
kubectl get pods -l app=dual-gateway
kubectl port-forward svc/dual-gateway-cend 8080:8080
curl http://<pod-ip>:9090/health
```

### 生产就绪清单
- [ ] 替换 `gateway-keys` Secret 为受信 CA 签发的 QUIC 证书与定期轮换的 JWT 密钥
- [ ] 补齐 `kafka-service`（或指向外部 Kafka），避免业务事件降级丢失
- [ ] ConfigMap `runtime.max_procs` 与 CPU limit 对齐
- [ ] 配置 UDP/QUIC LoadBalancer（云厂商支持）或 NodePort/hostPort 直连
- [ ] 挂接 `9090/metrics` 到 Prometheus，配置 `cend_conns`/`iot_devices`/`iot_dropped` 告警
- [ ] 按第 3 点建议补 PDB / anti-affinity / HPA
