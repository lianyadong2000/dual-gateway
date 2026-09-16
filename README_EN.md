# Dual-Gateway

> High-performance dual-stack access gateway: a highly concurrent IoT gateway that simultaneously supports tens of millions of C-end long connections and millions of IoT devices, with single-node management of millions of connections.
>
> A single process carries both C-end mobile-app WebSocket long connections and MQTT-SN access for millions of IoT devices, targeting IoT / connected-vehicle / EV-charging / smart-hardware scenarios.

## 1. Design Goals

| Goal | Description |
|---|---|
| Tens of millions of C-end long connections | Online push and remote control for mobile apps; horizontally scales to tens of millions of concurrent sessions per node |
| Millions of IoT devices | Mass access of weak-network, low-power devices; supports three transports: UDP / DTLS / QUIC |
| Single-node high performance | Access decoupling, lock-free counters, async external dependencies, connection pooling; tens of thousands to hundreds of thousands of concurrent sessions per node |
| High availability | Automatic degradation when Redis/Kafka is unavailable without blocking the hot path; graceful shutdown and shadow offline fallback |
| Single-node capacity | Millions of C-end TCP long connections; MQTT access for millions of IoT devices |

## 2. Overall Architecture

## 3. Features

### 3.1 C-End Access (WebSocket + Protobuf)

* Protocol: `ws://host:8080/ws`, custom binary frame `[2B headerLen][protobuf Header][protobuf Body]`, magic number `0xABCD`.
* Message types: `AUTH / AUTH_ACK / HEARTBEAT / HEARTBEAT_ACK / MESSAGE / MESSAGE_ACK / PUSH / ERROR`.
* Authentication: JWT RS256, tokens carry `user_id/device_id`, with expiry and issuer validation; keys are auto-generated when missing (development mode).
* Keep-alive: server Ping every 30s, Pong refresh at 90s, offline after 120s of inactivity.
* Business messages: Kafka forwarding, shadow query, charging commands (`charge.start/stop`, `set.param`, `query`), shadow subscribe/unsubscribe.

### 3.2 IoT Access (MQTT-SN over Three Transports)

* **UDP plaintext (5683)**: connectionless datagrams, ideal for massive weak-network fleets; per-source-IP rate limiting to prevent spoofed floods.
* **DTLS-PSK (5684)**: per-device PSK lookup with a default PSK fallback for unregistered devices; supports AES-128-CCM/GCM cipher suites.
* **QUIC (5685)**: TLS 1.3 + 0-RTT resumption + connection migration; single serial stream keeps concurrent goroutine count low.
* MQTT-SN extended packets: `STATUS(0x1E)` charger state report, `FETCH(0x1F)` wake-up command pull, `CMD(0x20)` downstream, `CMDACK(0x21)` execution ack, `RETRYINFO(0x22)` backoff hint.
* Session management: reconnecting the same device replaces the old session; sessions are cleaned after 300s of inactivity and the shadow is marked offline.

### 3.3 Device Shadow

* Authoritative charger state (idle/charging/fault/preoccupy/offline) with an incrementing version number.
* **Atomic pre-occupy**: `charge.start` first calls `TryPreoccupy` (prevents double booking, rejects with 409).
* Automatic rollback when pre-occupy times out (recovers to idle after 60s without ack), offline TTL (120s) detection, and offline-scan rollback.
* State changes are pushed via Pub/Sub to C-end subscribers of that charger; C-end always queries the shadow and never connects directly to weak-network chargers.

### 3.4 Command Queue

* Commands are enqueued idempotently by unique ID; pending commands have a 24h TTL and are delivered in batches (≤10).
* Online devices: after a C-end enqueues a command, Pub/Sub immediately triggers in-node direct delivery; offline devices: the command stays queued and is pulled when the device wakes up (piggybacked on FETCH/PINGREQ).
* CMDACK closed loop: updates queue completion state, syncs the shadow, and publishes the result event to the C-end.

### 3.5 Cross-Cutting Capabilities

* **Redis**: online user/device registration, gateway registration/discovery, cross-node message forwarding, shadow and command-queue storage; automatically falls back to local memory when unreachable.
* **Kafka**: forwards C-end messages and IoT telemetry events to business systems; on failure, drops with a counter without blocking the protocol.
* **Tiered rate limiting**: CONNECT token bucket, UDP source-IP bucket, C-end accept throttling; fast rejection + RetryInfo when over limit.
* **Observability**: `/metrics` (Prometheus), `/health` (full runtime JSON), `/admin` visual dashboard (live metrics, shadow query, load-test orchestration, one-click end-to-end acceptance).
* **Graceful shutdown**: SIGINT/SIGTERM with a 30s timeout; all device shadows on this node are marked offline on shutdown.

## 4. Technical Highlights

1. **Zero blocking on the hot path**: IoT CONNECT only does an in-memory lookup and reply; all external dependencies (Redis/Kafka) are asynchronous. 50k concurrent handshakes are handled robustly with zero errors, and the connection pool is never exhausted.
2. **Read/write decoupling**: the read loop only enqueues (1M capacity absorbs bursts) while a worker pool processes in parallel; when the queue is full, packets are dropped with a counter rather than blocking the read loop.
3. **Extreme memory efficiency**: C-end buffers of 4KB, `sync.Pool` for datagrams/messages, lock-free `atomic` counters; a single C-end connection costs ~20KB. The DTLS read buffer is sized to MTU (1200) at 1.5KB/connection, ~65KB per connection resident; 5000 DTLS connections measured with zero errors and zero packet loss, proving hundred-thousand/million-scale online capacity.
4. **Unified transport abstraction**: `Peer` unifies UDP/DTLS/QUIC endpoints, decoupling business processing from the transport; downstream delivery automatically selects stream/connection/datagram.
5. **Degrade-first**: Redis/Kafka/JWT/QUIC certificates all have fallbacks; the node boots with zero dependencies (auto-generates keys / self-signed certificates).
6. **Built-in load test & acceptance**: the admin dashboard runs C-end/IoT load tests and the full "health → auth → command loop → shadow" acceptance with one click.

## 5. Roadmap

| Priority | Area | Details |
|---|---|---|
| High | Scale observability | Replace admin's `KEYS` full scan with `SCAN`; shadow/command counts via atomic stats instead of live key scans |
| High | Gateway identity | Make the `GATEWAY_ID` env var actually take effect, supporting stable identity and routing for K8s replicas |
| Medium | Precise command push | Build a `cmd_id → user` mapping to replace the cmd.result subscription-table broadcast |
| Medium | Multi-active / sharding | Consistent-hash sharding, cross-node command routing, UDP/QUIC LB integration guide |
| Medium | Transport extensions | DTLS load-test tooling and canary switch; configurable QUIC multi-stream (control/data separation) |
| Low | Protocol enhancement | Full MQTT-SN QoS2 semantics, TLS certificate rotation, quota/billing dimensions |
| Low | Protocol enhancement | MQTT over QUIC support |
| Low | Multi-tier architecture | Flexible multi-level networking expansion |
