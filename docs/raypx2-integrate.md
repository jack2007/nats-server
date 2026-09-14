# raypx2 × nats-p2p V2 集成说明

本文面向接入 `nats-p2p-server` 的 raypx2 客户端开发者与运维人员，描述当前 P2P V2 控制面、可靠信令、鉴权、集群和监控约定。

实现位于旁路包 `p2p/` 与包装进程 `cmd/nats-p2p-server`，不修改 `server/` 内核。部署时必须启动包装二进制；普通 `nats-server` 不会启动 P2P Coordinator。

## 1. 架构与职责边界

```text
nats-p2p-server（同一 OS 进程）
  ├─ nats-server 内核
  ├─ Auth Callout ── InProcessConn ──► $SYS.REQ.USER.AUTH
  └─ V2 Coordinator
       ├─ conn-local REGISTER
       ├─ queue-group CMD worker
       ├─ session owner / replica 同步
       ├─ SIGNAL 重试与去重
       └─ setup / disconnect / tombstone 过期处理
             ▲
             │ NATS 控制与信令
        raypx2 Agent
             │
             └──────── QUIC / UDP 数据面 ──────── 对端 Agent
```

| 组件 | 职责 | 不负责 |
| --- | --- | --- |
| NATS 内核 | CONNECT、权限、request-reply、路由、集群 | 不理解 P2P 会话状态 |
| Auth Callout | 校验 Agent CONNECT，签发节点级最小权限 JWT | 不处理会话或信令 |
| V2 Coordinator | 注册、会话与连接状态、可靠信令、集群复制、TURN 短时凭据 | 不承载 QUIC 数据 |
| raypx2 Agent | 保存节点身份、驱动状态机、交换 ICE SDP/candidate、建立 QUIC | 不访问 Coordinator 内部主题 |

包装进程的固定启动顺序是：拆出顶层 `p2p {}` → `ValidateConfig` → 启动内嵌 NATS → 等待 NATS ready → 启动可选 Auth Callout → `StartManager`。即使没有 `p2p {}` 或该块为空，也会用默认配置启动 V2 Coordinator。

## 2. 身份、注册与权限

### 2.1 节点身份

- Agent 的 NATS CONNECT `name` 必须等于自己的 `node_key`。
- 服务端接受的 `node_key` 格式是 `^[A-Za-z0-9_-]{1,128}$`。raypx2 可继续生成并持久化 16 位小写 hex；重启后应复用。
- `node_key` 与 Center enrollment 身份互不替代。发起会话时填写对端的 NATS `node_key`。
- Coordinator 通过 connz 验证当前接入节点上恰好存在一条同名 Agent 连接。名称不匹配或存在多条同名连接时，注册失败。

### 2.2 注册标识

每次连接或重连都生成新的 `registration_id`：32 个小写 hex 字符。它既进入 REGISTER envelope，也组成本次注册专属的事件主题：

```text
$P2P.V2.EVENT.<node-key>.<registration-id>
```

Agent 应先准备该主题的订阅，再向当前连接所在 NATS 节点发送 REGISTER。旧连接对应的事件不能当作当前注册的事件处理。

REGISTER 主题最后一段是 CONNECT 时 NATS `INFO.server_id` 给出的 server public nkey：

```text
$P2P.V2.CMD.<node-key>.REGISTER.<server-id>
```

该订阅是 conn-local，不属于 Coordinator queue group，确保注册落到真正持有 Agent TCP 连接的节点。

### 2.3 Agent 权限

Auth Callout 按 CONNECT `name` 签发节点级权限：

| 方向 | 允许范围 |
| --- | --- |
| publish | `$P2P.V2.CMD.<自己的 node-key>.>` |
| subscribe | `$P2P.V2.EVENT.<自己的 node-key>.>` |
| request reply | `_INBOX.>` |

Agent 不能访问 `$P2P.V2.MGR.>`、其他节点的 CMD/EVENT 或 `$SYS.>`。应用实现不得依赖超出上表的权限。

## 3. V2 线协议

### 3.1 Envelope 与标识符

所有命令、回复和事件均为 `version: 2` 的 JSON envelope：

```json
{
  "version": 2,
  "request_id": "11111111-1111-4111-8111-111111111111",
  "message_id": "22222222-2222-4222-8222-222222222222",
  "registration_id": "0123456789abcdef0123456789abcdef",
  "payload": {}
}
```

字段按帧类型选用：

| 字段 | 用途 |
| --- | --- |
| `request_id` | 命令与其回复的关联键；必须是小写 UUID |
| `message_id` | Coordinator 事件或 SIGNAL 消息的去重键；必须是小写 UUID |
| `registration_id` | REGISTER 和定向事件的注册代次；32 个小写 hex |
| `payload` | 必须是 JSON object；内容由命令或事件类型决定 |

解码为 strict 模式：未知字段、错误版本、尾随 JSON、非 object payload 或格式错误的标识符都会得到 `invalid_request`。最大 envelope 为 256 KiB；description 的 SDP 最大 128 KiB；单条 candidate 最大 8 KiB。

### 3.2 Subject

| Subject | 方式 | 说明 |
| --- | --- | --- |
| `$P2P.V2.CMD.<node>.REGISTER.<server-id>` | REQ | 在当前接入节点注册 |
| `$P2P.V2.CMD.<node>.SESSION.CREATE` | REQ | 分配 `conn-0` 和会话 owner |
| `$P2P.V2.CMD.<node>.SESSION.COMMAND` | REQ | BIND、RESUME、CLOSE |
| `$P2P.V2.CMD.<node>.CONNECTION.COMMAND` | REQ | OPEN、RESTART、BIND、READY、REJECT、CLOSE |
| `$P2P.V2.CMD.<node>.SIGNAL.SEND` | REQ | 发送可靠 ICE 信令 |
| `$P2P.V2.CMD.<node>.SIGNAL.ACK` | REQ | 累计确认对端信令 |
| `$P2P.V2.EVENT.<node>.<registration-id>` | SUB | 接收 PREPARE、START、CLOSE、ERROR 和 SIGNAL |

除 REGISTER 外，外部命令由 queue group `$P2P.V2.COORDINATORS` 消费。命令落到非 owner 时，Coordinator 会转发给 owner；Agent 无需选择 owner 节点。

### 3.3 REGISTER

Agent 生成 request UUID 与本次 registration ID，向当前 `server-id` 请求：

```json
{
  "version": 2,
  "request_id": "11111111-1111-4111-8111-111111111111",
  "registration_id": "0123456789abcdef0123456789abcdef",
  "payload": {}
}
```

成功回复沿用 request 和 registration ID，并返回递增的注册 epoch：

```json
{
  "version": 2,
  "request_id": "11111111-1111-4111-8111-111111111111",
  "registration_id": "0123456789abcdef0123456789abcdef",
  "payload": {"registration_epoch": 1}
}
```

相同 `(node_key, request_id)` 的重试幂等。重连必须创建新的 `registration_id`、重新订阅 EVENT 并重新 REGISTER；不能假设进程重启或连接切换后注册仍然有效。

### 3.4 错误回复

错误仍使用统一 envelope，`payload.error` 为机器可读代码：

```json
{
  "version": 2,
  "request_id": "11111111-1111-4111-8111-111111111111",
  "payload": {"error": "busy", "retry_after_ms": 100}
}
```

请求无法提供合法 UUID 时，服务端会用保留 request ID 返回错误。客户端应按自己的原请求上下文处理，不能把保留 ID 当作新的幂等键。

## 4. 会话与连接生命周期

### 4.1 正常建链

```text
双方 REGISTER
  → client SESSION.CREATE
  → client 收 ALLOCATED（conn-0, epoch=1）
  → client SESSION.COMMAND BIND
  → 双方收 PREPARE
  → 双方完成本地 ICE 准备并各发 CONNECTION.COMMAND READY
  → 双方收 START
  → active 状态下交换 SIGNAL.SEND / SIGNAL.ACK
```

创建会话：

```json
{
  "version": 2,
  "request_id": "22222222-2222-4222-8222-222222222222",
  "payload": {
    "server_node_key": "fedcba9876543210",
    "total_timeout_ms": 10000
  }
}
```

`total_timeout_ms` 可省略，默认 10000；显式值必须大于 0。对端必须处于已注册状态且不能是自己。

ALLOCATED 回复：

```json
{
  "version": 2,
  "request_id": "22222222-2222-4222-8222-222222222222",
  "payload": {
    "ok": true,
    "session_id": "33333333-3333-4333-8333-333333333333",
    "connection_id": "conn-0",
    "epoch": 1,
    "state": "allocated",
    "revision": 1,
    "owner_server_id": "<server-public-nkey>"
  }
}
```

随后 client 对 `SESSION.COMMAND` 发 BIND：

```json
{
  "version": 2,
  "request_id": "44444444-4444-4444-8444-444444444444",
  "payload": {
    "command": "BIND",
    "session_id": "33333333-3333-4333-8333-333333333333",
    "connection_id": "conn-0",
    "epoch": 1,
    "revision": 1
  }
}
```

双方 EVENT 主题收到 PREPARE。以下为 client 侧示例；`role` 在另一端为 `server`，`peer_node_key` 相反：

```json
{
  "version": 2,
  "message_id": "55555555-5555-4555-8555-555555555555",
  "registration_id": "0123456789abcdef0123456789abcdef",
  "payload": {
    "session_id": "33333333-3333-4333-8333-333333333333",
    "connection_id": "conn-0",
    "epoch": 1,
    "revision": 2,
    "role": "client",
    "peer_node_key": "fedcba9876543210",
    "sender_node_key": "coordinator",
    "stun_urls": ["stun:turn.example.com:3478"],
    "setup_deadline_ms": 10000,
    "protocol_version": 2,
    "feature_bits": 1
  }
}
```

启用 TURN 时 PREPARE 的 payload 还包含 `turn`：

```json
{
  "turn": {
    "urls": ["turn:turn.example.com:3478?transport=udp"],
    "username": "<短时用户名>",
    "password": "<短时密码>",
    "expires_at": 1700000000
  }
}
```

双方准备好后，各自对 `CONNECTION.COMMAND` 发 READY，并携带最近看到的 revision：

```json
{
  "version": 2,
  "request_id": "66666666-6666-4666-8666-666666666666",
  "payload": {
    "command": "READY",
    "session_id": "33333333-3333-4333-8333-333333333333",
    "connection_id": "conn-0",
    "epoch": 1,
    "revision": 2
  }
}
```

两端都 READY 后收到 START：

```json
{
  "version": 2,
  "message_id": "77777777-7777-4777-8777-777777777777",
  "registration_id": "0123456789abcdef0123456789abcdef",
  "payload": {
    "session_id": "33333333-3333-4333-8333-333333333333",
    "connection_id": "conn-0",
    "epoch": 1,
    "revision": 4,
    "sender_node_key": "coordinator"
  }
}
```

EVENT 共用一个 subject，envelope 不带独立的 `kind` 字段。客户端按 payload 形状和当前状态识别事件：PREPARE 含 `role`，START 不含 `role/state/error/type`，CLOSE 含 `state: closed`，ERROR 含 `error`，SIGNAL 含 `type` 与 `seq`。

### 4.2 状态与命令

协议定义的会话状态为 `allocated`、`preparing`、`active`、`draining`、`closed`、`failed`。当前 Store 的主路径是 `allocated → preparing → active`，随后进入 `closed`；协商超时或协商期 owner 丢失进入 `failed`。`draining` 已列入协议枚举，但当前 Store 尚未写入该状态。

协议定义的连接状态为 `preparing`、`active`、`restarting`、`closed`，Store 另外把新分配连接表示为 `allocated`。当前 OPEN/RESTART 的分配回复均为 `allocated`，BIND 后转为 `preparing`，双方 READY 后转为 `active`；`restarting` 可被 BIND/REJECT 接受，但当前 RESTART 实现不会直接写入该状态。

| 范围 | 命令 | 作用 |
| --- | --- | --- |
| session | `BIND` | 绑定初始 `conn-0`，向两端发 PREPARE |
| session | `RESUME` | 按当前 session/connection/epoch 恢复协商状态 |
| session | `CLOSE` | 关闭整个会话并通知两端 |
| connection | `OPEN` | 分配新 `conn-N`，每会话最多 128 条连接 |
| connection | `RESTART` | 为指定连接分配下一 epoch，旧代信令失效 |
| connection | `BIND` | 绑定 OPEN/RESTART 后的连接并触发 PREPARE |
| connection | `READY` | 标记本端准备完成；两端均完成后触发 START |
| connection | `REJECT` | 拒绝当前连接代并通知两端关闭 |
| connection | `CLOSE` | 关闭指定连接并通知两端 |

`connection_id` 格式为 `conn-(0|[1-9][0-9]{0,9})`。OPEN 可省略 ID 让 Coordinator 分配；其他需要具体连接的命令必须带合法 ID。RESTART 必须指定连接，成功 ALLOCATED 回复中的 epoch 比上一代大。

SESSION/CONNECTION 命令格式允许携带 `revision`。客户端应缓存 ALLOCATED/PREPARE/START/CLOSE/ERROR 中最新 revision，并在后续状态命令中回传。当前 Store 会在 CONNECTION BIND 的非零 revision 与 session 当前 revision 不一致时返回 `stale_revision`；其他命令仍依靠 session/connection/epoch 与状态检查，不能把 revision 当作通用 CAS 成功保证。

关闭后的请求不会重新创建状态。Coordinator 在 tombstone 期内返回 closed 快照，使相同请求和迟到请求保持幂等。

## 5. 信令可靠性

SIGNAL 只允许在连接 `active` 后发送。每个方向按 `(session_id, connection_id, epoch, sender_node_key)` 独立维护序列。

发送 description：

```json
{
  "version": 2,
  "request_id": "88888888-8888-4888-8888-888888888888",
  "message_id": "99999999-9999-4999-8999-999999999999",
  "payload": {
    "session_id": "33333333-3333-4333-8333-333333333333",
    "connection_id": "conn-0",
    "epoch": 1,
    "seq": 1,
    "type": "description",
    "payload": {"sdp": "v=0..."}
  }
}
```

成功接收仅表示 Coordinator 已进入投递流程：

```json
{
  "version": 2,
  "request_id": "88888888-8888-4888-8888-888888888888",
  "payload": {"ok": true, "accepted": true}
}
```

对端 EVENT 收到的 SIGNAL 会补充真实发送者并沿用 message ID：

```json
{
  "version": 2,
  "message_id": "99999999-9999-4999-8999-999999999999",
  "registration_id": "fedcba9876543210fedcba9876543210",
  "payload": {
    "session_id": "33333333-3333-4333-8333-333333333333",
    "connection_id": "conn-0",
    "epoch": 1,
    "seq": 1,
    "type": "description",
    "payload": {"sdp": "v=0..."},
    "sender_node_key": "0123456789abcdef"
  }
}
```

| type | payload 约束 |
| --- | --- |
| `description` | 只允许 `sdp`，最大 128 KiB |
| `candidate` | 只允许 `candidate`，最大 8 KiB |
| `end_of_candidates` | 必须为空 object |

收到并处理信令后，对端发送累计 ACK：

```json
{
  "version": 2,
  "request_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  "payload": {
    "session_id": "33333333-3333-4333-8333-333333333333",
    "connection_id": "conn-0",
    "epoch": 1,
    "ack_seq": 1
  }
}
```

可靠性规则：

- 每个方向从 `seq=1` 开始严格递增；跳号或 ACK 超过已接收序号返回 `sequence_gap`。
- `ack_seq` 是累计确认，确认该方向不大于该值的所有消息。
- Coordinator 在 ACK 前按 100 ms 到 1 s 的退避区间重投，直到该连接代的 setup deadline。
- 相同 `(sender_node_key, request_id)` 的请求幂等；相同方向的 `message_id` 去重。重试必须复用原 request/message ID 和原 seq。
- RESTART、CLOSE、REJECT 或 setup 超时会清除对应连接代的待重试信令。

## 6. 集群与故障语义

### 6.1 Coordinator 协作

- REGISTER 在接入节点处理，并把注册的 pending/final 变更同步给集群同伴。
- SESSION.CREATE 与后续命令通过 `$P2P.V2.COORDINATORS` queue group 分流；创建节点成为 session owner。
- owner 把 session、connection、request 幂等记录和未确认事件复制为 replica 状态。
- 非 owner 收到会话命令时，通过 `$P2P.V2.MGR.<owner-server-id>.COMMAND` 转发；owner 不可达时返回 `coordinator_unavailable`，不会自动接管。
- 节点启动时先通过 `$P2P.V2.MGR.SNAPSHOT` / `SYNC` 追赶已有状态，完成稳定化后才加入外部命令 queue。

Agent 不应访问任何 `$P2P.V2.MGR.>` 主题。`$P2P.MGR.BEAT` 用于 Manager 存活跟踪；其他 `$P2P.MGR.*` 主题也仅属于内部 peer tracking。

### 6.2 超时与离线

| 情况 | 当前行为 |
| --- | --- |
| setup 超过 `total_timeout_ms` | 双方收到 `ERROR setup_timeout`，不泄露 TURN 密码 |
| 命令队列饱和或同步超时 | `busy`，附 100–1000 范围内的 `retry_after_ms` |
| owner 不可达 | `coordinator_unavailable`，不接管 owner |
| Agent 断连 | 保留注册一个 grace 窗口，重连 REGISTER 可恢复 |
| owner 离开 | 协商中的会话收到 `ERROR setup_timeout` 并进入 failed；active 会话不被强制 CLOSE |
| wrapper 重启 | 内存状态丢失，Agent 必须重连并重新 REGISTER/建会话 |

默认客户端 ping 为 `5s`，`ping_max=3`。当前 Manager 用这组产品默认值初始化注册断连 grace，因此固定为 20 秒；显式修改 NATS ping 配置不会同步改变该 Store 参数。当前 tombstone 也按默认 10 秒 setup timeout 初始化并固定为 60 秒，单个会话的 `total_timeout_ms` 不会延长 tombstone。

### 6.3 错误码

| code | 含义 |
| --- | --- |
| `invalid_request` | JSON、版本、标识符、字段、命令或 payload 非法 |
| `not_registered` | 发送者尚未注册或注册已过期 |
| `peer_not_registered` | SESSION.CREATE 的对端未注册 |
| `not_session_member` | 发送者不是该会话成员 |
| `session_not_found` | 会话不存在且无可用 tombstone |
| `connection_not_found` | 连接不存在 |
| `connection_limit` | 单会话达到 128 条连接；回复带 `limit` |
| `stale_epoch` | 请求 epoch 落后 |
| `future_epoch` | 请求 epoch 超前 |
| `sequence_gap` | SIGNAL/ACK 序号不连续或越界 |
| `stale_revision` | 状态命令使用了旧 revision |
| `invalid_state` | 当前 session/connection 状态不允许该操作 |
| `setup_timeout` | 协商超过 deadline |
| `busy` | 队列或集群同步暂不可用；按 `retry_after_ms` 重试 |
| `coordinator_unavailable` | 已知 owner 当前不可达 |
| `internal_error` | 编码或内部处理失败 |

## 7. Auth Callout

包装进程使用标准 `authorization { auth_callout { ... } }`：内核把认证请求送到 `$SYS.REQ.USER.AUTH`；同进程 Callout 校验 Agent 根据本条连接 INFO 中 `sharedkey` 派生的一次性十六进制用户名/密码、以及 CONNECT `name`，成功后签发 `$G` account 的短期 user JWT，只包含该节点的 V2 CMD/EVENT 与 `_INBOX.>` 权限。

Agent 不能复用固定的用户名/密码。建立 TCP 连接后，客户端应先读取服务端初始 `INFO` 的 `sharedkey`（8 位十六进制），生成一个大于 `1000000` 的 32 位十六进制 `user`，并将 `password` 生成为 `uint64(user) * uint64(sharedkey)` 的十六进制值，再用这组凭据发送 CONNECT。值可以省略前导零，但应使用小写十六进制；每次连接都必须重新从该连接的 INFO 派生。

未配置 Auth Callout 时，NATS 退回配置中的静态认证。公网或生产部署应使用 Callout。

内部账号注意事项：

- `auth-internal` 用于 Callout；`p2p-internal` 用于 Manager；可选 `sys` 用于系统事件与 STATSZ。
- 内部账号必须同时列入 `authorization.users` 和 `auth_users`，Agent 账号不能放入 `auth_users`。
- 不要给 `auth-internal` 或 `p2p-internal` 增加 permissions。Callout 响应使用 `$SYS._INBOX.*`，错误收窄会让所有认证超时。
- `auth_callout.issuer` 必须等于 `p2p.IssuerPublic()`；配置只放 account public nkey，不放 seed。
- Agent 派生凭据、issuer seed 与 TURN secret 不得复制到文档、配置模板或日志；`sharedkey` 及其派生值只在当前连接建立期间保存在内存中。

## 8. 配置与启动

### 8.1 启动命令

```bash
nats-p2p-server -c conf/nats-p2p-standalone.conf

# 双节点分别启动
nats-p2p-server -c conf/nats-p2p-dual-a.conf
nats-p2p-server -c conf/nats-p2p-dual-b.conf
```

| 文件 | 场景 |
| --- | --- |
| `conf/nats-p2p-standalone.conf` | 单机，无 route |
| `conf/nats-p2p-dual-a.conf` | 双节点 A |
| `conf/nats-p2p-dual-b.conf` | 双节点 B |

### 8.2 NATS 与 P2P 配置

顶层 `p2p {}` 由包装进程剥离，不会传给 `server.ProcessConfigFile`：

```text
host: 0.0.0.0
port: 4222
server_name: nats-p2p
max_payload: 2MB
# ping_interval / ping_max 省略时使用 5s / 3

authorization {
  timeout: 2s
  users: [
    { user: auth-internal, password: "<internal-auth-password>" }
    { user: p2p-internal, password: "<internal-manager-password>" }
  ]
  auth_callout {
    issuer: "<p2p.IssuerPublic() 返回的 account public nkey>"
    auth_users: [ auth-internal, p2p-internal ]
  }
}

p2p {
  agent_username: "p2p-internal"
  agent_password: "<与 authorization.users 一致>"
  # sys_username: "sys"
  # sys_password: "<internal-system-password>"
  # agent_account: "$G"
  # stun_urls: ["stun:turn.example.com:3478"]
  # turn_urls: ["turn:turn.example.com:3478?transport=udp"]
  # secret_file: "/etc/nats/coturn-rest-secret"
  # credential_ttl: 24h
}
```

配置规则：

- `stun_urls` 必须使用 `stun:`；`turn_urls` 必须使用 `turn:` 且 query 含 `transport=udp`。
- STUN/TURN host 不能是 `localhost` 或 loopback IP；非法 URL 使包装进程启动失败。
- `secret_file` 不可读时 Manager 仍可启动，但 PREPARE 不带 TURN；没有 TURN URL 时也不签凭据。
- TURN REST 凭据默认 TTL 为 24 小时；用户名包含 expiry/session/connection/epoch，密码为 HMAC-SHA1。长期 secret 只留在 Manager 与 coturn。
- 配置 `sys_username/sys_password` 时，还要设置 NATS `system_account`，并让同一 sys 用户绕过 Callout。它用于 `$SYS.ACCOUNT.*.DISCONNECT` 和 V2 STATSZ。
- 集群各节点的 `cluster.name`、内部认证、issuer、P2P/STUN/TURN 配置应一致；`server_name` 必须各不相同，route 指向对端 cluster 端口。

### 8.3 部署端口与防火墙

以下端口是默认值；变更 NATS 或 coturn 配置后，应以实际监听配置和公网映射为准。防火墙规则应限制来源地址，并同时放通相应的出站流量。

| 组件/用途 | 默认端口 | 协议 | 放通范围与说明 |
| --- | ---: | --- | --- |
| NATS 客户端连接（Agent、raypx2） | 4222 | TCP | Agent 所在网络到每个 `nats-p2p-server` 节点；这是控制面和信令连接。 |
| NATS cluster route（仅多节点） | 6222 | TCP | 仅各 NATS 节点之间互通；不要向公网或 Agent 开放。 |
| NATS monitoring（可选） | 8222 | TCP | 仅监控/运维网段；配置 `http_port` 后才监听，文档协议不依赖此端口。 |
| coturn STUN/TURN | 3478 | UDP | Agent 到 coturn；启用 `stun_urls`/`turn_urls` 时必须可达。 |
| coturn relay 端口范围 | 由 coturn 配置决定 | UDP（通常） | Agent 到 coturn 中继地址必须放通整个 `min-port`–`max-port` 范围；不要只开放 3478。 |

coturn 是独立服务：nats-p2p-server 只生成短时 TURN REST 凭据并把 URL 下发给 Agent，不代理媒体或 QUIC 数据。coturn 的 REST secret 必须与 `p2p.secret_file` 内容一致；Agent 建立 P2P 数据面时直接访问 coturn。单机部署至少需要 4222，以及启用 TURN 时的 3478/UDP 和 coturn relay 范围；双节点部署还需要节点间 6222。

### 8.4 raypx2 侧

raypx2 配置字段名以 raypx2 当前实现为准，语义必须满足：

- 持久化自己的 `node_key`，CONNECT `name` 使用同一值。
- 从 NATS INFO 保存当前 `server_id`，用于 conn-local REGISTER subject。
- 每次连接生成新的 registration ID，先订阅对应 EVENT，再 REGISTER。
- client 创建会话时使用 server Agent 已持久化并发布的 `node_key`。
- 断线重连后重新走 REGISTER；不能复用旧 EVENT subject。
- `node_key` 持久化保存；Agent 的 NATS 凭据按每条连接的 INFO `sharedkey` 动态派生，不写入配置文件或日志。

建议启动顺序：`nats-p2p-server` → server Agent 注册 → client Agent 注册并创建会话。只有双方注册完成后，SESSION.CREATE 才会成功。

## 9. 监控与排障

配置 system account 后，可用 system 用户 request：

```text
$SYS.REQ.SERVER.<server-id>.P2P.V2.STATSZ
```

返回聚合计数与 gauge，不包含 node/session 标识或 secret：

| 字段 | 含义 |
| --- | --- |
| `server_id` | 当前 NATS server public ID |
| `p2p_sessions` | 按 `state`、`role` 聚合的 session 数 |
| `p2p_connections` | 按 `state`、`role` 聚合的 connection 数 |
| `p2p_session_create_total` | 按 `ok/busy/error` 聚合的创建次数 |
| `p2p_signal_total` | 按信令 `type/direction/result` 聚合 |
| `p2p_signal_retry_total` | 进入重试队列的信令数 |
| `p2p_signal_deduplicated_total` | 去重的信令数 |
| `p2p_connection_limit_total` | 触发连接上限次数 |
| `p2p_command_queue_depth` | 当前命令队列深度 |
| `p2p_owner_sessions` | 本节点 owner session 数 |
| `p2p_replica_sessions` | 本节点 replica session 数 |
| `p2p_revision_sync_total` | 按 `ok/stale` 聚合的修订同步次数 |

排障顺序：

1. CONNECT 失败先检查 Auth Callout issuer、内部账号权限和 CONNECT `name`。
2. REGISTER 超时先核对 INFO `server_id` 与 subject，确认 EVENT 已在正确 registration ID 上订阅。
3. `busy` 按 `retry_after_ms` 原 ID 重试，同时看 command queue 和 revision sync 指标。
4. `stale_revision` 更新本地 revision 后生成新 request ID 重发状态命令。
5. `sequence_gap` 对照当前 connection epoch 和每方向 seq；不要跨 epoch 延续序号。
6. 没有 TURN 时检查 URL、secret_file 可读性和 Coordinator 日志，但不要输出 secret 或短时密码。

## 10. raypx2 接入清单

- [ ] CONNECT `name` 与持久化 `node_key` 完全一致。
- [ ] 从本条 TCP 连接初始 INFO 读取 `sharedkey`，按规则派生十六进制 user/password；不要复用其他连接的凭据。
- [ ] 从 NATS INFO 读取本次接入节点的 public `server_id`。
- [ ] 每次连接生成 32 位小写 hex `registration_id`，先订 EVENT 再 REGISTER。
- [ ] 所有 request/message ID 使用小写 UUID；重试复用同一 ID 和原 payload。
- [ ] REGISTER 成功后保存 `registration_epoch`，忽略旧注册主题上的事件。
- [ ] client 等待双方注册后发 SESSION.CREATE，并保存 ALLOCATED 的 session、connection、epoch、revision。
- [ ] 按 BIND → PREPARE → READY → START 驱动协商，不在 active 前发送 SIGNAL。
- [ ] 始终携带当前 epoch 和最新 revision；不要把 revision 当作所有命令都执行 CAS；RESTART 后清空旧代信令和序号状态。
- [ ] SIGNAL 每方向从 1 连续递增，按 message ID 去重，处理后发送累计 `ack_seq`。
- [ ] `busy` 遵守 `retry_after_ms`；`coordinator_unavailable` 不假设其他节点已接管。
- [ ] CLOSE/ERROR/setup timeout 后停止该连接代的信令发送。
- [ ] 重连后重新 REGISTER；wrapper 重启后重新建会话。
- [ ] 不访问 `$P2P.V2.MGR.>` 或其他节点的 CMD/EVENT。
- [ ] 不记录 Agent 密码、JWT、TURN secret 或 PREPARE 中的短时 TURN 密码。

## 11. 代码索引

| 路径 | 职责 |
| --- | --- |
| `cmd/nats-p2p-server/main.go` | 包装进程启动顺序 |
| `p2p/protocol_v2.go` | V2 subject、envelope、校验、状态和错误码 |
| `p2p/store_v2.go` | session/connection 状态机、幂等、过期与 tombstone |
| `p2p/signal_v2.go` | SIGNAL 序列、去重、累计 ACK |
| `p2p/manager_v2.go` | 命令处理、事件发布、重试、owner 转发 |
| `p2p/cluster.go` | Manager 存活、同步、快照和 owner-loss |
| `p2p/monitor_v2.go` | V2 STATSZ 指标 |
| `p2p/auth_callout.go` | 节点级 Auth Callout JWT 权限 |
| `p2p/config.go` / `p2p/wrapconfig.go` | ICE URL 校验、配置块与 ping 默认值 |
| `p2p/turn.go` | coturn REST 短时凭据 |
| `conf/nats-p2p-*.conf` | 单机与双节点参考配置 |
