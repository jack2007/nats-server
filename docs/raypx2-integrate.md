# raypx2 × nats-p2p 集成说明

本文面向把 raypx2 Agent 接到本仓库 `nats-p2p-server` 的运维与客户端开发，介绍两块新增能力：

1. **ICE 信令**：Agent 只用官方 NATS 协议完成登记、建会话、交换 ICE 帧，并由 P2P 模块签发 coturn 短时凭据。
2. **Auth Callout**：CONNECT 的用户名密码不再查静态 `users` 表，而是由同进程 auth 模块核对并回签仅含 `$P2P` 业务权限的 user JWT。

实现在旁路包 `p2p/` 与包装进程 `cmd/nats-p2p-server`，**不改** `server/` 内核。必须用包装二进制，不要用上游 `nats-server`。

对照规格：

- `docs/superpowers/specs/2026-08-21-nats-p2p-ice-signaling-design.md`
- `docs/superpowers/specs/2026-08-22-nats-p2p-auth-callout-design.md`

Agent 侧行为见 raypx2 的 `docs/nats-ice-signaling_cn.md`。

---

## 1. 架构

```text
包装进程 nats-p2p-server（同一 OS 进程）
  ├─ nats-server 内核（公开 API：NewServer / Start / InProcessConn）
  ├─ auth 模块  ──InProcessConn──► 订 $SYS.REQ.USER.AUTH
  └─ p2p 模块   ──InProcessConn──► 订 $P2P.REGISTER / CREATE / UNREGISTER
           ▲
           │ 明文 NATS（无 TLS）
     raypx2 Agent（CONNECT name = node_key，user/password 走 Callout）
           │
           └──── ICE / QUIC UDP（直连 / STUN / 独立 coturn）────┘
```

| 模块 | 职责 | 不做什么 |
| --- | --- | --- |
| auth | 验 CONNECT，签 `$G` user JWT | 不处理 REGISTER / ICE |
| p2p | 占用表、invite、TURN HMAC | 不验密码、不解析 ICE 帧、不转发媒体 |
| 内核 | 标准 NATS 路由与集群 | 不认识 `$P2P` 业务语义 |

数据面仍走 UDP。NATS 只做信令。coturn 是独立进程；长期 `static-auth-secret` 只留在 P2P 模块与 coturn，Agent 只拿短时 username/password。

启动顺序：

1. 剥掉顶层 `p2p {}`，其余交给 `server.ProcessConfigFile`。
2. 省略 `ping_interval` / `ping_max` 时套上产品默认 `5s` / `3`。
3. `Start` → `ReadyForConnections`。
4. 若配置了 `auth_callout`：校验 issuer 公钥 → 启 auth 订阅。
5. 若存在 `p2p {}`：`StartManager`。
6. 对外就绪以 auth 已订阅为准（有 Callout 时）。

---

## 2. ICE 信令

### 2.1 身份

- Agent 本地生成 **16 个 hex** 的 `node_key`（64-bit CSPRNG），写入配置后复用。
- 服务端接受 `^[A-Za-z0-9_-]{1,128}$`；raypx2 收窄为恰好 16 hex。
- NATS CONNECT `name` **必须等于** `node_key`。
- REGISTER / CREATE / UNREGISTER 请求头必须带 `Nats-P2P-Name: <node_key>`，且与 body 一致，否则 `name_mismatch`。
- 与 Center `enroll_secret_file.node_key` 独立，不必相等。CREATE 对端填对端的 NATS `node_key`（raypx2：`peers[].connectivity.server_node_key`）。

第一期不签发、不校验 HMAC `grant`。能通过 NATS 认证并连上即视为合法信令客户端。

### 2.2 Subject

控制面用 request-reply；ICE 用普通 PUB/SUB。P2P **不解析、不按成员转发** ICE 帧，靠 NATS 兴趣路由（含 cluster）。

`$P2P.REGISTER` / `CREATE` / `UNREGISTER` 在每个 Manager 上是 **conn-local `Subscribe`，不是 queue group**，占用留在持有该 named 连接的节点。

| Subject | 方式 | 谁用 | 用途 |
| --- | --- | --- | --- |
| `$P2P.REGISTER` | REQ | Agent | 登记 `node_key` |
| `$P2P.CREATE` | REQ | 发起方 Agent | 两端都已登记后建会话 |
| `$P2P.UNREGISTER` | REQ | Agent | 可选，显式释放占用 |
| `$P2P.NODE.<node_key>` | SUB | Agent 登记后必须订 | 收 `invite` |
| `$P2P.ICE.<session_id>` | PUB/SUB | 双方 | 交换 ICE 帧 |
| `$P2P.MGR.PREPARE` | REQ | 仅 Manager | 集群占用两阶段 |
| `$P2P.MGR.COMMIT` / `RELEASE` | PUB | 仅 Manager | 确认 / 释放占用 |
| `$P2P.MGR.BEAT` | PUB | 仅 Manager | 实例心跳（1s，TTL 5s） |

Agent JWT **没有** `$P2P.MGR.>` 和 `$SYS.>`。不要订、不要发这些主题。

### 2.3 REGISTER

请求：

```json
{"node_key":"<16-hex>"}
```

成功：

```json
{"ok":true,"node_key":"<16-hex>","inbox":"$P2P.NODE.<16-hex>"}
```

失败：`{"ok":false,"error":"<code>"}`。

同一连接、同一 `node_key` 且占用仍指向该连接时，重复 REGISTER 幂等成功。另一条连接抢同一 key → `node_key_in_use`。raypx2 收到该码后换 key、落盘、重连，最多 5 次。

占用按 **NATS account** 隔离，表在 Manager **内存**中，不落盘。包装进程重启后必须重新 REGISTER。第一期占用只在同一 NATS **cluster** 内有效，不跨 Gateway / Leafnode。

集群 REGISTER 走两阶段：处理节点向 `$P2P.MGR.PREPARE` 询问其它仍活着的实例（看最近 BEAT），任一已占用或 1 秒内未收齐 → `node_key_in_use`。并发 PREPARE 比较 `(claimed_at, server_id)`，较小者赢，再 `COMMIT`。单机只有一个 Manager 时跳过两阶段。

### 2.4 CREATE 与 invite

请求：

```json
{"peer_node_key":"<对端 node_key>","connection_id":"conn-0"}
```

`connection_id` 省略时为 `conn-0`。对端必须已登记且不是自己。

成功时：

1. 回复发起方：`session_id`、STUN、短时 TURN（若可用）、`ice_subject`。
2. 向双方 `$P2P.NODE.<node_key>` 发布同一份 `invite`（**没有 `grant`**）。

成功回复：

```json
{
  "ok": true,
  "session_id": "<uuid>",
  "connection_id": "conn-0",
  "epoch": 1,
  "ice_subject": "$P2P.ICE.<session_id>",
  "stun_urls": ["stun:turn.example.com:3478"],
  "turn": {
    "urls": ["turn:turn.example.com:3478?transport=udp"],
    "username": "<expiry>:<session>_<conn>_<epoch>",
    "password": "<hmac>",
    "expires_at": 1700000000
  }
}
```

TURN 未启用时整段省略 `turn`，仍可返回 `stun_urls`。未配 STUN 时 `stun_urls` 也可省略。

invite 信封（7 字段，对齐 raypx2 `TqP2pSignalFrame`）：

```json
{
  "id": "<uuid>",
  "session_id": "<uuid>",
  "connection_id": "conn-0",
  "epoch": 1,
  "seq": 1,
  "type": "invite",
  "payload": {
    "stun_urls": ["stun:turn.example.com:3478"],
    "turn": { "...": "同上，未启用则整段省略" },
    "ice_subject": "$P2P.ICE.<session_id>"
  }
}
```

会话状态不在集群复制：invite 与 ICE subject 走 NATS 兴趣路由。两端可连不同节点。

没有 `$P2P.TURN.REFRESH`。票过期后重新 CREATE。

### 2.5 ICE 帧

双方订 `ice_subject`，发布 `description` / `candidate` / `end_of_candidates` / `p2p_ack` / `p2p_resume` / `p2p_resume_miss`。P2P 模块不校验成员、不拆帧。提名后 QUIC 仍走 UDP，不经过 NATS。

### 2.6 占用释放与探活

| 事件 | 行为 |
| --- | --- |
| `$SYS.ACCOUNT.*.DISCONNECT` 且 name 匹配占用 | 删本机记录，PUB `$P2P.MGR.RELEASE`（需配置 `sys_*`） |
| `$P2P.UNREGISTER` | 同上；必须是该 name 的当前占用者，否则 `not_registered` |
| 对等 Manager 心跳消失 / 节点离开 | 删所有该 `server_id` 的记录 |
| 本包装进程重启 | 本机表清空；对等因心跳消失清理 |

未配 `sys_username` / `sys_password` 时，断连释放不走 `$SYS` 事件，对端可能在 ping 超时前仍看到旧占用（`node_key_in_use`）。生产建议打开，见 §4.3。

内核会向客户端发协议 `PING`（不是 TCP keepalive）。未应答超过 `ping_max` 后关连接（`Stale Connection`），再发 `$SYS DISCONNECT`。包装进程产品默认：

| 项 | 默认 | 含义 |
| --- | --- | --- |
| `ping_interval` | `5s` | 服务端发 PING 的周期 |
| `ping_max` | `3` | 允许未应答的 PING 数 |

死连接检出最坏约 **20 秒**。配置里显式写出则尊重运维值。`nats.c` 自动回 PONG，Agent 不必另做探活。NATS 断线不拆已提名的 QUIC；新建 ICE 会话要等重连并重新 REGISTER。

### 2.7 错误码

失败体：`{"ok":false,"error":"<code>"}`。

| code | 含义 |
| --- | --- |
| `invalid_node_key` | `node_key` 或 `peer_node_key` 非法 |
| `name_mismatch` | header / body 与 CONNECT `name` 不一致 |
| `already_registered` | 本连接已用另一身份登记（防御性） |
| `node_key_in_use` | 已被占用，或两阶段 PREPARE 失败/超时 |
| `not_registered` | 未 REGISTER 就 CREATE / UNREGISTER |
| `peer_not_registered` | 对端不在占用表 |
| `peer_is_self` | `peer_node_key` 等于自己 |
| `invalid_request` | JSON 无法解析或缺字段 |

NATS 认证失败、subject 权限拒绝仍走 NATS 协议错误，不是上述 JSON。

### 2.8 TURN 短时票

与 coturn `use-auth-secret` / pocketbase `turncred.IssueREST` 一致：

- `username` = `{expiry_unix}:{session_id}_{connection_id}_{epoch}`
- `password` = `Base64(HMAC-SHA1(secret, username))`
- `expires_at` = `expiry_unix`
- `credential_ttl` 默认 `24h`

`realm` 只配在 coturn，不发给客户端。长期 secret 不写日志、不进 `$SYS` 业务事件、不进监控。

| 情况 | `$P2P` 控制面 | invite |
| --- | --- | --- |
| secret 可读且 URL 合法 | 全开 | `stun_urls` + `turn` |
| secret 不可用、STUN URL 合法 | REGISTER/CREATE 可用 | 仅 `stun_urls` |
| `p2p {}` 存在且 URL 非法（环回、缺 `transport=udp` 等） | **包装进程启动失败** | — |
| 占用 / 对端检查失败 | 返回错误码 | 不发 invite |

---

## 3. Auth Callout

### 3.1 行为

NATS Server ≥ 2.10 的官方 Auth Callout：内核把客户端出示的 user/password 发到 `$SYS.REQ.USER.AUTH`，由同进程 `p2p.StartAuthCallout` 判定并回签 user JWT。

- Agent 仍用官方客户端明文 CONNECT，字段仍是 `nats.user` / `nats.password`（raypx2：`natsOptions_SetUserInfo`）。
- 凭据与 `p2p/auth_callout.go` 里的临时常量一致 → 进 `$G`，只能碰约定的 `$P2P` 业务 subject。
- 凭据不一致或为空 → `Authorization Violation`，到不了 `$P2P.REGISTER`。
- 不升级 Agent↔NATS 为 TLS，不开 Callout xkey。
- 集群：Callout 发生在**接入那一台**。每台本地订阅，**不要**给 `$SYS.REQ.USER.AUTH` 加 queue group。各节点同一套代码常量与同一份 conf 公钥。

没有 `auth_callout` 块时不启 auth 协程，退回静态 `users`（实验室兼容）。公网 / 正式部署必须开 Callout。

临时 Agent 用户名密码、issuer 种子在代码常量中，**不要**抄进本文、示例 conf、日志或 `$SYS` 业务事件。客户端从该常量或环境变量 `NATS_P2P_USER` / `NATS_P2P_PASSWORD` 取值。

### 3.2 JWT（成功 CONNECT 后）

有效期 4 小时，断线重连再走 Callout。`sub` 为请求中的一次性 user nkey。

| 项 | v1 取值 |
| --- | --- |
| account / aud | `$G` |
| publish | `$P2P.REGISTER`、`$P2P.CREATE`、`$P2P.UNREGISTER`、`$P2P.ICE.>` |
| subscribe | `$P2P.NODE.>`、`$P2P.ICE.>`、`_INBOX.>` |
| 不给 | `$P2P.MGR.>`、`$SYS.>`、`>` |

`_INBOX.>` 为 REGISTER/CREATE 的 request-reply 所必需，实现不得省略。

### 3.3 Agent 看到的错误

| 情况 | Agent | 服务端 |
| --- | --- | --- |
| 用户名/密码不是代码常量（含空） | `Authorization Violation` | 回失败；不打密码 |
| auth 未 SUB / 超时 | 超时或 `Authorization Violation` | 未就绪不对外报 ready |
| JWT 未覆盖的 subject | 连得上但 pub/sub 被拒 | `$P2P.MGR` / `$SYS` 必须被拒 |
| REGISTER `name` ≠ `node_key` | `name_mismatch` | 与 Callout 无关 |

### 3.4 进程内账号

`auth_users` **只放**进程内客户端，不要放 Agent 账号。开 Callout 后，静态 `users` 里除 `auth_users` 外都会走 Callout。

| 用户 | 用途 |
| --- | --- |
| `auth-internal` | 订 `$SYS.REQ.USER.AUTH`。用户名必须是 `p2p.AuthInternalName` |
| `p2p-internal` | `StartManager` 的 InProcessConn。必须等于 `p2p.agent_username` / `agent_password` |
| `sys`（可选） | 订 `$SYS.ACCOUNT.*.DISCONNECT`；启用时也要进 `users` 与 `auth_users` |

**不要**给 `auth-internal`（以及 `p2p-internal`）加 `permissions`。内核 Callout 回包走 `$SYS._INBOX.*`（`newRespInbox`），不是 Agent JWT 里的 `_INBOX.>`。若只允许 `publish: ["_INBOX.>"]` 或只订 `$SYS.REQ.USER.AUTH`，应答被丢掉，合法凭据也会在 `authorization.timeout` 后变成 `Authorization Violation`。

---

## 4. 配置

包装进程先剥顶层 `p2p {}`，再把剩余交给内核。`p2p {}` 不是 nats-server 官方语法。

### 4.1 启动

```bash
nats-p2p-server -c conf/nats-p2p-standalone.conf
# 双机：
nats-p2p-server -c conf/nats-p2p-dual-a.conf
nats-p2p-server -c conf/nats-p2p-dual-b.conf
```

参考配置：

| 文件 | 场景 |
| --- | --- |
| `conf/nats-p2p-standalone.conf` | 单机，无 cluster / route |
| `conf/nats-p2p-dual-a.conf` | 双机节点 A（示例 `172.16.10.80`，`server_name=spark-1619`） |
| `conf/nats-p2p-dual-b.conf` | 双机节点 B（示例 `172.16.10.81`，`server_name=spark-1b6f`） |

### 4.2 NATS 段（交给 `ProcessConfigFile`）

```text
host: 0.0.0.0
port: 4222
server_name: nats-p2p          # 集群内必须唯一
max_payload: 2MB               # 信令足够；媒体不走 NATS
# ping_interval / ping_max 省略则包装进程套 5s / 3

authorization {
  timeout: 2s                  # auth 未就绪或回包丢失时，客户端等这么久后失败
  users: [
    { user: auth-internal, password: "auth-internal" }
    { user: p2p-internal,  password: "p2p-internal" }
  ]
  auth_callout {
    issuer: "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA"
    auth_users: [ auth-internal, p2p-internal ]
  }
}
```

硬约束：

1. `issuer` 必须等于 `p2p.IssuerPublic()`（由代码内种子导出的 account nkey，`A` 开头）。不一致则 `CheckIssuer` 启动失败。不要把种子（`S` 开头）写进配置。
2. `users` / `auth_users` 只放进程内账号，不要放 Agent。
3. 不要给内部用户加 `permissions`（见 §3.4）。
4. 不要写顶层 `auth {}`。必须是 `authorization { auth_callout { ... } }`。
5. 不要额外加残留 `accounts { APP { ... } }` 却不设 `p2p.agent_account`：Agent JWT 仍进 `$G`，占用表容易看错账号。

双机额外：

```text
cluster {
  name: nats-p2p-dgx           # 两侧必须同名
  listen: 0.0.0.0:6222
  routes: [
    nats-route://<对端IP>:6222 # 对端 cluster 口，不是 4222；不要写成 nats://
  ]
}
```

两侧必须相同：`cluster.name`、issuer、内部用户密码、`auth_users`、`p2p.agent_*`（以及若启用：`sys_*`、STUN/TURN、`secret_file`）、同一套包装二进制常量。

两侧必须不同：`server_name`、`cluster.routes`（指向对端）。

### 4.3 `p2p {}` 段（包装进程解析）

```text
p2p {
  agent_username: "p2p-internal"
  agent_password: "p2p-internal"

  # 可选：断连释放占用（生产建议打开）
  # sys_username: "sys"
  # sys_password: "sys-internal"

  # 可选：STUN / TURN
  # stun_urls: ["stun:turn.example.com:3478"]
  # turn_urls: ["turn:turn.example.com:3478?transport=udp"]
  # secret_file: "/etc/nats/coturn-rest-secret"
  # credential_ttl: 24h
}
```

| 键 | 必填 | 说明 |
| --- | --- | --- |
| `agent_username` / `agent_password` | 开 Callout 时必填 | 必须等于 `p2p-internal` |
| `sys_username` / `sys_password` | 否 | 订 `$SYS` DISCONNECT；集群时 `$P2P.MGR.*` 改走 system account |
| `agent_account` | 否 | Agent 不在默认 `$G` 时填写实际账号名 |
| `stun_urls` | 否 | scheme 必须是 `stun:`；主机不得为 `127.0.0.1` / `::1` / `localhost` |
| `turn_urls` | 否 | scheme 必须是 `turn:`，query 必须含 `transport=udp`；主机同样禁止环回 |
| `secret_file` | 否 | coturn `static-auth-secret`；缺失或不可读时控制面仍启用，invite 省略 `turn` |
| `credential_ttl` | 否 | 短时票 TTL，默认 `24h` |

打开 `sys_*` 时，NATS 段还要同时改：

1. `authorization` 之前增加 `accounts { SYS { users: [ { user: sys, password: "..." } ] } }` 与 `system_account: SYS`。
2. `authorization.users` 增加同一 `sys` 用户。
3. `auth_users` 改为 `[ auth-internal, p2p-internal, sys ]`。

`p2p {}` 存在但 STUN/TURN URL 非法：包装进程启动失败。没有 `p2p {}` 时不订 `$P2P.*`，客户端按无响应者处理。

### 4.4 raypx2 Agent 配置

```json
{
  "nats": {
    "enabled": true,
    "url": "nats://127.0.0.1:4222",
    "node_key": "0123456789abcdef",
    "user": "<与 p2p/auth_callout.go 常量一致>",
    "password": "<同上，或用环境变量覆盖>"
  },
  "peers": [{
    "id": "p",
    "connectivity": {
      "mode": "ice",
      "server_node_key": "fedcba9876543210"
    }
  }]
}
```

规则：

- `nats.enabled=true` 且 `connectivity.mode=ice`：信令只走 NATS。`ice` / `auto` / `server.p2p.enabled` 在 `nats.enabled=false` 时必须失败，**不回退** Center WebSocket ICE。
- `center.enabled` 与 `nats.enabled` 可同时为 true；同时开启时 ICE 仍只走 NATS。
- 省略 `nats.node_key` 时 Agent 生成 16 hex 并写回。
- 开 Callout 时 `nats.user` / `nats.password` 必填。密码不要写进文档、不要打进日志。脚本可用 `NATS_P2P_USER` / `NATS_P2P_PASSWORD` 覆盖。
- CONNECT `name` = `nats.node_key`；REGISTER 后订 `$P2P.NODE.<node_key>`。

建议启动顺序：先 `nats-p2p-server`，再 server Agent（`server.p2p.enabled=true`），再 client Agent（`server_node_key` 填 **server 已落盘的** `nats.node_key`）。Client REGISTER 后 CREATE；server 收 invite；两端在 `$P2P.ICE.<session>` 上交换 ICE。

### 4.5 公网切换

1. 先部署带 auth 模块的 `nats-p2p-server` 和带 `auth_callout` 的 conf。
2. 再改 Agent / 脚本中的 `nats.user` / `nats.password`。顺序反了会集体连不上。
3. 不启停 coturn，不改 TURN secret。切 nats 时信令短中断，已有 ICE 不保证存活。
4. 回滚：旧二进制 + 旧静态 `users` 表，Agent 改回旧账号。

门禁（与现有公网 ICE 对齐）：两端 Agent 连上 Callout 后的 nats-p2p，REGISTER/CREATE 成功，Admin 同一条连接 `connected:true`，HTTP 响应以 `nats-ice-ok` 开头。

---

## 5. 代码位置

| 路径 | 职责 |
| --- | --- |
| `cmd/nats-p2p-server/main.go` | 剥 `p2p {}`、套 ping 默认、先 auth 再 Manager |
| `p2p/auth_callout.go` | 常量、订 `$SYS.REQ.USER.AUTH`、验密、签 JWT |
| `p2p/manager.go` | REGISTER / CREATE / UNREGISTER / DISCONNECT |
| `p2p/protocol.go` | JSON 与错误码、invite 信封 |
| `p2p/cluster.go` | `$P2P.MGR.*` 两阶段与心跳 |
| `p2p/occupancy.go` | 本机占用表 |
| `p2p/turn.go` | HMAC-SHA1 短时票 |
| `p2p/config.go` / `wrapconfig.go` | URL 校验、剥配置块 |
| `conf/nats-p2p-*.conf` | 单机 / 双机参考配置 |
