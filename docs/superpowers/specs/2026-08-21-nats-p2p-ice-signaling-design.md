# nats-server P2P ICE 信令设计

日期：2026-08-21  
状态：待实现  
仓库：`nats-server`

## 1. 背景与目标

在 `nats-server` 上增加可选的 `$P2P` 服务，让 raypx2 Agent（以及其它 NATS 客户端）用现有 NATS 协议交换 ICE 信令，并由服务端签发 coturn 短时凭据。

成功标准：

- Agent 集成 NATS 客户端库，连上 `nats-server` 后即可做 ICE **协商**（SDP / candidate）。
- ICE / QUIC **数据面**仍走 UDP（直连、STUN 或 coturn），不进 NATS。
- 单机与同一 cluster 内部署都可用；占用表经 cluster route 同步，不引入数据库，不要求 JetStream。
- coturn 长期 `static-auth-secret` 只留在 `nats-server` 与 coturn；Agent 只拿短时 username/password。

非目标（第一期不做）：

- HMAC `grant`（不再做 Center enroll；能通过 NATS 认证并连上即视为合法客户端）。
- 启动或管理 coturn 进程。
- 跨 Gateway / Leafnode 的占用表。
- `$P2P.TURN.REFRESH`；票过期后重新 `CREATE`。
- 解析或成员校验 ICE 帧；知道 `session_id` 的已认证客户端均可 PUB/SUB `$P2P.ICE.<session_id>`。

参考实现：`~/src/pocketbase/apps/raypx2-center/internal/p2p` 与 `internal/turncred`（invite 信封、TURN REST），传输面改为 NATS 而不是 Center WebSocket。

## 2. 架构

`p2p {}` 未配置时不订阅 `$P2P.REGISTER` / `$P2P.CREATE` / `$P2P.UNREGISTER`，客户端按普通 NATS 无响应者处理。配置块存在但 STUN/TURN URL 非法时，**进程启动失败**，不带残缺服务启动。

启用后，`server/` 内三个内存模块：

| 模块 | 职责 | 不做什么 |
| --- | --- | --- |
| 占用表 | `node_key → 连接`；先到先得；断开释放；经 cluster route 做两阶段同步 | 不持久化、不验 grant |
| 会话 | `CREATE` 时确认两端已登记，生成 `session_id`，向双方 inbox 发 `invite` | 不转发 QUIC，不在集群复制会话 |
| TURN 签发 | 读本机长期 secret，按 coturn REST 算法算短时票 | 不访问 coturn 网络接口 |

```text
raypx2 client                         raypx2 server
     |                                      |
     |  NATS CONNECT + $P2P.REGISTER        |
     +------------> nats-server <-----------+
     |               |        |             |
     |     $P2P.CREATE / 占用表 / TURN HMAC  |
     |               |        |             |
     |     PUB/SUB $P2P.ICE.<session_id>    |
     |                                      |
     |         STUN / TURN Allocate         |
     +--------------> coturn <--------------+
     |                                      |
     +-------- ICE + QUIC 数据面 -----------+
```

`node_key` 由每个 Agent 本地生成，在 `REGISTER` 时声明。发起 `CREATE` 时的「自己」只认该连接已登记的 `node_key`，请求体不得改写己方身份。

## 3. Subject 与报文

控制面为 request-reply（风格对齐 `$JS.API`）。ICE 为普通 PUB/SUB。

| Subject | 方式 | 用途 |
| --- | --- | --- |
| `$P2P.REGISTER` | REQ | 登记 `node_key` |
| `$P2P.CREATE` | REQ | 已登记的一方发起会话 |
| `$P2P.UNREGISTER` | REQ | 可选；显式释放本连接的占用 |
| `$P2P.NODE.<node_key>` | SUB | 登记后必须订阅；收 `invite` |
| `$P2P.ICE.<session_id>` | PUB/SUB | 双方交换 ICE 帧 |

`node_key` 必须匹配 `^[A-Za-z0-9_-]{1,128}$`。`session_id` 由服务端生成（UUID），同样只含该字符集，可安全嵌入 subject。

### 3.1 REGISTER

请求：

```json
{"node_key":"agent-self-id"}
```

成功：

```json
{"ok":true,"node_key":"agent-self-id","inbox":"$P2P.NODE.agent-self-id"}
```

失败：`{"ok":false,"error":"<code>"}`（见第 6 节）。

同一连接对同一 `node_key` 重复 REGISTER 幂等成功。同一连接登记另一个 `node_key` 失败（`already_registered`）。

### 3.2 CREATE

请求：

```json
{"peer_node_key":"server-id","connection_id":"conn-0"}
```

`connection_id` 省略时为 `conn-0`。发起方 `node_key` 来自本连接的登记。对端必须已登记且不是自己。

成功时同时：

1. 回复发起方：session、STUN、短时 TURN（若可用）、`ice_subject`。
2. 向双方 `$P2P.NODE.<node_key>` 发布一帧 `invite`。对端只靠 inbox 上的 `invite` 开工。

成功回复字段：

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

TURN 未启用时省略 `turn`，仍返回 `stun_urls`。

### 3.3 invite 信封

对齐 raypx2 / pocketbase 信封，**没有 `grant`**：

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
    "turn": {
      "urls": ["turn:turn.example.com:3478?transport=udp"],
      "username": "<expiry>:<session>_<conn>_<epoch>",
      "password": "<hmac>",
      "expires_at": 1700000000
    },
    "ice_subject": "$P2P.ICE.<session_id>"
  }
}
```

`payload.turn` 在 TURN 未启用时整段省略。

### 3.4 ICE 帧

双方订阅 `ice_subject`，发布现有类型：`description`、`candidate`、`end_of_candidates`、`p2p_ack`、`p2p_resume`、`p2p_resume_miss`。服务端不解析、不按成员转发；靠 NATS 兴趣路由（含集群）。

## 4. 占用表与集群

占用按 **NATS account** 隔离：不同账号可用相同 `node_key` 字符串。表仅内存，不落盘。节点重启后 Agent 必须重新 REGISTER。

记录：

`node_key → {server_id, cid, inbox, claim_id, claimed_at}`

第一期占用只在同一 cluster 的 **route** 内有效，不跨 Gateway / Leafnode。

### 4.1 REGISTER 两阶段

在 Agent 当前连接的那台服务器上：

1. 本连接已登记同一 key → 成功（幂等）。
2. 本连接已登记其它 key → `already_registered`。
3. 本机表已被其它连接占用 → `node_key_in_use`。
4. **PREPARE**：经 cluster route 询问当前所有对等节点。
5. 任一对等已占用或未在超时内收齐应答 → `node_key_in_use`。PREPARE 超时为 1 秒。
6. 两路 PREPARE 并发 → 比较 `(claimed_at, server_id)`，字典序较小者获胜，另一方视为后到。`claimed_at` 为该节点的 Unix 纳秒（本地时钟）；`server_id` 为 NATS 节点 ID。
7. **COMMIT** 后写入本机表并绑定该连接，再回复 Agent。

无 route（单机）时跳过 4–6，仅本地加锁。

网络分区愈合后若出现双登记，按同一比较留下一条，另一条 RELEASE；被释放的 Agent 需重新 REGISTER。这是不引入 Raft/JetStream 的明确代价。

### 4.2 释放

| 事件 | 行为 |
| --- | --- |
| 连接断开 | 本机删除记录，经 route 发送 `RELEASE` |
| `$P2P.UNREGISTER` | 同上；必须是登记该 key 的那条连接，否则 `not_registered` |
| 到某对等节点的 route 断开 | 删除所有 `server_id` 为该对等的记录 |
| 本机重启 | 本机表清空；对等因 route 变化清掉本机持有的占用 |

不另做 P2P 心跳；依赖 NATS PING/PONG 与断连。

### 4.3 CREATE 与会话

CREATE 只查占用表：对端必须已登记且不是自己。对端连在另一台节点上即可。会话状态不在集群复制：`invite` 与 ICE subject 走现有兴趣路由。

## 5. TURN

coturn 为独立进程。`nats-server` 不发起 Allocate，不转发媒体。

### 5.1 配置

```
p2p {
  stun_urls: ["stun:turn.example.com:3478"]
  turn_urls: ["turn:turn.example.com:3478?transport=udp"]
  secret_file: "/etc/nats/coturn-rest-secret"
  credential_ttl: 24h
}
```

- 每个 TURN URL 必须包含 `transport=udp`。
- STUN/TURN 主机不得为 `127.0.0.1`、`::1`、`localhost`。
- `secret_file` 内容即 coturn `static-auth-secret`；集群每台 `nats-server` 配置同一份。
- `realm` 只配在 coturn，不发给客户端。
- 配置块存在且 URL 非法：进程启动失败，不带残缺 `$P2P` 运行。
- `secret_file` 缺失或不可读：`$P2P` 仍启用（REGISTER/CREATE 可用），invite 省略 `turn`。

长期 secret 不写入日志、`$SYS` 事件或监控接口。

### 5.2 短时票

与 pocketbase `turncred.IssueREST` / coturn `use-auth-secret` 一致：

- `username` = `{expiry_unix}:{session_id}_{connection_id}_{epoch}`
- `password` = `Base64(HMAC-SHA1(secret, username))`
- `expires_at` = `expiry_unix`
- `credential_ttl` 默认 86400 秒

签发为纯函数，不访问 coturn。第一期无独立刷新接口。

### 5.3 降级

| 情况 | `$P2P` | invite |
| --- | --- | --- |
| secret 可读且 URL 合法 | 全开 | `stun_urls` + `turn` |
| secret 不可用、STUN URL 合法 | REGISTER/CREATE 可用 | 仅 `stun_urls` |
| 配置块存在且 STUN/TURN URL 非法 | 进程启动失败 | — |
| 占用/对端检查失败 | 返回错误码 | 不发 invite |

## 6. 错误码

REGISTER / CREATE / UNREGISTER 失败体：`{"ok":false,"error":"<code>"}`。

| code | 含义 |
| --- | --- |
| `invalid_node_key` | `node_key` 或 `peer_node_key` 非法 |
| `already_registered` | 本连接已登记了另一个 `node_key` |
| `node_key_in_use` | 已被占用，或两阶段 PREPARE 失败/超时 |
| `not_registered` | 未 REGISTER 就 CREATE / UNREGISTER |
| `peer_not_registered` | 对端不在占用表 |
| `peer_is_self` | `peer_node_key` 等于自己 |
| `invalid_request` | JSON 无法解析或缺字段 |

NATS 认证失败、subject 权限拒绝仍走现有 NATS 协议错误，不包这层 JSON。

## 7. 测试

实现放在 `server/p2p*.go`，测试在 `server/p2p_*_test.go`。不测 libjuice、真实 coturn Allocate、QUIC Hello、Gateway/Leaf。TURN 用测试 secret 在测试中复算 HMAC-SHA1。

单机：

1. REGISTER 成功后，同一连接再 REGISTER 同一 key 成功。
2. 第二条连接 REGISTER 同一 key → `node_key_in_use`。
3. 第一条断开后，同一 key 可被新连接登记。
4. 两端均已登记时 CREATE：双方 inbox 收到无 `grant` 的 `invite`，且含 `ice_subject`。
5. 对端未登记 → `peer_not_registered`。
6. 配置了 secret → `turn.username` / `turn.password` 可用同一 secret 复算通过。
7. 无 secret → invite 无 `turn`，仍有 `stun_urls`。
8. 配置了 `p2p {}` 但 TURN URL 为环回地址 → 进程启动失败。

集群（2 或 3 节点）：

9. 在节点 A 登记的 key，在节点 B 再登记 → `node_key_in_use`。
10. A 上持有者断开后，B 可以登记该 key。
11. 两端连不同节点，CREATE 后双方都收到 invite。
12. 两节点同时 REGISTER 同一 key → 恰好一个成功。

## 8. 已定决策

| 决策 | 选择 |
| --- | --- |
| 产品形态 | NATS 作信令总线 + 可选 `$P2P` 服务 |
| 身份 | Agent 自生成 `node_key`；NATS 认证通过即合法 |
| 占用 | 同一 account 内同一 `node_key` 先到先得，后到拒绝 |
| grant | 不签发 |
| TURN | 服务端长期 secret，CREATE 签发短时 REST 票 |
| 集群占用 | route 两阶段，不依赖 JetStream |
| ICE 转发 | 普通 PUB/SUB，服务端不校验成员 |
