# nats-server P2P ICE 信令设计

日期：2026-08-21  
状态：待实现  
仓库：`nats-server`（P2P 为旁路包，不改 `server/` 内核）

修订：同日改为 **Go 库嵌入**——包装进程用公开 API 嵌入 `nats-server`，`$P2P` 在独立包中以 NATS 客户端实现。不再把占用表/TURN 做进 `server/*.go`，也不再走 cluster route 私有协议。

同日再修订：包装进程产品默认 `ping_interval=5s`、`ping_max=3`（§4.4），缩短强杀后占用释放窗口；不改 `server/` 内核默认。Agent `node_key` 为 16 hex。

## 1. 背景与目标

让 raypx2 Agent（以及其它 NATS 客户端）连上嵌入式 `nats-server` 后做 ICE **协商**，并由 **P2P 模块**签发 coturn 短时凭据。数据面仍走 UDP（直连 / STUN / coturn）。

成功标准：

- Agent 只使用官方 NATS 客户端协议；不引入第二套信令端口。
- P2P 与 `nats-server` **逻辑分离**：实现在独立 Go 包，只依赖 `server` 的公开 API（`NewServer` / `Start` / `Shutdown` / `InProcessConn` 等）。
- 升 NATS：合并或升级同一模块的 `server` 包并重编包装二进制，**不必 rebase 对 `server/*.go` 的功能补丁**。
- 单机与同一 cluster 可用。占用表在 P2P 进程内内存中；多节点时用 NATS 主题做两阶段，不引入数据库，不要求 JetStream。
- coturn 长期 secret 只留在 P2P 模块与 coturn；Agent 只拿短时 username/password。

非目标（第一期不做）：

- HMAC `grant`。能通过 NATS 认证并连上即视为合法客户端。
- 启动或管理 coturn 进程。
- 修改 nats-server 解析器、route 协议、MQTT、JetStream。
- 独立 sidecar 进程（已否决；同进程嵌入）。
- 跨 Gateway / Leafnode 的占用表。
- `$P2P.TURN.REFRESH`；票过期后重新 `CREATE`。
- 解析或成员校验 ICE 帧。

参考：`~/src/pocketbase/apps/raypx2-center/internal/p2p` 与 `internal/turncred`。传输面为 NATS，而不是 Center WebSocket。

## 2. 架构（Go 库嵌入）

包装二进制（例如 `cmd/nats-p2p-server`）启动顺序：

1. 读取包装配置（NATS `Options` + P2P 段，见 5.1）。
2. `server.NewServer(opts)` → `Start()`，等到 `ReadyForConnections`。
3. P2P 模块用 **进程内连接**（优先 `Server.InProcessConn` + `nats.Connect`）作为客户端连上嵌入式服务器。
4. P2P 订阅 `$P2P.REGISTER` / `CREATE` / `UNREGISTER`（**queue group `p2p`**，集群里只有一个实例应答某个请求）。
5. 各节点 P2P 另订 `$P2P.MGR.>`（**不要** queue group），用于占用表两阶段。

`server/` 内核不增加 `$P2P` 处理、不增加 `p2p {}` 配置语法。包装进程自己解析 P2P 段，把其余项交给 `server.Options`。

未配置 P2P 段时不订阅 `$P2P.*` 控制面，客户端按 NATS 无响应者处理。P2P 段存在但 STUN/TURN URL 非法时，**包装进程启动失败**。

```text
                    包装进程（同一 OS 进程）
         +-------------------------------------------+
         |  p2p 包（占用表 / invite / TURN HMAC）     |
         |       | InProcessConn                      |
         |       v                                    |
         |  github.com/nats-io/nats-server/v2/server  |
         +-------------------------------------------+
                    ^                 ^
                    | NATS 协议        | NATS 协议
             raypx2 client      raypx2 server
                    |                 |
                    +---- ICE/QUIC UDP / coturn ----+
```

| 模块（均在 `p2p/`，不在 `server/`） | 职责 | 不做什么 |
| --- | --- | --- |
| 占用表 | `node_key → 登记`；先到先得；断连释放；经 `$P2P.MGR.*` 两阶段 | 不改 route、不持久化、不验 grant |
| 会话 | `CREATE` 确认两端已登记，发 `invite` | 不转发 QUIC，不复制会话状态 |
| TURN 签发 | 读本模块长期 secret，算短时票 | 不访问 coturn 网络接口 |

`node_key` 由 Agent 本地用 64-bit CSPRNG 生成，编码为 **16 个 hex 字符**，写入 Agent 配置后复用。NATS `CONNECT` 的 `name` **必须等于** `node_key`，以便用 `$SYS` 连接事件把占用绑到真实连接（嵌入模式下订阅方看不到对端 `cid`，除非走系统事件 / CONNZ）。

`CREATE` 的「自己」只认该连接已登记的 `node_key`（且等于 CONNECT name），请求体不得改写己方身份。

## 3. Subject 与报文

控制面 request-reply；ICE 普通 PUB/SUB。Agent 侧报文与前一版相同。

| Subject | 方式 | 用途 |
| --- | --- | --- |
| `$P2P.REGISTER` | REQ（queue group `p2p`） | 登记 `node_key` |
| `$P2P.CREATE` | REQ（queue group `p2p`） | 已登记的一方发起会话 |
| `$P2P.UNREGISTER` | REQ（queue group `p2p`） | 可选；显式释放 |
| `$P2P.NODE.<node_key>` | SUB | Agent 登记后必须订；收 `invite` |
| `$P2P.ICE.<session_id>` | PUB/SUB | 双方交换 ICE 帧 |
| `$P2P.MGR.PREPARE` | REQ（全体 P2P 实例） | 占用两阶段 |
| `$P2P.MGR.COMMIT` / `RELEASE` | PUB 或 REQ | 确认占用 / 释放 |

`node_key` 必须匹配 `^[A-Za-z0-9_-]{1,128}$`。`session_id` 由 P2P 生成（UUID），字符集相同。

### 3.1 REGISTER

请求：

```json
{"node_key":"agent-self-id"}
```

约束：`node_key` 必须等于该客户端 CONNECT `name`，否则 `name_mismatch`。

成功：

```json
{"ok":true,"node_key":"agent-self-id","inbox":"$P2P.NODE.agent-self-id"}
```

失败：`{"ok":false,"error":"<code>"}`。

同一连接（同一 CONNECT name 且占用记录仍指向该连接）重复 REGISTER 幂等成功。

### 3.2 CREATE

请求：

```json
{"peer_node_key":"server-id","connection_id":"conn-0"}
```

`connection_id` 省略时为 `conn-0`。对端必须已登记且不是自己。

成功时：

1. 回复发起方：session、STUN、短时 TURN（若可用）、`ice_subject`。
2. 向双方 `$P2P.NODE.<node_key>` 发布 `invite`。

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

TURN 未启用时省略 `turn`，仍返回 `stun_urls`。

### 3.3 invite 信封

对齐 raypx2 / pocketbase，**没有 `grant`**：

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

双方订 `ice_subject`，发布 `description` / `candidate` / `end_of_candidates` / `p2p_ack` / `p2p_resume` / `p2p_resume_miss`。P2P 模块不解析、不按成员转发；靠嵌入式 NATS 的兴趣路由（含集群）。

## 4. 占用表与集群

占用按 **NATS account** 隔离。表在 **P2P 模块内存**，不落盘。包装进程重启后 Agent 必须重新 REGISTER。

记录：

`node_key → {server_id, conn_name, inbox, claim_id, claimed_at}`

`server_id` 为该 P2P 实例所嵌入的 NATS 节点 ID。`conn_name` 即 CONNECT name（= `node_key`）。

第一期占用只在同一 NATS **cluster** 内有效（各节点都跑同一包装进程），不跨 Gateway / Leafnode。

连接在线与否：P2P 用系统账号订阅 `$SYS.ACCOUNT.*.CONNECT` / `DISCONNECT`（必要时辅以 `$SYS.REQ.SERVER.CONNZ`）。Agent 断开后释放对应占用。不另做 P2P 心跳。

进程被强杀时，占用要等 nats-server 协议 PING 超时、发出 `$SYS DISCONNECT` 后才释放。内核默认 `ping_interval=2m`、`ping_max=2` 太长，同 key 的新进程会在窗口内收到 `node_key_in_use` 并换身份。包装进程必须把客户端探活缩短，见 §4.4。

### 4.1 REGISTER 两阶段

处理该请求的 P2P 实例（queue group 赢家）执行：

1. `node_key` ≠ CONNECT name → `name_mismatch`。
2. 本实例已登记同一 key 且系统事件显示该连接仍在 → 成功（幂等）。
3. 本实例表已被其它连接占用 → `node_key_in_use`。
4. **PREPARE**：向 `$P2P.MGR.PREPARE` 询问本 cluster 内其它 P2P 实例。
5. 任一实例已占用或 1 秒内未收齐已知实例的应答 → `node_key_in_use`。
6. 两路 PREPARE 并发 → 比较 `(claimed_at, server_id)`，较小者赢。`claimed_at` 为 Unix 纳秒；`server_id` 为 NATS 节点 ID。
7. **COMMIT**（`$P2P.MGR.COMMIT`）后写入本机表，再回复 Agent。

单机只有一个 P2P 实例时跳过 4–6，仅本地加锁。

实例发现：每个 P2P 定期在 `$P2P.MGR.BEAT` 发布 `{server_id}`；PREPARE 只等待最近一次心跳仍活着的实例。某实例退出或嵌入式节点离开 cluster 后，其它实例丢掉该 `server_id` 持有的占用（等价于原「route 断开」）。

分区愈合后若双登记，按同一比较留下一条，另一条 RELEASE；被释放的 Agent 需重新 REGISTER。不引入 Raft/JetStream。

### 4.2 释放

| 事件 | 行为 |
| --- | --- |
| `$SYS` DISCONNECT 且 name 匹配占用 | 删本机记录，PUB `$P2P.MGR.RELEASE` |
| `$P2P.UNREGISTER` | 同上；必须是该 name 的当前占用者，否则 `not_registered` |
| 对等 P2P 心跳消失 / 节点离开 | 删所有该 `server_id` 的记录 |
| 本包装进程重启 | 本机表清空；对等因心跳消失清理 |

### 4.3 CREATE 与会话

CREATE 只查占用表（本机 + 已 COMMIT 的副本）。对端连在另一台嵌入节点上即可。会话不在集群复制：`invite` 与 ICE subject 走 NATS 兴趣路由。

### 4.4 客户端探活（协议 PING）

nats-server **会主动**向客户端发协议 `PING`，等 `PONG`。这不是 OS TCP keepalive。未应答次数超过 `ping_max` 后关连接，错误为 `Stale Connection`，随后 `$SYS DISCONNECT` 释放占用。

P2P 包装进程产品默认（**不改** `server/` 内核默认值；在 `ProcessConfigFile` 之后写入 `server.Options`）：

```
ping_interval: "5s"
ping_max: 3
```

| 项 | 值 | 含义 |
| --- | --- | --- |
| `ping_interval` | `5s` | 服务端向客户端发 PING 的周期 |
| `ping_max` | `3` | 允许未应答的 PING 数；再多一个周期则关连接 |

死连接检出最坏约 **20 秒**（发出 3 个 PING 后，下一个 5s 周期关闭）。`nats.c` 自动回 PONG，Agent 不必实现探活。

nats 配置段若**显式**写了 `ping_interval` / `ping_max`，尊重运维值。省略则套上 5s/3，避免沿用内核 2m/2。

缩短 ping 只压缩「旧 TCP 尚未 DISCONNECT」窗口。两份活进程共用同一 `node_key`、或集群 PREPARE 超时，仍回 `node_key_in_use`；换身份由 Agent 处理。

## 5. TURN

coturn 仍为独立进程。P2P 模块不发起 Allocate，不转发媒体。嵌入式 `nats-server` **不持有** coturn secret。

### 5.1 配置

包装进程配置示例（P2P 段由包装器解析，不传给 `server.ProcessConfigFile` 作为未知块——避免改内核 parser。可用独立文件或包装器先剥段再交给 NATS）：

```
# nats 段：标准 nats-server 选项（cluster、authorization 等）
# 省略 ping_* 时包装进程套上 5s/3；也可显式写出：
# ping_interval: "5s"
# ping_max: 3

p2p {
  stun_urls: ["stun:turn.example.com:3478"]
  turn_urls: ["turn:turn.example.com:3478?transport=udp"]
  secret_file: "/etc/nats/coturn-rest-secret"
  credential_ttl: 24h
}
```

- 每个 TURN URL 必须包含 `transport=udp`。
- STUN/TURN 主机不得为 `127.0.0.1`、`::1`、`localhost`。
- `secret_file` 即 coturn `static-auth-secret`；集群每台包装进程配置同一份。
- `realm` 只配在 coturn，不发给客户端。
- P2P 段存在且 URL 非法：包装进程启动失败。
- `secret_file` 缺失或不可读：`$P2P` 控制面仍启用，invite 省略 `turn`。
- nats 段省略 `ping_interval` / `ping_max` 时，包装进程在 `ProcessConfigFile` 之后把 `Options` 设为 `5s` / `3`（见 §4.4）。

长期 secret 不写日志、不经 `$SYS` 业务事件、不进面向 Agent 的报文以外的监控。

### 5.2 短时票

与 pocketbase `turncred.IssueREST` / coturn `use-auth-secret` 一致：

- `username` = `{expiry_unix}:{session_id}_{connection_id}_{epoch}`
- `password` = `Base64(HMAC-SHA1(secret, username))`
- `expires_at` = `expiry_unix`
- `credential_ttl` 默认 86400 秒

签发为纯函数。第一期无独立刷新接口。

### 5.3 降级

| 情况 | `$P2P` | invite |
| --- | --- | --- |
| secret 可读且 URL 合法 | 全开 | `stun_urls` + `turn` |
| secret 不可用、STUN URL 合法 | REGISTER/CREATE 可用 | 仅 `stun_urls` |
| P2P 段存在且 URL 非法 | 包装进程启动失败 | — |
| 占用/对端检查失败 | 返回错误码 | 不发 invite |

## 6. 错误码

失败体：`{"ok":false,"error":"<code>"}`。

| code | 含义 |
| --- | --- |
| `invalid_node_key` | `node_key` 或 `peer_node_key` 非法 |
| `name_mismatch` | REGISTER 的 `node_key` 与 CONNECT `name` 不一致 |
| `already_registered` | 本连接语义下已用另一身份登记（防御性；正常路径 name=node_key） |
| `node_key_in_use` | 已被占用，或两阶段 PREPARE 失败/超时 |
| `not_registered` | 未 REGISTER 就 CREATE / UNREGISTER |
| `peer_not_registered` | 对端不在占用表 |
| `peer_is_self` | `peer_node_key` 等于自己 |
| `invalid_request` | JSON 无法解析或缺字段 |

NATS 认证失败、subject 权限拒绝仍走 NATS 协议错误。

## 7. 测试与代码位置

| 路径 | 内容 |
| --- | --- |
| `p2p/` | 占用表、REGISTER/CREATE、TURN、MGR 两阶段 |
| `cmd/nats-p2p-server/` | 嵌入 `server.NewServer` 并启动 P2P |
| `p2p/*_test.go` | 单机与多嵌入实例测试 |

**禁止**为该功能修改 `server/*.go`（除非发现公开 API 的缺陷并单独上游化）。

不测 libjuice、真实 coturn Allocate、QUIC Hello、Gateway/Leaf。TURN 用测试 secret 复算 HMAC-SHA1。

单机：

1. REGISTER 成功后，同一连接再 REGISTER 同一 key 成功。
2. 第二条连接（同一 `name`）REGISTER → `node_key_in_use`。
3. 第一条断开后，同一 key 可被新连接登记。
3b. 配置省略 `ping_*` 时，包装后的 `Options.PingInterval==5s` 且 `MaxPingsOut==3`；显式写出则保持运维值。
4. `node_key` ≠ CONNECT name → `name_mismatch`。
5. 两端均已登记时 CREATE：双方 inbox 收到无 `grant` 的 `invite`，且含 `ice_subject`。
6. 对端未登记 → `peer_not_registered`。
7. 有 secret → `turn` 可用同一 secret 复算通过。
8. 无 secret → invite 无 `turn`，仍有 `stun_urls`。
9. P2P 段中 TURN URL 为环回 → 包装进程启动失败。

集群（2 或 3 个包装进程组成 NATS cluster）：

10. 节点 A 已登记的 key，节点 B 再登记 → `node_key_in_use`。
11. A 上持有者断开后，B 可以登记该 key。
12. 两端连不同节点，CREATE 后双方都收到 invite。
13. 两节点同时 REGISTER 同一 key → 恰好一个成功。

## 8. 已定决策

| 决策 | 选择 |
| --- | --- |
| 产品形态 | NATS 作信令总线；`$P2P` 为嵌入式旁路模块 |
| 进程模型 | Go 库嵌入：一进程 = nats-server + P2P 客户端 |
| 与内核关系 | 不改 `server/`；只用公开 API |
| 身份 | Agent 自生成 16 hex `node_key`；CONNECT `name` = `node_key` |
| 客户端探活 | 包装进程默认 `ping_interval=5s`、`ping_max=3`；不改 `server/` 内核默认 |
| 占用 | 同一 account 内先到先得；`$P2P.MGR.*` 两阶段 |
| grant | 不签发 |
| TURN | P2P 模块持有长期 secret，CREATE 签发短时票 |
| ICE 转发 | 普通 PUB/SUB，P2P 不校验成员 |
| 否决 | 改 nats-server 内核；独立 sidecar 进程 |
