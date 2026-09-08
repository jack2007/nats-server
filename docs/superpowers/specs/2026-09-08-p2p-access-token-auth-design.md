# nats-p2p 短期接入 Token 与 TLS 认证设计

日期：2026-09-08

状态：设计待评审

服务端仓库：`nats-server`（`cmd/nats-p2p-server` + `p2p/`）

客户端仓库：`~/src/raypx2`
替代基线：`2026-08-22-nats-p2p-auth-callout-design.md` 中的临时共享用户名/密码

## 1. Scope and Goals

### 1.1 背景

`nats-p2p-server` 是 raypx2 P2P ICE signaling 的控制面。Agent 数量不可预先确定，
不适合在 NATS 配置中持续增加、删除账号；所有 Agent 属于同一业务角色，不需要账号级
RBAC，但必须保留当前按 `CONNECT name == nats.node_key` 动态签发的节点级 subject
隔离，防止一个已接入节点直接操作另一个节点的 CMD/EVENT 前缀。

当前实现有两个生产阻断：

1. Agent 使用源码内硬编码的共享用户名/密码，任何持有者都可声明任意 `node_key`；
2. raypx2 bundled `nats.c` 强制 `NATS_BUILD_WITH_TLS=OFF`，只接受 `nats://`，跨不可信
   网络时 CONNECT 凭据和信令没有端到端传输保护。

本设计使用 TLS 加密 Agent↔NATS 链路；由 `nats-p2p-server` 的内部接口签发短期、
自包含、不可篡改的接入 Token；可信带外服务获取后分发到对应 Agent；同进程 Auth
Callout 无状态验证 Token，并继续签发节点级 NATS User JWT。服务端不保存 Agent、Token
或登录 Session 数据库。

### 1.2 核心目标

- Agent 只需 NATS URL、CA 信任、稳定 `node_key` 和一个可刷新 Token。
- 新增、删除 Agent 不修改 `nats-server.conf`，不维护 NATS 用户表。
- Token 绑定 `node_key`、audience、签发时间和过期时间；任何字段被修改后验签失败。
- Token 由可信带外服务经独立内部接口获取；终端 Agent不能访问签发接口。
- Agent 启动时检查缓存 Token；缺失或即将过期时由带外通道刷新；长运行进程后台刷新。
- NATS 连接和自动重连从线程安全 Token provider 读取最新 Token。
- Auth Callout生成的 User JWT 最长4小时，且绝不晚于接入 Token 过期时间。
- Token、Token HMAC key、Callout issuer seed 不进入配置正文、日志、metrics、trace、
  core dump 上传物和测试产物。
- 单节点与多节点 NATS 集群使用相同验证 keyring；各接入节点本地完成 Callout。

### 1.3 非目标

- 不建设 OIDC、LDAP、通用 IAM 或面向终端 Agent 的账号系统。
- 第一版不提供单 Token即时吊销、Token使用次数限制或 Agent在线列表。
- 第一版不以 Token 证明终端软件未被修改；终端可读取进程内存，Bearer Token 在有效期
  内可能被复制。
- 不改变 P2P V2 REGISTER、SESSION、CONNECTION、SIGNAL 的 wire contract。
- 不把 Center `node_key` 与 `nats.node_key` 合并；两者保持现有独立语义。
- 不使用 NATS 顶层单值 `authorization.token`；它不能验证动态签发 Token。
- 不启用客户端证书，不把 Agent↔NATS TLS 扩展成 mTLS。

## 2. Functional and Non-Functional Targets

### 2.1 功能契约

| 项 | 固定设计 |
| --- | --- |
| Token 类型 | Bearer、自包含、HMAC-SHA256 签名 |
| Token 格式 | `p2p1.<kid>.<payload-b64url>.<mac-b64url>` |
| 默认/最大 TTL | 24小时；签发 API 不接受调用方自定义 TTL |
| 时钟偏差 | `nbf`/`exp` 验证允许正负120秒；TTL上限不因偏差放宽 |
| Token 最大长度 | 2048字节 |
| `node_key` | 与 raypx2 当前规则一致：恰好16位十六进制；签发和验证时规范化为小写 |
| Audience | 固定 `raypx2-nats` |
| Callout Session JWT | `exp = min(token.exp, now+4h)` |
| 刷新门槛 | `exp - max(1h, Token TTL*20%)` |
| 重试 | 带外刷新使用带抖动指数退避；认证失败只强制刷新一次，禁止紧循环 |
| Keyring | 一个 active signing key，加零个或多个 verify-only key；正常仅保留前一代 |

### 2.2 初始非功能门槛

| 目标 | 门槛 |
| --- | --- |
| 签发 API 延迟 | 单实例500 RPS时 p95 ≤ 50 ms、p99 ≤ 100 ms，5xx < 0.1% |
| Callout计算延迟 | 单实例1000次新连接/秒时 p95 ≤ 10 ms、p99 ≤ 25 ms |
| NATS TLS+认证 | 同地域正常网络 p95 ≤ 200 ms、p99 ≤ 500 ms，失败率 < 0.1% |
| 可用性 | 任一 NATS 节点只依赖本地 keyring和本地 Callout，不依赖在线 IAM/数据库 |
| 恢复 | 单节点故障 RTO ≤ 5分钟；无持久认证状态，RPO = 0 |
| 容量 | 第一版门禁至少覆盖1000次 TLS 新连接/秒/节点和10万保持连接/节点 |
| 密钥轮换 | 无全量停机；旧 key移除前等待 `max_token_ttl + 2*clock_skew` |
| 日志安全 | 已知 Token/HMAC key/issuer seed 扫描零命中 |

这些是发布基线而不是业务峰值承诺；若现网容量目标更高，发布前必须按第5、6节重新
测量并上调门槛，不能静默降低。

## 3. System-Level End-to-End Function Map

### 3.1 组件和信任边界

```text
终端 Agent（不可信设备）
  │  既有带外安全通道：请求/接收 node_key 对应的短期 Token
  ▼
可信带外服务
  │  HTTPS + mTLS，仅内部网络
  ▼
nats-p2p-server Token Issuer API
  ├─ 读 HMAC keyring，签发 Token，不落库
  └─ 不暴露给 Agent 公网

Agent
  │  tls:// + CONNECT auth_token + name=node_key
  ▼
nats-server 内核
  │  $SYS.REQ.USER.AUTH（独立 AUTH account）
  ▼
同进程 Auth Callout
  ├─ 验 HMAC、aud/iat/nbf/exp、node_key绑定
  └─ 用独立 Account NKey issuer seed 签短期 User JWT
       │
       └─ publish 仅 own CMD；subscribe 仅 own EVENT + _INBOX
```

安全域必须分开：

- Token HMAC key只用于接入 Token签发/验证；
- Callout Account NKey seed只用于签 Authorization Response/User JWT；
- HTTPS mTLS server key只用于签发接口传输；
- NATS TLS server key只用于 Agent NATS 传输；
- 四类 key禁止复用。

### 3.2 Token wire format

Token 必须恰好包含四段：

```text
p2p1.<kid>.<payload-b64url>.<mac-b64url>
```

- `kid`：ASCII `[A-Za-z0-9_-]{1,32}`，不是 Base64；只用于选择候选 key，未经签名验证
  前不可信。
- `payload-b64url`：RFC 4648 URL-safe、无 `=` padding 的 JSON 原始字节编码。
- MAC 输入：前三段的 ASCII 原文，精确为
  `p2p1 + "." + kid + "." + payload-b64url`。
- `mac-b64url`：`HMAC-SHA256(key, mac_input)` 的32字节结果，以 Raw URL Base64编码。
- 比较必须使用 `hmac.Equal`；不接受截断 MAC、标准 Base64替代、额外空白或额外段。

Payload 只允许以下字段，未知字段和重复 JSON key 均拒绝，数字必须是 JSON int64：

```json
{
  "v": 1,
  "aud": "raypx2-nats",
  "node_key": "0123456789abcdef",
  "iat": 1788840000,
  "nbf": 1788840000,
  "exp": 1788926400,
  "jti": "22-char-base64url"
}
```

规则：

1. `v == 1`、`aud == "raypx2-nats"`；
2. `node_key` 必须已是16位小写十六进制；接口可接受大小写输入，但签入前转小写；
3. `iat <= nbf <= exp`；签发端设置 `exp-iat == configured_ttl`，验证端要求
   `0 < exp-iat <= configured_ttl <= 86400`；首版默认 `configured_ttl=86400`；
4. `jti` 是16字节 CSPRNG的 Raw URL Base64，共22字符；它只用于关联审计，不提供无状态
   重放阻断；
5. 验证流程先限制总长度、拆段、按 `kid` 取 key并验 MAC，再解析/信任 JSON；
6. 不通过的 Token统一返回 NATS `Authorization Violation`，服务端内部 reason使用低基数
   枚举，不回显 Token或 payload。

### 3.3 Token keyring

`p2p.access_token.keyring_file` 指向不进入主配置的 JSON Secret：

```json
{
  "version": 1,
  "active_kid": "2026-09-a",
  "keys": [
    {"kid": "2026-09-a", "state": "active", "key": "43-char-base64url"},
    {"kid": "2026-08-a", "state": "verify", "key": "43-char-base64url"}
  ]
}
```

- key 解码后必须恰好32字节，来源为操作系统 CSPRNG。
- 恰好一个 `active`，必须与 `active_kid` 一致；零个或多个 `verify`。
- `kid` 不重复；未知 state、未知字段、重复 key材料均拒绝启动。
- 文件必须是绝对路径、普通文件、非符号/硬链接、进程所有者持有且权限精确 `0600`；
  任一检查失败均 fail closed。
- 各集群节点必须部署相同 keyring；启动日志只打印 active/verify `kid`，不打印 key或摘要。
- 第一版通过受控重启加载 keyring，不实现文件 watcher。轮换采用“先下发验证 key、再切
  active、最后移除旧 key”的三阶段滚动流程，见第7节。

Callout issuer seed改由 `p2p.access_token.auth_response_seed_file` 加载，使用同等级文件
检查；其公钥必须等于 `authorization.auth_callout.issuer`，不一致则拒绝启动。源码内
`IssuerSeed` 和共享 Agent用户名/密码从生产路径删除。

### 3.4 内部签发 API

第一版提供专用 HTTPS listener，不复用 NATS monitoring端口。非 mTLS 请求不能进入
HTTP handler。

配置草案：

```text
p2p {
  access_token {
    keyring_file: "/run/secrets/p2p-token-keyring.json"
    auth_response_seed_file: "/run/secrets/p2p-auth-response.nk"
    ttl: 24h
    session_ttl: 4h
    clock_skew: 2m

    issuer_api {
      listen: "10.20.0.15:7443"
      cert_file: "/run/secrets/token-api-cert.pem"
      key_file:  "/run/secrets/token-api-key.pem"
      ca_file:   "/run/secrets/token-api-client-ca.pem"
    }
  }
}
```

`listen` 禁止 wildcard、loopback以外的公网地址和 Unix未解析主机名；部署层还必须用
防火墙只允许带外服务网段。HTTPS最低 TLS 1.2，必须验证客户端证书链；第一版使用专用
client CA，不增加 DN/SAN allowlist。若 access_token启用但 listener、证书、keyring或
Callout issuer不完整，包装进程拒绝启动。

接口：

```http
POST /internal/v1/p2p/access-tokens
Content-Type: application/json
Accept: application/json

{"node_key":"0123456789abcdef"}
```

请求体最大1024字节，只允许 `node_key`；缺失、null、非字符串、未知字段、非法格式均
返回400。调用方不能指定 TTL、audience、kid、权限或 role。

成功响应：

```http
HTTP/1.1 200 OK
Content-Type: application/json
Cache-Control: no-store
Pragma: no-cache

{
  "token_type": "Bearer",
  "token": "p2p1.2026-09-a.eyJ2IjoxLC4uLn0.xxxxx",
  "node_key": "0123456789abcdef",
  "issued_at": 1788840000,
  "expires_at": 1788926400,
  "refresh_after": 1788909120
}
```

`refresh_after = exp - max(3600, ttl/5)`。每次调用生成新 `jti` 和新 Token；接口不保证
相同响应幂等，但重复签发不会使旧 Token失效。

mTLS 握手或客户端证书验证失败时连接在 TLS 层终止，不产生 HTTP 状态码，也不进入
handler。只有通过 mTLS 的请求才适用下表：

状态码：

| 状态 | 含义 | 是否重试 |
| --- | --- | --- |
| 200 | 成功 | 否 |
| 400 | 请求/schema/node_key错误 | 修复请求后重试 |
| 404/405 | 路径或方法错误 | 不重试 |
| 413 | 请求过大 | 不重试 |
| 429 | 超过调用方/IP限流 | 按 `Retry-After` + jitter |
| 500 | CSPRNG/编码等内部错误 | 有界退避 |
| 503 | keyring不可用或服务停止中 | 有界退避并告警 |

`GET /internal/v1/p2p/healthz` 只在 mTLS通过后返回 `{"status":"ok"}`；不得返回 key、
Token、配置正文或客户端统计。listener设置读头、读体、写响应和 idle timeout，慢请求
不能无限占用 goroutine。

### 3.5 Auth Callout判定

Agent CONNECT 使用：

```text
auth_token = p2p1....
name       = lowercase nats.node_key
```

Callout按以下顺序 fail closed：

1. 解码 NATS Authorization Request；
2. 读取 `ac.ConnectOptions.Token`，拒绝同时携带 user/password/JWT/NKey 的混合凭据；
3. 按3.2节验 Token；
4. 使用服务端当前 UTC检查 `nbf`/`exp`；客户端时间不参与安全决策；
5. 规范化并验证 `ac.ConnectOptions.Name`，要求与 Token `node_key` 完全相等；
6. 使用请求的一次性 `ac.UserNkey` 作为 User JWT subject；
7. `audience=$G`，`exp=min(token.exp, now+session_ttl)`；
8. 复用当前 V2权限：
   `publish=$P2P.V2.CMD.<node_key>.>`，
   `subscribe=$P2P.V2.EVENT.<node_key>.>,_INBOX.>`；
9. Authorization Response audience绑定请求的 `server.ID`，由独立 Account NKey seed签名；
10. 所有拒绝都返回签名错误响应，不把内部原因发给 Agent。

Callout运行在独立 `AUTH` Account。`auth-internal` 是该 Account唯一应用用户；
`p2p-internal`、sys用户和 Agent都不在 `AUTH`。这避免其他内部连接订阅
`$SYS.REQ.USER.AUTH` 读取 Bearer Token。为此现有 `AuthInternalCredentials` 必须改为按
`authorization.auth_callout.account` 找到账号内用户，不能只扫描顶层 `opts.Users`。

### 3.6 Agent TLS 与 Token provider

raypx2 当前状态：

- `TqNatsConfig` 只有 `Url/NodeKey/User/Password`；
- `NatsConnectionDriver::Connect` 调用 `natsOptions_SetUserInfo`；
- bundled nats.c 已提供 `natsOptions_SetTokenHandler`；
- CMake当前强制 `NATS_BUILD_WITH_TLS OFF`；URL校验只接受 `nats://`。

目标配置：

```json
{
  "nats": {
    "enabled": true,
    "url": "tls://nats.example.com:4222",
    "node_key": "0123456789abcdef",
    "token_file": "/var/lib/raypx2/nats/access-token.json",
    "ca_file": "/etc/raypx2/nats-ca.pem",
    "ping_interval_ms": 5000,
    "max_pings_out": 2
  }
}
```

- 生产 token模式禁止同时配置 `user/password`；兼容字段只允许隔离测试 fixture，发布
  配置校验拒绝混用。
- `tls://` 必须启用 nats.c TLS、加载 `ca_file` 并验证 URL hostname；禁止 skip verify。
- 若服务器证书链由系统信任根签发，`ca_file` 可省略并使用系统 trust store。
- `token_file` 不写入通用 runtime config响应；Admin只返回路径和 `token_present`、
  `expires_at`、`refresh_due`，绝不返回 Token。

Token文件合同：

```json
{
  "version": 1,
  "token_type": "Bearer",
  "token": "p2p1....",
  "node_key": "0123456789abcdef",
  "expires_at": 1788926400,
  "refresh_after": 1788909120
}
```

文件使用 raypx2现有 Admin token相同的安全读写基线：绝对路径、专用 `0700` 父目录、
当前用户所有、普通文件、非符号/硬链接、权限精确 `0600`、inode复核和原子替换。

Agent 内部新增线程安全 `NatsAccessTokenProvider`：

- `Snapshot()` 返回内存中当前 Token和时间元数据；
- `Publish()` 验证外层 schema、node_key绑定和过期时间后原子替换；不在 Agent侧验
  HMAC，服务端仍是唯一安全判定者；
- `natsOptions_SetTokenHandler` 回调只执行无锁/短锁内存读取，不做HTTP、文件 I/O、日志
  或等待；回调返回字符串生命周期必须满足 nats.c合同；
- 带外服务更新文件或内存 provider后，新连接/重连自动使用新 Token；已有连接不在原地
  重认证，最迟在 User JWT 4小时到期后由服务器关闭并自动重连；
- 收到 Authorization Violation时触发一次强制带外刷新并重连；同一失败代次只允许一次，
  防止认证风暴。

第一版自动下发通道明确复用现有 Center Agent，而不是引入新的终端侧服务：Center
enroll/session refresh请求携带当前 `nats.node_key`，成功响应扩展可选 `nats_access`
对象；Center服务端用 mTLS调用签发 API，再把响应原样安全下发。Center session token和
NATS Token是两类独立 Token，禁止互用。服务端尚未启用 NATS能力时可暂不返回
`nats_access`，客户端据此保持降级状态。

由于 Center当前异步启动且 NATS runtime可能更早初始化，缺 Token时 NATS ICE进入
`credential_unavailable/degraded`，不阻塞 Direct peer；Center发布 Token后触发 NATS
transport首次连接。Token callback本身绝不调用 Center网络接口。

`token_file` 同时作为 Center下发后的安全缓存和人工预置边界。不使用 Center的部署可由
受信带外 Agent按同一合同原子写文件，但这不属于首版自动刷新实现范围；若要长期启用短期
Token，部署方必须实现等价的刷新适配器。终端 Agent在任何模式下都不能直接访问内部签发
API。

### 3.7 正常时序

1. raypx2加载配置并确保稳定 `nats.node_key`；
2. 读取安全 Token缓存；存在且 `now < refresh_after` 时可立即启动 NATS；
3. 缺失、过期或到刷新时间时，向既有带外通道请求刷新；旧 Token仍有效则继续使用；
4. 可信带外服务用 mTLS 调用签发 API并把结果下发；
5. Agent原子持久化并 `Publish()` 到 provider；
6. nats.c用 `tls://` 建连，Token handler提供最新 Token，CONNECT name使用 node_key；
7. Callout验 Token并签4小时以内的 User JWT；
8. Agent订 own EVENT、Flush、REGISTER，进入 Ready；
9. 运行中到 `refresh_after` 再走步骤3～5；
10. 连接重连或 Session JWT到期时 nats.c重新调用 Token handler，无需重启进程。

### 3.8 每段可测试断言

| 链路 | 成功断言 | 失败断言 | 可观测结果 |
| --- | --- | --- | --- |
| OOB→Issuer | mTLS + 合法 node返回200 | 无证书/错CA拒绝 | API request计数、低基数状态 |
| Issuer→Token | exp等于配置 TTL（默认24h）、MAC可验证 | CSPRNG/keyring错返回500/503 | issuance latency/error；无 Token日志 |
| OOB→Agent | node_key匹配后原子发布 | 错 node/过期/schema错不覆盖旧值 | refresh result metric |
| Agent→NATS TLS | hostname/CA验证成功 | 明文、错CA、错hostname失败 | TLS/auth reason metric |
| CONNECT→Callout | token/name匹配成功 | 篡改、过期、混合凭据拒绝 | auth result/reason，无原文 |
| Callout→权限 | own subject可用 | other node、`$SYS.>`、`>`拒绝 | NATS权限事件 |
| 重连 | handler取新 Token并重REGISTER/RESUME | 刷新不可用时有界退避 | degraded和收敛耗时 |

## 4. Test Strategy and Coverage Matrix

### 4.1 分层策略

- Go单元：Token编码/验签、strict JSON、时间边界、keyring安全加载、权限生成。
- Go TCP/TLS集成：真实 `ProcessConfigFile`、Auth Callout、独立 AUTH Account、错误凭据和
  subject越权。
- raypx2 C++单元：配置、token_file安全加载、provider并发、nats.c callback生命周期、
  TLS选项和刷新状态机。
- 双仓端到端：mTLS签发→带外落盘/发布→Agent TLS CONNECT→REGISTER→ICE signaling。
- 系统故障：滚动轮换、节点/Callout/OOB故障、时钟偏差、网络分区和连接风暴。

### 4.2 覆盖矩阵

| ID | 场景 | 期望 |
| --- | --- | --- |
| F-01 | 合法 node签发并连接 | 200；TLS CONNECT/REGISTER成功 |
| F-02 | 同 node重复签发 | 两 Token均有效、JTI不同，旧 Token不被隐式吊销 |
| F-03 | Token缺失/多段/超长/Base64错误 | Callout拒绝，无 panic |
| F-04 | 修改 payload/exp/node或MAC | Authorization Violation |
| F-05 | 未知 kid/verify key/active key | 未知拒绝；两代合法 key均接受 |
| F-06 | `iat/nbf/exp` 边界±120秒 | 边界按规格；超过即拒绝 |
| F-07 | Token node与 CONNECT name不同 | 拒绝且不签 JWT |
| F-08 | user/password+token混用 | 拒绝，不选择降级认证 |
| F-09 | own/other/$SYS/`>` subject | own成功，其余拒绝 |
| F-10 | Token后台更新后断线重连 | handler使用新 Token，重新REGISTER/RESUME |
| F-11 | Token到期但连接仍在 | 最迟 User JWT exp断开；新 Token自动重连 |
| F-12 | 本地时间被调前/调后 | 可影响刷新调度，不能绕过服务端 exp；认证失败强制刷新一次 |
| F-13 | Token文件symlink/hardlink/0644/换 inode竞态 | 拒绝读取或更新，旧内存 Token不泄漏 |
| F-14 | TLS错CA/hostname/过期证书/明文客户端 | 均失败关闭，不降级 `nats://` |
| F-15 | issuer API无/错客户端证书 | TLS层拒绝，无业务响应 |
| F-16 | Callout issuer/keyring缺失或不匹配 | server拒绝启动 |
| F-17 | AUTH Account隔离 | p2p/sys/Agent不能订阅 auth request |
| F-18 | 双NATS节点各本地Callout | 任一节点可独立认证；同 Token跨节点有效 |
| F-19 | Center未Ready、Token缺失 | NATS ICE degraded；Direct不受影响；Token到达后收敛 |
| F-20 | 停止/升级期间签发请求 | 503或完成中的请求有界结束，无半响应 |

安全 fuzz目标：Token splitter、Base64、strict JSON、数值溢出、Unicode、重复键、未知字段、
node_key和 keyring parser；任何输入不得 panic、超量分配或把输入写入日志。

## 5. k6 Performance Baseline

k6只压内部 HTTPS签发 API；NATS协议连接使用 Go/nats.c专用 load probe，二者结果合并为
同一发布证据。

### 5.1 k6 workload

| 场景 | 流量模型 | 负载 |
| --- | --- | --- |
| baseline | 30秒 ramp + 5分钟 steady + 30秒 down | 100 RPS |
| expected-peak | 1分钟 ramp + 10分钟 steady | 500 RPS |
| spike | 10秒内升高、保持60秒、30秒恢复 | 2000 RPS |
| stress | 每3分钟增加500 RPS | 直到 p99>100ms或错误>1% |
| soak | 稳态 | 200 RPS，2小时 |

请求 mix：95%合法随机16 hex node，2%非法 node，1%未知字段，1%超大请求，1%错误/缺失
client cert（由独立 negative scenario建立连接）。成功请求不复用 HTTP响应 Token；k6输出
和 summary禁止保存响应体。

阈值：

```text
http_req_failed{scenario:expected_peak} < 0.001
http_req_duration{scenario:expected_peak} p(95) < 50ms, p(99) < 100ms
checks > 99.9%
dropped_iterations == 0
```

同时采集进程 CPU、RSS、goroutine、TLS handshake、打开 FD、GC pause、签发 RPS和错误。
expected-peak时单核不能持续超过80%，RSS相对稳态增长不得超过10%；soak结束后 goroutine
和FD回到基线±5%。

环境固定为与生产相同 TLS/keyring配置的单实例，调用方和服务端同地域1 ms以内 RTT；
不得 mock HMAC、CSPRNG或 mTLS。每次记录二进制 SHA-256、Go版本、CPU/内存、配置脱敏
摘要和前一发布基线。

### 5.2 NATS连接基线

专用 probe建立真实 TLS连接、发送 Token、等待 REGISTER成功后按场景保持或关闭：

- baseline 100 connects/s，5分钟；
- expected peak 1000 connects/s，10分钟；
- spike 5000 connects/s，60秒；
- 10万保持连接，随机1%/分钟断线重连，30分钟；
- 双节点各承载50%，停一节点验证重连风暴。

门槛使用2.2节；另外要求认证失败请求不比成功请求消耗超过2倍 CPU时间，防止恶意无效
Token形成明显放大。

## 6. Capacity and Scalability Validation

- Token验证完全无状态；容量随 NATS节点水平扩展，不引入共享数据库锁。
- keyring、clock skew、TTL在集群所有节点一致；配置摘要不一致触发启动告警和 readiness
  失败，避免同一 Token随机成功/失败。
- 每个 Authorization Request最大 Token 2048字节，解析前限长；签发 API请求1024字节，
  防止内存放大。
- issuer API按 mTLS peer证书指纹和源IP分别限流：初始1000 RPS持续、2000 burst；429带
  `Retry-After`。这是保护门槛，不低于 expected peak。
- Callout timeout初始1秒，Agent connect timeout必须大于 Callout timeout并包含 TLS预算；
  无响应时快速失败，不堆积无限 goroutine。
- 10万连接容量门禁同时确认 Session JWT到期分散。Token签发 `iat`自然分散；部署服务
  不得在整点批量刷新，Agent刷新加入0～10% TTL的稳定 jitter。

## 7. Abnormal Conditions and Disaster Recovery

认证数据无数据库，RPO为0；恢复目标主要是恢复 keyring、issuer seed、TLS证书和进程。

| 场景 | 注入/用户影响 | 检测 | 缓解与恢复 | 验收 |
| --- | --- | --- | --- | --- |
| OOB服务不可用 | 新/即将过期 Token不能刷新；有效连接继续 | refresh error、Token余量告警 | 有界退避；恢复OOB后刷新 | 有效 Token期内无断；恢复后10分钟内收敛 |
| issuer API单节点挂 | 签发部分失败 | 5xx/连接错误、health | 多实例/负载均衡；重试其他实例 | RTO≤5分钟，无状态丢失 |
| 本地 Callout停止 | 该节点新连接失败 | auth timeout、ready失败 | LB摘除节点；重启节点 | 不接受未认证连接；恢复后2分钟可连 |
| NATS节点重启 | 连接迁移并重认证 | disconnect/reconnect指标 | nats.c自动重连，最新 Token handler | P2P信令按现有RTO恢复，已有QUIC不主动断 |
| keyring损坏/权限错 | 节点启动失败 | startup fatal | 回滚上一个受控 Secret版本 | fail closed；5分钟内恢复 |
| active key泄漏 | 攻击者可伪造未过期新 Token | Secret事件/异常JTI与连接 | 生成新 key，三阶段轮换；泄漏 key立即移出 verify会中断旧连接后续重认证 | 新伪造Token全部拒绝；记录影响窗口 |
| Callout issuer泄漏 | 可伪造授权响应 | Secret事件 | 生成新 Account NKey、同步配置、滚动重启 | 旧 issuer响应不再接受 |
| TLS证书过期/错SAN | Agent全部新连失败 | 到期告警、TLS error | 提前轮换证书；回滚证书Secret | 不允许 skip verify绕过 |
| 时钟漂移>120秒 | Token随机失败 | NTP/chrony告警、reason=not_yet_valid/expired升高 | 摘除漂移节点、校时后恢复 | 校时后2分钟内恢复；不放宽全局窗口 |
| 网络分区 | Agent连接当前节点失败 | connect error、cluster指标 | 自动发现/重连其他节点 | Token跨节点验证一致 |
| CPU/内存饱和 | 签发/Callout延迟升高 | p99、CPU、queue、GC | 限流、LB摘除、扩容 | 不 OOM；恢复后资源回基线 |
| 滚动 key轮换 | 混部期间新旧 Token并存 | per-kid验证指标 | 阶段A所有节点加入verify key；阶段B切active；等待24h+240s；阶段C删除旧 key | 全阶段无随机认证失败 |

备份只保存加密的 Secret管理系统版本，不把明文 keyring打入普通配置备份。恢复演练每季度
至少一次：从受控 Secret版本重建新节点、验证已签 Token、签发新 Token、连接并完成一次
REGISTER/CREATE/ICE signaling。RTO ≤ 5分钟，RPO = 0。

## 8. Observability and Test Evidence

### 8.1 Metrics

- `p2p_token_issuance_total{result,reason,kid}`；
- `p2p_token_issuance_duration_seconds`；
- `p2p_auth_callout_total{result,reason,kid}`；
- `p2p_auth_callout_duration_seconds`；
- `p2p_auth_token_remaining_seconds` 只在 Agent本地以区间表达，不带 JTI；
- `p2p_auth_refresh_total{result,source}`；
- `p2p_auth_keyring_info{kid,state}=1`，不带 key摘要；
- 既有 `nats_degraded_total`、`nats_ready_convergence_ms`、连接数继续使用。

reason限定枚举：`ok/missing/malformed/unknown_kid/bad_mac/wrong_audience/not_yet_valid/
expired/ttl_invalid/node_invalid/node_mismatch/mixed_credentials/internal`。禁止把 node_key、JTI、
Token片段放入 metric label。

### 8.2 日志与审计

- 签发审计可记录 mTLS peer指纹、规范化 node_key、JTI、kid、iat/exp、结果和 request ID；
- Callout日志只记录 server ID、client ID、node_key、kid和低基数 reason；
- 不记录 Authorization header、CONNECT原文、Token、payload-b64url、MAC、HMAC key、issuer
  seed或 token_file正文；
- 测试产物发布前扫描 `p2p1\.`、`Authorization:`、JSON `token/key/secret/password`、
  `BEGIN USER NKEY SEED` 等模式；命中敏感实值则门禁失败并销毁/重生证据。

每轮系统测试证据包括：版本与摘要、脱敏配置、拓扑、k6 summary、NATS load summary、
故障注入时间线、关键 metrics、敏感信息扫描结果、清理结果。清理失败时整轮失败。

## 9. Entry, Exit, and Release Gates

### 9.1 Entry

- 设计和实现计划经 nats-server、raypx2、带外服务三方 owner确认；
- 明确生产域名、CA、issuer API内部地址和 Secret交付方式；
- raypx2 bundled nats.c TLS能与现有 quictls/OpenSSL构建链兼容；
- 测试环境能运行双节点 NATS、真实 TLS/mTLS和带外适配器。

### 9.2 Exit

- 第4节 F-01～F-20全部通过；安全 fuzz无 crash/泄漏；
- 第5节 expected peak、spike、soak和10万连接门禁通过；
- 第7节节点故障、OOB故障、keyring损坏、时钟漂移和三阶段轮换通过；
- 源码和生产样例不再含 Agent共享密码或 Callout issuer seed；
- `raypx2` 生产构建支持 `tls://`、CA/hostname验证和 token handler；
- 明文 `nats://` + user/password只允许显式 test-only构建/fixture；生产配置拒绝；
- 文档、配置示例、升级/回滚 runbook同步；敏感扫描零命中。

### 9.3 发布和回滚顺序

发布：

1. 部署 HMAC keyring、外置 Callout issuer seed和 mTLS Secret；
2. 部署支持 Token但继续兼容旧凭据的 nats-p2p-server，完成 issuer API探测；
3. 部署带外服务 Token获取/分发；
4. 部署支持 TLS/token_file/provider的 raypx2，按节点切换；
5. 确认所有节点不再使用旧 user/password；
6. 关闭并删除旧共享凭据兼容路径及源码常量；
7. 执行完整门禁后声明生产基线。

回滚只允许回到“同时支持 Token和旧凭据”的上一阶段，不允许为了恢复连接关闭 TLS或
Auth Callout。旧共享凭据在最终删除后不再恢复；紧急恢复应重新签 Token或回滚应用版本，
而不是开放匿名连接。

## 10. Risks, Assumptions, and Open Questions

### 10.1 已接受风险

- Bearer Token从终端内存提取后可在 `exp` 前重放；TLS只防网络窃听，不防终端控制者。
- 第一版无 denylist，不能立即单独吊销；风险窗口最多24小时，已建立连接的授权窗口最多
  `min(剩余Token时间, 4小时)`。
- 本地 Agent时间可被修改，因此刷新可能过早/过晚；服务端时间始终权威，认证失败会触发
  一次强制刷新。

### 10.2 假设

- 带外服务已经能认证/定位目标 Agent，且只有它能访问内部 mTLS签发接口；
- 带外服务按请求中的 NATS `node_key` 分发，不把 Token发给其他节点；
- 各 NATS节点有可靠时间同步；
- 生产 Secret由现有 Secret管理系统分发，能够执行三阶段 keyring轮换；
- Agent可能长时间不重启，因此后台刷新是必须项而非优化项。

### 10.3 后续可选增强

- 小型 `node_key`/JTI denylist，实现紧急单节点吊销；
- 将 HMAC签名改成独立 Ed25519签发服务，使 NATS节点只持公钥；
- Auth Callout request `xkey`加密，作为独立 AUTH Account之外的纵深防御；
- 硬件/平台 attestation，提高“官方客户端”而非“持 Token者”的可信度；
- 缩短 Token TTL到1～4小时，并让带外刷新成为持续高可用能力。

## 11. 预期代码边界

| 仓库/路径 | 责任 |
| --- | --- |
| `p2p/access_token.go` | Token/keyring strict编解码、HMAC、时间校验 |
| `p2p/token_issuer.go` | 内部 mTLS HTTPS listener和接口 |
| `p2p/auth_callout.go` | 从 user/password切到 Token verifier；外置 issuer seed；节点级 JWT |
| `p2p/config.go`, `wrapconfig.go` | `p2p.access_token` 配置和 fail-closed校验 |
| `cmd/nats-p2p-server/main.go` | 加载 Secret、启动 issuer→Callout→Manager、停止顺序 |
| `conf/nats-p2p-*.conf` | TLS、AUTH Account、Token配置示例 |
| `raypx2/src/config/*` | `tls://`、`token_file`、`ca_file` schema与脱敏 |
| `raypx2/src/runtime/nats_signal.cpp` | nats.c TLS和 `SetTokenHandler`、重连读取 provider |
| `raypx2/src/runtime/center_agent.*` | 首版 Center `nats_access`自动下发与刷新适配器 |
| 双仓测试 | 单元、真实 TLS/Auth Callout、双节点、负载、故障和泄漏门禁 |

## 12. 参考

- NATS Auth Callout：<https://docs.nats.io/learn/security/auth-callout>
- NATS TLS：<https://docs.nats.io/learn/security/encryption>
- HMAC：RFC 2104
- Base64URL：RFC 4648
- 现有 raypx2 NATS说明：`~/src/raypx2/docs/nats-ice-signaling_cn.md`
- 现有客户端连接：`~/src/raypx2/src/runtime/nats_signal.cpp`
- 现有配置：`~/src/raypx2/src/config/config.h`、`config.cpp`
