# nats-p2p Auth Callout 设计

日期：2026-08-22  
状态：已批准  
仓库：`nats-server`（包装进程 `cmd/nats-p2p-server` + `p2p/`）  
对照：`docs/superpowers/specs/2026-08-21-nats-p2p-ice-signaling-design.md`  
Agent 侧：`~/src/raypx2/docs/superpowers/specs/2026-08-22-nats-auth-callout-design.md`

## 1. 背景与目标

当前公网与实验室 `nats-p2p-server` 用静态 `authorization.users`（共享账号）验 CONNECT。要改成官方 **Auth Callout**（NATS Server ≥ 2.10）：服务端把客户端出示的用户名密码发到 `$SYS.REQ.USER.AUTH`，由**同进程**的 auth 模块判定并回签 user JWT。

第一版目标是**跑通 Callout 管道**，不是每机一把钥匙。核对逻辑写在 auth 模块代码里（临时测试用户名密码），后续版本改代码即可。

成功标准：

- Agent 仍用官方 NATS 客户端、明文（无 TLS）CONNECT，字段仍是 `nats.user` / `nats.password`。
- 凭据与代码中临时常量一致 → 进 `$G`，只能碰约定的 `$P2P` 业务 subject。
- 凭据不一致或为空 → `Authorization Violation`，到不了 `$P2P.REGISTER`。
- 现有 REGISTER / CREATE / invite / ICE / 占用表行为不变。
- 单进程与多进程（cluster 各节点各答本地 Callout）都能工作。

## 2. 非目标（v1）

- 不改 `server/*.go`。
- 不做独立 auth 进程 / sidecar。
- 不加顶层 `auth { allowed_user / allowed_password / issuer_seed_file }`。
- 不把临时用户名密码、issuer 种子写入本仓库文档、日志或 `$SYS` 业务事件。
- 不升级 Agent↔NATS 为 TLS，不开 Callout xkey。
- 不接 OIDC / LDAP / Center enroll，不做每机不同密码。
- 不启停 coturn，不改 TURN secret。
- 不跨 Gateway / Leafnode。

## 3. 进程模型

与现有 P2P Manager 相同：包装进程嵌入 `nats-server`，用 `InProcessConn` 当客户端，不把业务写进内核。

```text
包装进程 nats-p2p-server
  ├─ server.NewServer / Start          （官方内核，不改 server/*.go）
  ├─ auth 模块  ──InProcessConn──► 订 $SYS.REQ.USER.AUTH
  └─ p2p 模块   ──InProcessConn──► 订 $P2P.*（现有逻辑）
           ▲
           │ 明文 NATS（无 TLS）
     raypx2 Agent（nats.user / nats.password）
```

| 模块 | 职责 | 不做什么 |
| --- | --- | --- |
| auth | 订 `$SYS.REQ.USER.AUTH`，核对代码里的临时用户名密码，签 user JWT | 不处理 REGISTER/ICE，不读配置块里的密码 |
| p2p | 现有占用表 / invite / TURN | 不验 CONNECT 密码 |

约束：

1. auth 与 p2p（以及已有的 sys 断连客户端）必须列入 `auth_users`，否则会 Callout 自己或在订阅前死锁。
2. 启动顺序：`Start` → `ReadyForConnections` → **先** auth 订阅 → **再** `StartManager` → 包装进程才报就绪。`Start` 后 TCP 已监听，auth 订阅前有极短窗口，v1 接受；对外就绪以「auth 已 SUB」为准。
3. 集群：Callout 发生在**接入那台**。每个进程用本地 `InProcessConn` 回答，**不要**对 `$SYS.REQ.USER.AUTH` 使用 queue group。各节点同一套代码常量（用户名密码 + issuer 种子）和同一份 conf 公钥。

## 4. 配置

### 4.1 NATS 段（`ProcessConfigFile`）

内核要求：若写了 `auth_callout`，必须有合法的 **issuer 公钥**（account nkey，`A` 开头）和至少一名 `auth_users`。

```text
authorization {
  timeout: 2s
  users: [
    { user: auth-internal, password: "<内部>", permissions: { subscribe: ["$SYS.REQ.USER.AUTH"] } }
    { user: p2p-internal,  password: "<内部>", permissions: { publish: [">"], subscribe: [">"] } }
  ]
  auth_callout {
    issuer: "A..."          # 与代码内测试种子对应的公钥
    auth_users: [ auth-internal, p2p-internal ]
  }
}
```

- `auth_users` 只放进程内客户端，不要放 Agent 临时账号。
- `p2p {}` 的 `agent_username` / `agent_password` 必须等于 `p2p-internal`（现有 `StartManager` 的 `UserInfo`）。
- 若已用 `sys_username` 订 `$SYS` DISCONNECT：该用户也要进 `users` 与 `auth_users`。
- 开 Callout 后，静态 `users` 里除 `auth_users` 外都会走 Callout。Agent 不要再写进这份 `users` 表。
- 包装进程在 `ProcessConfigFile` 之后：用代码内 issuer 种子导出公钥，与 `authorization.auth_callout.issuer` 比较，不一致则启动失败。

### 4.2 不加 `auth {}`

临时 Agent 用户名密码、issuer 种子均在 auth 模块常量中。后续版本改这些常量（或再做成配置）即可。文档与示例 conf **不抄写**临时密码和种子。

无 `auth_callout` 块时：不启 auth 协程，行为与现在一致（兼容未切换的实验室 conf）。公网本次切换必须带 `auth_callout`。

## 5. 判定与 JWT

1. 读 Callout 请求 `connect_opts` 的 user / password。
2. 与代码常量做恒定时间比较。空用户名或空密码视为失败。
3. 失败：按 ADR-26 回错误（不要丢弃请求装死），客户端见 `Authorization Violation`。日志不打密码。
4. 成功：用 issuer 种子签 user JWT，`sub` = 请求中的一次性 `user_nkey`。

| 项 | v1 取值 |
| --- | --- |
| account / aud | `$G` |
| publish | `$P2P.REGISTER`、`$P2P.CREATE`、`$P2P.UNREGISTER`、`$P2P.ICE.>` |
| subscribe | `$P2P.NODE.>`、`$P2P.ICE.>`、`_INBOX.>` |
| 不给 | `$P2P.MGR.>`、`$SYS.>`、`>` |

`_INBOX.>` 为 REGISTER/CREATE 的 request-reply 所必需；实现时不得省略。

JWT 有效期按连接生命周期（数小时量级即可）。断线重连再走 Callout。CONNECT `name`（`node_key`）仍由现有 REGISTER 规则校验。

## 6. 错误表

| 情况 | Agent 看到 | 服务端 |
| --- | --- | --- |
| 用户名/密码不是代码常量（含空） | `Authorization Violation` | 回失败；不打密码 |
| auth 未 SUB / 超时 | 超时或 `Authorization Violation` | 未就绪不对外报 ready |
| JWT 未覆盖的 subject | 连得上但 pub/sub 被拒 | `$P2P.MGR` / `$SYS` 必须被拒 |
| REGISTER `name` ≠ `node_key` | `name_mismatch` | 与 Callout 无关 |

## 7. 测试

- 正确常量：CONNECT 成功；可 REGISTER/CREATE；不可 pub/sub `$P2P.MGR.>` / `$SYS.>`。
- 错误密码、空凭据：CONNECT 失败。
- `auth-internal` / `p2p-internal` 绕过 Callout；现有 `go test ./p2p/...` 回归通过。
- 原 `app`/`app` 测试改为走 Callout + 代码常量，或改用内部账号。
- 双实例 cluster：各节点本地 Callout，对端不能代答。

## 8. 公网切换（`64.176.42.49`）

1. 先部署带 auth 模块的 `nats-p2p-server` 和带 `auth_callout` 的 conf（内部用户 + issuer 公钥 + `auth_users`）。
2. 再改 Agent / 脚本中的 `nats.user` / `nats.password`。顺序反了会集体连不上。
3. 不启停 coturn，不改 TURN secret。切 nats 时信令短中断，已有 ICE 不保证存活。
4. 回滚：旧二进制 + 旧 `users` 表，Agent 改回旧账号。

成功标准（与现有公网 ICE 门禁对齐）：两端 Agent 连上 Callout 后的 nats-p2p，REGISTER/CREATE 成功，Admin 同一条连接 `connected:true`，HTTP 响应以 `nats-ice-ok` 开头。

## 9. 代码位置（预期）

| 路径 | 职责 |
| --- | --- |
| `p2p/auth_callout.go`（或同级新文件） | 常量、订 `$SYS.REQ.USER.AUTH`、验密、签 JWT |
| `cmd/nats-p2p-server/main.go` | 先启 auth 再 `StartManager`；校验 issuer 公钥 |
| 现有 `p2p/manager.go` | 仍用 `agent_*` 做 `InProcessConn`，列入 `auth_users` |
| `p2p/*_test.go` | Callout 与回归 |

不修改 `server/*.go`。
