# nats-p2p Auth Callout 补充测试设计

日期：2026-08-22  
状态：历史 V1 测试基线；Callout 管道负例继续有效，V1 subject 权限、REGISTER/CREATE
和 invite 断言将在 V2 实施时由 raypx2 仓库的
`docs/superpowers/specs/2026-08-28-nats-p2p-v2-multi-session-design.md` 对应测试取代。
主规格：`docs/superpowers/specs/2026-08-22-nats-p2p-auth-callout-design.md`  
对照：公网切 Callout 时 `auth-internal` 过窄 `permissions` 导致全部 CONNECT 在 `authorization.timeout` 后失败（回包 subject 为 `$SYS._INBOX.*`）。

## 1. 目标

现有 `go test ./p2p` 的 Callout 用例全部走 `InProcessServer`，且内部用户无 `permissions`。公网 ICE direct/relay 只覆盖正确凭据的 happy path。本规格补上能复现「配置抄错 / JWT 越权 / 无 sys 占用 / 集群各答各的」的自动化，以及一条**不启停**公网进程的负例探测脚本。

成功标准：`go test ./p2p/` 覆盖下表 P0+P1；探测脚本契约测试通过；文档与帮助不出现临时 Agent 密码、issuer 种子、TURN secret。

## 2. 非目标

- 不改 `server/*.go`，不加顶层 `auth {}`。
- 不把本轮改成「给 Manager 补 sys 释放」（占用泄漏只**测出来**，不在本规格修产品）。
- 不把 cluster 旧用例从 `app/app`+`APP` 整表迁移到 Callout。
- 不启停公网 `nats-p2p` / coturn，不加进 `run-nats-ice-dgx-public.sh` 的默认门禁。
- 不做 k6 / 长 soak / 公网 ICE 负例全链路。

## 3. 已覆盖 vs 本轮

| 已有 | 本轮必须补 |
| --- | --- |
| InProcess 错密码拒、对密码 REGISTER、`$P2P.MGR` 异步拒 | **TCP** + `ProcessConfigFile` 的 CONNECT 对/错/空 |
| JWT allow 列表字符串 | 连上后内核拒绝 `$SYS.>` / `$SYS.REQ.SERVER.PING` / `>` |
| `TestCreateWithSecretIncludesTurn`（无 Callout） | Callout JWT 下 CREATE，invite 带 STUN/TURN |
| `TestReleaseAfterDisconnectAllowsReregister`（有 sys） | **无 sys** 硬断开后占用仍在 |
| cluster `app/app` + `APP` | 两节点各 `StartAuthCallout`（无 queue group），跨节点 CREATE；停 A 的 auth 后只有 A 上 CONNECT 失败 |
| `CheckIssuer` 纯函数 | 从 conf 解析出的 issuer 不一致则拒绝；`StartAuthCallout` 内部密码错则失败；`auth.Stop` 后新 CONNECT 失败 |
| 公网 ICE happy path | 探测脚本：错密码/空密码失败，正确凭据成功；不启停远端 |

## 4. P0 用例

夹具：临时 conf → `ProcessConfigFile` → `NewServer` 听 `127.0.0.1:随机端口` → `StartAuthCallout` + `StartManager`（`agent_username=p2p-internal`，**不**设 `agent_account`，JWT/`$G`）。客户端用 `nats.Connect(server.ClientURL(), UserInfo(...))`，禁止 `InProcessServer`。

| ID | 名称 | 断言 |
| --- | --- | --- |
| P0-1 | TCP 正确凭据 | 代码常量 CONNECT 成功 |
| P0-2 | TCP 错密码 / 空 user / 空 password / 仅 token | 均为 `Authorization Violation`（或等价 CONNECT 失败） |
| P0-3 | 过窄 permissions 回归 | `auth-internal` 仅 `publish: ["_INBOX.>"]`（或仅订 `$SYS.REQ.USER.AUTH`）时，**正确凭据**也在 timeout 内失败 |
| P0-4 | `$SYS` / `>` 拒绝 | CONNECT 成功后 `Request("$SYS.REQ.SERVER.PING")` 失败；`Publish("$P2P.MGR.PREPARE")` 权限错；`Subscribe(">")` 失败或异步权限错 |
| P0-5 | CREATE + TURN | 临时 `secret_file` + 公网形态 TURN URL；REGISTER 两端后 CREATE `ok`，body 含 `stun_urls` 与 `turn.username`；测试日志不打印 turn password |
| P0-6 | 无 sys 硬断开 | 不配 `sys_*`；REGISTER 后 `Close`；立即同 `node_key` 再 REGISTER 必须成功（`handleRegister` 用 connz count 回收，不依赖 `$SYS` DISCONNECT） |

## 5. P1 用例

| ID | 名称 | 断言 |
| --- | --- | --- |
| P1-1 | 双节点本地 Callout | 两嵌入 server 组 cluster，各启 auth（**无** queue group）+ manager；Agent 分别连 A/B，跨节点 CREATE + 双侧 invite |
| P1-2 | 全停才拒 | 只停 A 时，集群可能把 `$SYS.REQ.USER.AUTH` 交给 B（v1 各节点同一套常量，代答结果相同）。**两边 auth 都停**后，连 A、连 B 都必须失败 |
| P1-3 | 同密码不同 key | 两连接同一对常量、不同 `Name`，双方 REGISTER `ok` |
| P1-4 | 同 key 双开 | 第二连接 REGISTER → `node_key_in_use` |
| P1-5 | auth 停止 | 单节点 `auth.Stop()` 后新 CONNECT 失败（server 仍听） |
| P1-6 | 启动闭合 | conf issuer ≠ `IssuerPublic()` → `CheckIssuer` 错；`Users` 无 `auth-internal` → `AuthInternalCredentials` 错；内部密码错 → `StartAuthCallout` 错 |
| P1-7 | 残留 APP 账号 | conf 同时有 `accounts { APP }` 与 `auth_callout`、**不**设 `agent_account` 时：Agent JWT 仍进 `$G`，REGISTER/CREATE 成功。若 `ProcessConfigFile` 直接拒绝该混配，则断言启动失败（不得静默占错表） |

## 6. 探测脚本（raypx2）

路径：`scripts/probe-nats-auth-callout.sh`。

- 默认 `NATS_URL=nats://64.176.42.49:4222`，凭据来自 `NATS_P2P_USER` / `NATS_P2P_PASSWORD`（与公网 ICE 脚本同一对默认值）。
- **禁止** ssh 启停 `nats-p2p-server` / coturn。
- 对目标依次：旧共享账号、空密码、错误密码 → 必须失败；代码常量 → 必须成功。
- stdout 只报 `ok`/`fail` 与类别，不回显密码。`--help` 不含 `password`/`secret` 字面量。
- 契约：`tests/scripts/test_nats_auth_callout_probe.py`。

## 7. 代码位置

| 路径 | 职责 |
| --- | --- |
| `p2p/auth_callout_tcp_test.go` | P0 夹具与用例、P1-3..P1-7 |
| `p2p/auth_callout_cluster_test.go` | P1-1、P1-2 |
| `~/src/raypx2/scripts/probe-nats-auth-callout.sh` | 公网/任意 URL 负例探测 |
| `~/src/raypx2/tests/scripts/test_nats_auth_callout_probe.py` | 脚本契约 |

不修改 `cmd/nats-p2p-server/main.go`，除非某条 P1-6 证明包装进程漏了已有函数调用（本轮以测现有 `CheckIssuer` / `AuthInternalCredentials` / `StartAuthCallout` 为准）。
