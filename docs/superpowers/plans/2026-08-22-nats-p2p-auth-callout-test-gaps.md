# Auth Callout P0/P1 补充测试 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 用 TCP/`ProcessConfigFile` 夹具和一条不启停公网进程的探测脚本，把 Callout 的配置抄错、JWT 越权、无 sys 占用、双节点各答各的路径锁进 CI。

**Architecture:** 测试只加在 `p2p/*_test.go` 与 raypx2 探测脚本。生产代码仅在测试证明包装进程漏调现有函数时才改。客户端一律 `server.ClientURL()`，禁止再只靠 `InProcessServer` 冒充公网 CONNECT。

**Tech Stack:** Go `testing` + `nats.go` + `server.ProcessConfigFile`；bash/python3 探测脚本。

## Global Constraints

- 文档、`--help`、测试失败信息不得打印 `AgentPassword`、`IssuerSeed`、TURN secret。
- 不改 `server/*.go`，不加 `auth {}`。
- 探测脚本不得 ssh 启停 nats-p2p / coturn。
- 内部测试账号可用 `auth-internal` / `p2p-internal`（已在现有测试中）。
- Agent 凭据只用 `AgentUser` / `AgentPassword` 常量，不要把字面量抄进新文档。

---

### Task 1: TCP conf 夹具 + P0 CONNECT / 权限回归 / `$SYS`

**Files:**
- Create: `p2p/auth_callout_tcp_test.go`

**Interfaces:**
- Consumes: `IssuerPublic()`, `StartAuthCallout`, `StartManager`, `AgentUser`, `AgentPassword`, `AuthInternalName`, `CheckIssuer`
- Produces: `startTCPCalloutFromConf(t, conf) (*server.Server, *AuthCalloutService, *Manager)`；`connectTCP(t, s, user, pass, name) (*nats.Conn, error)`

- [ ] **Step 1: 写夹具与 P0-1..P0-4、P1-5、P1-6 测试**

`startTCPCalloutFromConf`：把 conf 写入 `t.TempDir()`，`ProcessConfigFile`，`NewServer`/`Start`/`ReadyForConnections`，`CheckIssuer`+`AuthInternalCredentials`+`StartAuthCallout`，再 `StartManager`（`AgentUsername: "p2p-internal"`）。`connectTCP` 使用 `s.ClientURL()`。

必含测试名：
- `TestTCPCalloutAcceptsAgentRejectsBadAndEmpty`
- `TestTCPCalloutRestrictedInboxPermsTimesOut`
- `TestTCPCalloutDeniesSysAndWildcard`
- `TestTCPCalloutStopsRejectingNewConnects`
- `TestCalloutStartFailsClosed`

- [ ] **Step 2: 跑这些测试**

Run: `go test ./p2p/ -count=1 -timeout 60s -run 'TestTCPCallout|TestCalloutStartFailsClosed'`

Expected: P0-3（过窄 permissions）必须失败 CONNECT；其余按规格。若 P0-4 内核未拒 `$SYS`，测试失败并停下来报告，不要放宽断言。

---

### Task 2: P0 CREATE+TURN 与无 sys 占用

**Files:**
- Modify: `p2p/auth_callout_tcp_test.go`

- [ ] **Step 1: 追加**

- `TestTCPCalloutCreateIncludesTurn`：临时 secret + `turn:turn.example.com:3478?transport=udp`，两端 REGISTER 后 CREATE 含 `turn.username` 与 `stun_urls`。
- `TestTCPCalloutHardDisconnectWithoutSysKeepsOccupancy`：无 `sys_*`，REGISTER、`Close`、立即同 key REGISTER → `"ok":true`（connz 回收）。

- [ ] **Step 2: 跑**

Run: `go test ./p2p/ -count=1 -timeout 60s -run 'TestTCPCalloutCreate|TestTCPCalloutHardDisconnect'`

---

### Task 3: P1 双节点 Callout

**Files:**
- Create: `p2p/auth_callout_cluster_test.go`

- [ ] **Step 1: 写** `startCalloutClusterPair`（两节点 `AuthCallout` + 各 `StartAuthCallout`，**无** queue group，manager 不设 `AgentAccount`）以及：

- `TestCalloutClusterCreateCrossNode`
- `TestCalloutClusterPeerMayAnswerButAllStoppedRejects`

- [ ] **Step 2: 跑**

Run: `go test ./p2p/ -count=1 -timeout 60s -run 'TestCalloutCluster'`

Expected: 跨节点 CREATE + 两 invite；停 A 的 auth 后连 A 失败、连 B 成功。

---

### Task 4: P1 同密码多 key / 残留 APP

**Files:**
- Modify: `p2p/auth_callout_tcp_test.go`

- [ ] **Step 1: 追加**

- `TestTCPCalloutTwoNodeKeysShareCreds`
- `TestTCPCalloutDuplicateNodeKeyInUse`
- `TestTCPCalloutLeftoverAppAccountStaysOnGlobal`

残留 APP：conf 含 `accounts { APP { users: [{user: leftover, password: leftover}] } }` 与 `auth_callout`。若解析失败，断言 error 非空；若能启动，REGISTER/CREATE 必须在 `$G` 成功。

- [ ] **Step 2: 跑** `go test ./p2p/ -count=1 -timeout 90s`

---

### Task 5: raypx2 探测脚本

**Files:**
- Create: `~/src/raypx2/scripts/probe-nats-auth-callout.sh`
- Create: `~/src/raypx2/tests/scripts/test_nats_auth_callout_probe.py`
- Modify: `~/src/raypx2/docs/nats-ice-signaling_cn.md`（追加一条探测命令，不写密码）

- [ ] **Step 1: 脚本对 `NATS_URL` 做四次 CONNECT**（旧 `app/app`、空密码、错密码、环境变量凭据）。实现可用 python3 短探针，禁止 `nats-p2p-server -c` / `systemctl`。

- [ ] **Step 2: 契约测试** — help 无 password/secret；不出现 `nats-p2p-server -c`；使用 `NATS_P2P_USER`/`NATS_P2P_PASSWORD`。

Run: `python3 -m unittest tests.scripts.test_nats_auth_callout_probe -v`

---

### Task 6: 全量回归

Run: `go test ./p2p/ -count=1 -timeout 120s`

Expected: 全部通过。若仅占用/混配账号失败，把失败信息写进测试注释与本计划「发现」节，不要删断言。
