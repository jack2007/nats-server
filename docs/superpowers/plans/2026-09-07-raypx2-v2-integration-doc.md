# raypx2 V2 Integration Documentation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the obsolete V1 raypx2 integration guide with a self-contained Chinese guide that exactly describes the current P2P V2 implementation.

**Architecture:** Rewrite the single user-facing document from the V2 protocol outward: wire envelope and subjects first, then the session/connection state machine, cluster reliability, authorization, configuration, observability, and a client checklist. Treat the Go implementation and focused tests as authoritative; use `p2p/README.md` only as a cross-check.

**Tech Stack:** Markdown, Go source and tests, NATS subjects and JSON protocol examples, `rg`, `go test`.

## Global Constraints

- Modify only `docs/raypx2-integrate.md` as user-facing project documentation.
- Describe V2 only; do not retain a V1 compatibility or migration section.
- Never copy embedded credentials, issuer seeds, TURN secrets, or other sensitive constants from source.
- Prefix every shell command with `rtk` as required by `/home/jack/.codex/RTK.md`.

---

### Task 1: Rewrite the raypx2 integration guide for V2

**Files:**
- Modify: `docs/raypx2-integrate.md`
- Reference: `p2p/protocol_v2.go`
- Reference: `p2p/store_v2.go`
- Reference: `p2p/signal_v2.go`
- Reference: `p2p/manager_v2.go`
- Reference: `p2p/cluster.go`
- Reference: `p2p/monitor_v2.go`
- Reference: `p2p/config.go`
- Reference: `p2p/auth_callout.go`
- Reference: `cmd/nats-p2p-server/main.go`
- Test: `p2p/protocol_v2_test.go`
- Test: `p2p/store_v2_test.go`
- Test: `p2p/manager_v2_test.go`
- Test: `p2p/signal_v2_test.go`
- Test: `p2p/cluster_v2_test.go`
- Test: `cmd/nats-p2p-server/main_test.go`

**Interfaces:**
- Consumes: P2P V2 wire subjects, strict JSON envelope, lifecycle state machine, authorization grants, cluster behavior, configuration, and STATSZ fields defined by the reference files.
- Produces: A standalone Chinese integration contract for raypx2 developers and operators at `docs/raypx2-integrate.md`.

- [ ] **Step 1: Record the stale-document baseline**

Run:

```bash
rtk rg -n '\$P2P\.(REGISTER|CREATE|UNREGISTER|NODE|ICE)(\.|`|\s|$)|Nats-P2P-Name|invite|第一期|V1' docs/raypx2-integrate.md
```

Expected: matches in the current document, demonstrating that the V1 contract is still present and that the final legacy scan must fail until the rewrite is applied.

- [ ] **Step 2: Replace the document with the V2 structure and verified facts**

Rewrite `docs/raypx2-integrate.md` in Chinese with these exact top-level sections:

```markdown
# raypx2 × nats-p2p V2 集成说明

## 1. 架构与职责边界
## 2. 身份、注册与权限
## 3. V2 线协议
## 4. 会话与连接生命周期
## 5. 信令可靠性
## 6. 集群与故障语义
## 7. Auth Callout
## 8. 配置与启动
## 9. 监控与排障
## 10. raypx2 接入清单
## 11. 代码索引
```

The content must state and exemplify all of the following:

- The wrapper always calls `ValidateConfig` and `StartManager` after the embedded NATS server and optional Auth Callout are ready; an empty `p2p {}` block still starts V2.
- Agents use `CONNECT name = node_key`. A node key matches `[A-Za-z0-9_-]{1,128}`; raypx2 may continue to use its persisted 16-hex convention.
- A registration ID is 32 lowercase hex characters. A request ID and message ID are lowercase UUIDs. `connection_id` matches `conn-(0|[1-9][0-9]{0,9})`.
- The maximum frame is 256 KiB, SDP is 128 KiB, and a candidate is 8 KiB.
- Every frame uses the strict envelope shown below; unknown JSON fields are rejected:

```json
{
  "version": 2,
  "request_id": "11111111-1111-4111-8111-111111111111",
  "message_id": "22222222-2222-4222-8222-222222222222",
  "registration_id": "0123456789abcdef0123456789abcdef",
  "payload": {}
}
```

- Document this external subject table exactly:

```text
$P2P.V2.CMD.<node-key>.REGISTER.<server-id>
$P2P.V2.CMD.<node-key>.SESSION.CREATE
$P2P.V2.CMD.<node-key>.SESSION.COMMAND
$P2P.V2.CMD.<node-key>.CONNECTION.COMMAND
$P2P.V2.CMD.<node-key>.SIGNAL.SEND
$P2P.V2.CMD.<node-key>.SIGNAL.ACK
$P2P.V2.EVENT.<node-key>.<registration-id>
```

- Explain that REGISTER is connection-local and addressed to the connected server's public server ID; after its reply, the agent subscribes to its registration-scoped EVENT subject.
- Show the successful path `REGISTER → SESSION.CREATE → ALLOCATED → SESSION.COMMAND BIND → PREPARE → CONNECTION.COMMAND READY → START` with concrete JSON requests/replies/events.
- Explain session states `allocated`, `preparing`, `active`, `draining`, `closed`, `failed` and connection states `allocated`, `preparing`, `active`, `restarting`, `closed`.
- Explain `SESSION.COMMAND` values `BIND`, `RESUME`, `CLOSE`; `CONNECTION.COMMAND` values `OPEN`, `RESTART`, `BIND`, `READY`, `REJECT`, `CLOSE`.
- Explain that a session begins with `conn-0`, epoch 1; OPEN allocates another connection, RESTART increments its epoch, and each session is limited to 128 connections.
- Explain optimistic `revision` checks, request idempotency by `(sender_node_key, request_id)`, message deduplication, monotonically increasing per-direction `seq`, cumulative `ack_seq`, coordinator retry until ACK/deadline, and `SIGNAL.SEND` payload types `description`, `candidate`, `end_of_candidates`.
- Explain queue-group command handling, owner forwarding, owner/replica synchronization, startup catch-up before joining the external command queue, and `coordinator_unavailable` without owner takeover.
- Explain the default 10-second setup timeout, 15-second disconnect grace adjusted from client ping settings, 60-second tombstone baseline, `busy` with `retry_after_ms` between 100 and 1000, and owner-loss behavior for negotiating versus active sessions.
- List every code-defined error: `invalid_request`, `not_registered`, `peer_not_registered`, `not_session_member`, `session_not_found`, `connection_not_found`, `connection_limit`, `stale_epoch`, `future_epoch`, `sequence_gap`, `stale_revision`, `invalid_state`, `setup_timeout`, `busy`, `coordinator_unavailable`, `internal_error`.
- Explain node-scoped Auth Callout permissions (`$P2P.V2.CMD.<node-key>.>`, `$P2P.V2.EVENT.<node-key>.>`, `_INBOX.>`) and explicitly deny manager subjects and other nodes' subjects.
- Retain valid wrapper/configuration guidance, but remove V1-specific conditions such as starting the manager only when `p2p {}` exists.
- Describe STUN/TURN URL validation, TURN REST credentials, system-account disconnect tracking, default client ping values 5 seconds / 3, and the existing sample configuration filenames without exposing credential values.
- Add the STATSZ request subject `$SYS.REQ.SERVER.<server-id>.P2P.V2.STATSZ` and describe the counter/gauge field names from `StatsSnapshotV2`.
- End with a concrete raypx2 implementation checklist and a source-file index.

- [ ] **Step 3: Prove obsolete V1 protocol content is gone**

Run:

```bash
if rtk rg -n '\$P2P\.(REGISTER|CREATE|UNREGISTER|NODE|ICE)(\.|`|\s|$)|Nats-P2P-Name|第一期|V1' docs/raypx2-integrate.md; then exit 1; fi
```

Expected: no matches and exit code 0.

- [ ] **Step 4: Check required V2 coverage mechanically**

Run:

```bash
rtk rg -n '\$P2P\.V2\.CMD|\$P2P\.V2\.EVENT|REGISTER|SESSION\.CREATE|SESSION\.COMMAND|CONNECTION\.COMMAND|SIGNAL\.SEND|SIGNAL\.ACK|ALLOCATED|PREPARE|START|registration_id|request_id|message_id|revision|ack_seq|coordinator_unavailable|connection_limit|STATSZ' docs/raypx2-integrate.md
```

Expected: every listed concept has at least one meaningful match in the rewritten guide.

- [ ] **Step 5: Run focused executable verification**

Run:

```bash
rtk go test ./p2p ./cmd/nats-p2p-server
```

Expected: both packages report `ok` and the command exits 0.

- [ ] **Step 6: Validate formatting and scope**

Run:

```bash
rtk git diff --check
rtk git status --short
rtk git diff -- docs/raypx2-integrate.md
```

Expected: `git diff --check` exits 0; the status contains only the planned integration guide plus already committed workflow documents; the diff is a full V2 rewrite with no sensitive constants.

- [ ] **Step 7: Commit the documentation rewrite**

Run:

```bash
rtk git add docs/raypx2-integrate.md
rtk git commit -m "docs: update raypx2 integration guide for V2"
```

Expected: one commit containing only `docs/raypx2-integrate.md`.
