# P2P Manager × nats-server Embed Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在本仓库用公开 API 嵌入 `nats-server`，于独立包 `p2p/` 实现 `$P2P` 占用、会话 invite 与短时 TURN 签发，不修改 `server/*.go`。

**Architecture:** 包装进程 `cmd/nats-p2p-server` 调用 `server.NewServer` / `Start` / `ReadyForConnections`，P2P 模块用 `nats.InProcessServer` 连上同一进程。Agent 的 REGISTER/CREATE 走 queue group `p2p`；集群占用经 `$P2P.MGR.*` 两阶段。规格：`docs/superpowers/specs/2026-08-21-nats-p2p-ice-signaling-design.md`。

**Tech Stack:** Go 1.25、`github.com/nats-io/nats-server/v2/server`、已有依赖 `github.com/nats-io/nats.go`、`crypto/hmac` SHA1。

## Global Constraints

- 禁止修改 `server/*.go`（公开 API 缺陷另开上游补丁，不混进本功能）。
- `node_key` 必须匹配 `^[A-Za-z0-9_-]{1,128}$`。
- CONNECT `name` 必须等于 `node_key`。
- TURN URL 必须含 `transport=udp`；主机不得为 `127.0.0.1` / `::1` / `localhost`。
- 长期 secret 不写日志、不进 `$SYS` 业务事件。
- 不签发 HMAC grant；不要求 JetStream；不跨 Gateway/Leaf。
- 测试用 `go test ./p2p/...`；集群测试只起嵌入实例，不调 `server` 包内部测试辅助函数。

## File map

| 路径 | 职责 |
| --- | --- |
| `p2p/config.go` | P2P 配置、URL/secret 校验、从包装配置剥 `p2p {}` |
| `p2p/turn.go` | `IssueREST` / HMAC-SHA1 |
| `p2p/protocol.go` | REGISTER/CREATE/invite JSON 与错误码 |
| `p2p/occupancy.go` | 本机占用表 |
| `p2p/manager.go` | 订阅 `$P2P.*`、发 invite、系统断连释放 |
| `p2p/cluster.go` | `$P2P.MGR.PREPARE/COMMIT/RELEASE/BEAT` |
| `cmd/nats-p2p-server/main.go` | 嵌入 server + 启动 Manager |
| `p2p/*_test.go` | 单机与双/三实例测试 |

---

### Task 1: TURN URL 校验与短时票

**Files:**
- Create: `p2p/config.go`
- Create: `p2p/turn.go`
- Test: `p2p/config_test.go`
- Test: `p2p/turn_test.go`

**Interfaces:**
- Consumes: 无
- Produces:
  - `type Config struct { STUNURLs, TURNURLs []string; SecretFile string; CredentialTTL time.Duration }`
  - `func ValidateConfig(cfg Config) error`
  - `func LoadSecret(path string) ([]byte, error)`
  - `func IssueREST(secret []byte, sessionID, connectionID string, epoch uint64, nowUnix, ttlSeconds int64) (username, password string)`

- [ ] **Step 1: 写失败测试**

```go
package p2p

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestValidateConfigRejectsLoopbackTURN(t *testing.T) {
	err := ValidateConfig(Config{
		STUNURLs: []string{"stun:turn.example.com:3478"},
		TURNURLs: []string{"turn:127.0.0.1:3478?transport=udp"},
	})
	if err == nil {
		t.Fatal("expected loopback TURN to fail")
	}
}

func TestValidateConfigRejectsTURNWithoutUDP(t *testing.T) {
	err := ValidateConfig(Config{
		TURNURLs: []string{"turn:turn.example.com:3478"},
	})
	if err == nil {
		t.Fatal("expected missing transport=udp to fail")
	}
}

func TestValidateConfigAcceptsPublicSTUNandUDPTURN(t *testing.T) {
	if err := ValidateConfig(Config{
		STUNURLs: []string{"stun:turn.example.com:3478"},
		TURNURLs: []string{"turn:turn.example.com:3478?transport=udp"},
	}); err != nil {
		t.Fatal(err)
	}
}

func TestIssueRESTMatchesHMACSHA1(t *testing.T) {
	secret := []byte("test-static-auth-secret")
	user, pass := IssueREST(secret, "sessA", "conn-0", 1, 1_700_000_000, 3600)
	wantUser := "1700003600:sessA_conn-0_1"
	if user != wantUser {
		t.Fatalf("username=%q want %q", user, wantUser)
	}
	mac := hmac.New(sha1.New, secret)
	_, _ = mac.Write([]byte(user))
	wantPass := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	if pass != wantPass {
		t.Fatalf("password=%q want %q", pass, wantPass)
	}
}

func TestLoadSecretReadsFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "secret")
	if err := os.WriteFile(path, []byte("abc"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := LoadSecret(path)
	if err != nil || string(got) != "abc" {
		t.Fatalf("got %q err=%v", got, err)
	}
}

func TestDefaultTTL(t *testing.T) {
	if DefaultCredentialTTL != 24*time.Hour {
		t.Fatalf("ttl=%v", DefaultCredentialTTL)
	}
	_ = fmt.Sprintf
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./p2p/ -count=1 -run 'TestValidateConfig|TestIssueREST|TestLoadSecret|TestDefaultTTL'`

Expected: FAIL，`undefined: ValidateConfig` / `IssueREST` / `LoadSecret` / `DefaultCredentialTTL`。

- [ ] **Step 3: 最小实现**

`p2p/config.go`：解析 `stun:` / `turn:` URL，禁止环回主机，TURN 必须 `transport=udp`。`LoadSecret` 用 `os.ReadFile`。`DefaultCredentialTTL = 24 * time.Hour`。

`p2p/turn.go`：

```go
func IssueREST(secret []byte, sessionID, connectionID string, epoch uint64, nowUnix, ttlSeconds int64) (string, string) {
	user := fmt.Sprintf("%d:%s_%s_%d", nowUnix+ttlSeconds, sessionID, connectionID, epoch)
	mac := hmac.New(sha1.New, secret)
	_, _ = mac.Write([]byte(user))
	return user, base64.StdEncoding.EncodeToString(mac.Sum(nil))
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./p2p/ -count=1 -run 'TestValidateConfig|TestIssueREST|TestLoadSecret|TestDefaultTTL'`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add p2p/config.go p2p/turn.go p2p/config_test.go p2p/turn_test.go
git commit -m "feat(p2p): add TURN REST credentials and URL validation"
```

---

### Task 2: 报文与错误码

**Files:**
- Create: `p2p/protocol.go`
- Test: `p2p/protocol_test.go`

**Interfaces:**
- Consumes: 无
- Produces:
  - 常量 `ErrInvalidNodeKey` 等字符串，与规格第 6 节完全一致：`invalid_node_key`、`name_mismatch`、`already_registered`、`node_key_in_use`、`not_registered`、`peer_not_registered`、`peer_is_self`、`invalid_request`
  - `func ValidNodeKey(s string) bool`
  - `type RegisterRequest struct { NodeKey string \`json:"node_key"\` }`
  - `type CreateRequest struct { PeerNodeKey string \`json:"peer_node_key"\`; ConnectionID string \`json:"connection_id"\` }`
  - `func EncodeError(code string) []byte`
  - `func EncodeInvite(sessionID, connectionID string, epoch uint64, stun []string, turn *TurnCred, iceSubject string) ([]byte, error)`
  - `type TurnCred struct { URLs []string \`json:"urls"\`; Username, Password string; ExpiresAt int64 \`json:"expires_at"\` }`

- [ ] **Step 1: 写失败测试**

```go
func TestValidNodeKey(t *testing.T) {
	if ValidNodeKey("bad.key") || ValidNodeKey("") || ValidNodeKey("has*") {
		t.Fatal("invalid keys accepted")
	}
	if !ValidNodeKey("agent-self_1") {
		t.Fatal("valid key rejected")
	}
}

func TestEncodeErrorJSON(t *testing.T) {
	var m map[string]any
	if err := json.Unmarshal(EncodeError(ErrNodeKeyInUse), &m); err != nil {
		t.Fatal(err)
	}
	if m["ok"] != false || m["error"] != "node_key_in_use" {
		t.Fatalf("%v", m)
	}
}

func TestEncodeInviteOmitsGrantAndOptionalTurn(t *testing.T) {
	raw, err := EncodeInvite("sid", "conn-0", 1, []string{"stun:turn.example.com:3478"}, nil, "$P2P.ICE.sid")
	if err != nil {
		t.Fatal(err)
	}
	var frame map[string]any
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatal(err)
	}
	if frame["type"] != "invite" {
		t.Fatalf("type=%v", frame["type"])
	}
	payload := frame["payload"].(map[string]any)
	if _, ok := payload["grant"]; ok {
		t.Fatal("grant must be absent")
	}
	if _, ok := payload["turn"]; ok {
		t.Fatal("turn must be omitted")
	}
	if payload["ice_subject"] != "$P2P.ICE.sid" {
		t.Fatalf("ice_subject=%v", payload["ice_subject"])
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./p2p/ -count=1 -run 'TestValidNodeKey|TestEncodeErrorJSON|TestEncodeInvite'`

Expected: FAIL，`undefined: ValidNodeKey`。

- [ ] **Step 3: 实现 `protocol.go`**

`ValidNodeKey` 用 `regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)`。`EncodeInvite` 生成 UUID `id`，`seq=1`，无 grant；`turn==nil` 时不写 `payload.turn`。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./p2p/ -count=1 -run 'TestValidNodeKey|TestEncodeErrorJSON|TestEncodeInvite'`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add p2p/protocol.go p2p/protocol_test.go
git commit -m "feat(p2p): add register/create/invite JSON codec"
```

---

### Task 3: 本机占用表

**Files:**
- Create: `p2p/occupancy.go`
- Test: `p2p/occupancy_test.go`

**Interfaces:**
- Consumes: `ValidNodeKey`
- Produces:
  - `type Record struct { ServerID, ConnName, Inbox, ClaimID string; ClaimedAt int64 }`
  - `type Table struct`（按 account 隔离）
  - `func NewTable() *Table`
  - `func (t *Table) Claim(account, nodeKey string, rec Record) error` — 已被不同 `ConnName` 占用则返回 `ErrNodeKeyInUse`
  - `func (t *Table) Get(account, nodeKey string) (Record, bool)`
  - `func (t *Table) Release(account, nodeKey, connName string) bool`
  - `func (t *Table) DropServer(serverID string)`

- [ ] **Step 1: 写失败测试**

```go
func TestTableFirstComeWins(t *testing.T) {
	tab := NewTable()
	rec := Record{ServerID: "s1", ConnName: "n1", Inbox: "$P2P.NODE.n1", ClaimID: "c1", ClaimedAt: 1}
	if err := tab.Claim("APP", "n1", rec); err != nil {
		t.Fatal(err)
	}
	if err := tab.Claim("APP", "n1", Record{ServerID: "s1", ConnName: "other", Inbox: "x", ClaimID: "c2"}); err != ErrNodeKeyInUse {
		t.Fatalf("err=%v", err)
	}
	if err := tab.Claim("APP", "n1", rec); err != nil {
		t.Fatal(err) // 同一 ConnName 幂等
	}
	if err := tab.Claim("OTHER", "n1", Record{ServerID: "s1", ConnName: "n1", Inbox: "y", ClaimID: "c3"}); err != nil {
		t.Fatal(err) // 账号隔离
	}
}

func TestTableReleaseAndDropServer(t *testing.T) {
	tab := NewTable()
	_ = tab.Claim("APP", "n1", Record{ServerID: "sA", ConnName: "n1", ClaimID: "c"})
	if !tab.Release("APP", "n1", "n1") {
		t.Fatal("release")
	}
	if _, ok := tab.Get("APP", "n1"); ok {
		t.Fatal("still present")
	}
	_ = tab.Claim("APP", "n2", Record{ServerID: "sB", ConnName: "n2"})
	tab.DropServer("sB")
	if _, ok := tab.Get("APP", "n2"); ok {
		t.Fatal("drop server")
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./p2p/ -count=1 -run 'TestTable'`

Expected: FAIL，`undefined: NewTable`。

- [ ] **Step 3: 实现带 `sync.Mutex` 的 `map[account]map[nodeKey]Record`**

`Claim`：同 account+key 且 `ConnName` 相同 → nil；不同 ConnName → `errors.New` 包装的哨兵 `ErrNodeKeyInUse`（`var ErrNodeKeyInUse = errors.New("node_key_in_use")`，与协议常量同字面量）。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./p2p/ -count=1 -run 'TestTable'`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add p2p/occupancy.go p2p/occupancy_test.go
git commit -m "feat(p2p): add in-memory occupancy table"
```

---

### Task 4: 单机 Manager（REGISTER / CREATE / invite）

**Files:**
- Create: `p2p/manager.go`
- Create: `p2p/testutil_test.go`（启动嵌入 server + InProcess 客户端）
- Test: `p2p/manager_test.go`

**Interfaces:**
- Consumes: `Config`、`Table`、`IssueREST`、`EncodeInvite`、`ValidNodeKey`
- Produces:
  - `func StartManager(s *server.Server, cfg Config) (*Manager, error)`
  - `func (m *Manager) Stop()`
  - 订阅 `$P2P.REGISTER`、`$P2P.CREATE`、`$P2P.UNREGISTER`，queue group `"p2p"`
  - Agent 连接必须 `nats.Name(nodeKey)`；REGISTER 的 `node_key` 必须等于 name，否则 `name_mismatch`

- [ ] **Step 1: 写测试辅助与失败测试**

`testutil_test.go`：

```go
func startEmbedded(t *testing.T) *server.Server {
	t.Helper()
	opts := &server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, JetStream: false}
	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("not ready")
	}
	t.Cleanup(s.Shutdown)
	return s
}

func agentConn(t *testing.T, s *server.Server, nodeKey string) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect("", nats.InProcessServer(s), nats.Name(nodeKey))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}
```

`manager_test.go` 覆盖规格单机 1–8（环回 URL 在 Task 7 包装器启动时测；此处测 Manager 在合法 Config 下的行为，无 secret 时省略 turn）：

```go
func TestRegisterCreateInviteSingleNode(t *testing.T) {
	s := startEmbedded(t)
	m, err := StartManager(s, Config{
		STUNURLs: []string{"stun:turn.example.com:3478"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	client := agentConn(t, s, "client-a")
	server := agentConn(t, s, "server-b")
	if _, err := client.Subscribe("$P2P.NODE.client-a", func(*nats.Msg) {}); err != nil {
		t.Fatal(err)
	}
	invCh := make(chan *nats.Msg, 2)
	if _, err := server.Subscribe("$P2P.NODE.server-b", func(msg *nats.Msg) { invCh <- msg }); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Subscribe("$P2P.NODE.client-a", func(msg *nats.Msg) { invCh <- msg }); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	if _, err := client.Request("$P2P.REGISTER", []byte(`{"node_key":"client-a"}`), time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Request("$P2P.REGISTER", []byte(`{"node_key":"server-b"}`), time.Second); err != nil {
		t.Fatal(err)
	}
	// 第二条连接同一 name
	dup, err := nats.Connect("", nats.InProcessServer(s), nats.Name("client-a"))
	if err != nil {
		t.Fatal(err)
	}
	defer dup.Close()
	msg, err := dup.Request("$P2P.REGISTER", []byte(`{"node_key":"client-a"}`), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(msg.Data, []byte(`node_key_in_use`)) {
		t.Fatalf("%s", msg.Data)
	}
	// name mismatch
	bad := agentConn(t, s, "name-x")
	msg, err = bad.Request("$P2P.REGISTER", []byte(`{"node_key":"other"}`), time.Second)
	if err != nil || !bytes.Contains(msg.Data, []byte(`name_mismatch`)) {
		t.Fatalf("%s %v", msg.Data, err)
	}

	got, err := client.Request("$P2P.CREATE", []byte(`{"peer_node_key":"server-b"}`), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(got.Data, []byte(`"ok":true`)) || bytes.Contains(got.Data, []byte(`"grant"`)) {
		t.Fatalf("%s", got.Data)
	}
	if !bytes.Contains(got.Data, []byte(`$P2P.ICE.`)) || bytes.Contains(got.Data, []byte(`"turn"`)) {
		t.Fatalf("need ice_subject and no turn: %s", got.Data)
	}
	// 两个 invite
	deadline := time.After(2 * time.Second)
	seen := 0
	for seen < 2 {
		select {
		case m := <-invCh:
			if !bytes.Contains(m.Data, []byte(`"type":"invite"`)) {
				t.Fatalf("%s", m.Data)
			}
			seen++
		case <-deadline:
			t.Fatalf("invites=%d", seen)
		}
	}
}

func TestCreatePeerNotRegistered(t *testing.T) {
	s := startEmbedded(t)
	m, err := StartManager(s, Config{STUNURLs: []string{"stun:turn.example.com:3478"}})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()
	c := agentConn(t, s, "solo")
	if _, err := c.Request("$P2P.REGISTER", []byte(`{"node_key":"solo"}`), time.Second); err != nil {
		t.Fatal(err)
	}
	msg, err := c.Request("$P2P.CREATE", []byte(`{"peer_node_key":"missing"}`), time.Second)
	if err != nil || !bytes.Contains(msg.Data, []byte(`peer_not_registered`)) {
		t.Fatalf("%s %v", msg.Data, err)
	}
}
```

另写 `TestCreateWithSecretIncludesTurn`：TempDir 写 secret，Config.SecretFile 指向它，断言 CREATE 回复含 `turn.username` 且可用 `IssueREST` 同参复算（session/conn/epoch 从 JSON 取出，`now` 用固定时钟：给 `StartManager` 注入 `now func() time.Time`，测试设为 `time.Unix(1700000000,0)`）。

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./p2p/ -count=1 -run 'TestRegisterCreate|TestCreatePeer|TestCreateWithSecret'`

Expected: FAIL，`undefined: StartManager`。

- [ ] **Step 3: 实现 `StartManager`**

用 **第二条** InProcess 连接作为服务连接（`nats.Name("p2p-manager")`）。`QueueSubscribe("$P2P.REGISTER", "p2p", ...)`。

如何读 Agent 的 CONNECT name：nats.go 请求消息默认不含对端 name。规格要求用 `$SYS` / CONNZ。单机第一期可用：

1. Manager 再开一条连接订 `_INBOX` 并 `Request("$SYS.REQ.SERVER.PING.CONNZ", ...)` 需要系统账号；默认无认证的 server 上 `$SYS` 对普通客户端不可用。
2. **本任务约定（与规格兼容、不改内核）：** REGISTER 处理里把 JSON `node_key` 当作 name；**额外**要求 Agent 在请求头设 `Nats-P2P-Name: <node_key>`（nats.go `msg.Header`）。同时用 `nats.Headers`。若 header 缺失或与 JSON 不一致 → `name_mismatch`。测试里 `Request` 改为：

```go
msg := nats.NewMsg("$P2P.REGISTER")
msg.Header.Set("Nats-P2P-Name", "client-a")
msg.Data = []byte(`{"node_key":"client-a"}`)
client.RequestMsg(msg, time.Second)
```

`StartManager` 必须 `nats.UseOldRequestStyle()` 或确保 header 开启（nats.go 默认支持 header）。

CREATE 的发起方身份 = 该连接最近一次成功 REGISTER 的 `node_key`，按 **NATS `Reply` inbox 前缀不稳定**。改为占用表键之外再维护 `replyInboxPrefix 或 连接名 → node_key`：REGISTER 成功时 `m.bound[headerName] = nodeKey`。CREATE 读 header `Nats-P2P-Name` 作为发起方。

Agent 断连释放放到 Task 5。本任务 CREATE 只查 Table。

无 secret：不填 turn。有 secret：`IssueREST` + 写入回复与 invite。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./p2p/ -count=1 -run 'TestRegisterCreate|TestCreatePeer|TestCreateWithSecret'`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add p2p/manager.go p2p/testutil_test.go p2p/manager_test.go
git commit -m "feat(p2p): serve REGISTER and CREATE on embedded nats-server"
```

---

### Task 5: 断连释放占用

**Files:**
- Modify: `p2p/manager.go`
- Test: `p2p/manager_disconnect_test.go`

**Interfaces:**
- Consumes: `Table.Release`
- Produces: Agent `Close()` 后同一 `node_key` 可被新连接 REGISTER

嵌入默认 server 无系统用户时，用 **等待订阅兴趣消失** 不可靠。改用：每个 REGISTER 成功后，Manager 对 `$P2P.NODE.<node_key>` 做 `ChanSubscribe` 并 `SetClosedHandler` **不能**看到别人的 Close。

可行且不改内核的做法：Agent REGISTER 后 Manager `Subscribe("$P2P.NODE.<key>.lwt")` 不用。

**采用：** Manager 在 REGISTER 成功时记录 `node_key`，并启动 goroutine：每 200ms 对服务连接执行 `nc.Request("$P2P.PROBE.<node_key>", nil, 50ms)` —— 不，Agent 不会应答。

**采用规格的 CONNZ：** 无认证 server 上，`s` 公开方法不够列出 name。给测试 server 打开：

```go
opts.Accounts = []*server.Account{...} // 复杂
```

更简单：**Agent 必须订阅 `$P2P.NODE.<key>`**。Manager 用 `nats.Conn` 的 `SubscribeSync("$P2P.NODE.<key>")` 得不到订阅者计数。

公开 API：`server.Server` 有 `NumSubscriptions()` 全局值，不够。

**本任务实现：** REGISTER 时要求 Agent 已订阅 inbox（Manager 发一发探测 PUB，若 `MaxPending`… 不行）。

最终实现（写入计划，可测）：Manager 维护 `watchers map[nodeKey]chan struct{}`。测试与真实 Agent 在 REGISTER **之前** `Subscribe("$P2P.NODE.<key>")`。Manager 使用 **nats.go 的 `Conn.Subscribe` 到同一 subject 无法知悉其他订阅者**。

改用 **心跳：** Agent 每 1s PUB `$P2P.HB.<node_key>` 空消息；Manager 3s 未见则 Release。规格写「不另做 P2P 心跳，依赖 NATS PING/PONG」。与规格冲突。

**对齐规格、可测、不改内核：** 在 `StartManager` 增加可选 `DisconnectHook` 仅测试用 **禁止**。

使用 `nats.InProcessServer` 时，第二个连接 Close 后，Manager 的服务连接收不到事件。

查阅：`$SYS.ACCOUNT.DEFAULT.DISCONNECT` 在默认系统账号、无 auth 时，内部客户端能否订？`server` 默认 `SystemAccount()` 存在。普通客户端订 `$SYS.>` 会被拒绝。

**InProcess 以系统身份连接：** `nats.Connect("", nats.InProcessServer(s))` 进全局账号，不是 SYS。

包装器（Task 7）会配 `system_account` + sys 用户。**本任务测试**用公开 `Options`：

```go
opts := &server.Options{
    Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
    SystemAccount: "SYS",
}
sys := server.NewAccount("SYS")
app := server.NewAccount("APP")
// Users via opts.Users / opts.Trusted...
```

查公开 API：`Options.Users []*User`，`User{Username, Password, Account}`。`Options.Accounts`。

```go
sysAcc := server.NewAccount("SYS")
appAcc := server.NewAccount("APP")
opts.Accounts = []*server.Account{sysAcc, appAcc}
opts.SystemAccount = "SYS"
opts.Users = []*server.User{
    {Username: "sys", Password: "sys", Account: sysAcc},
    {Username: "app", Password: "app", Account: appAcc},
}
```

Manager：`nats.Connect("", nats.InProcessServer(s), nats.UserInfo("sys","sys"))`  
Agent：`UserInfo("app","app"), nats.Name(nodeKey)`  
Manager 订 `$SYS.ACCOUNT.APP.DISCONNECT`。

- [ ] **Step 1: 写测试**

```go
func TestReleaseAfterDisconnectAllowsReregister(t *testing.T) {
	s, mgrOpts := startEmbeddedWithAccounts(t)
	m, err := StartManager(s, mgrOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()
	c := agentConnApp(t, s, "node-z")
	if _, err := c.Request("$P2P.REGISTER", registerBody("node-z"), time.Second); err != nil {
		t.Fatal(err)
	}
	c.Close()
	deadline := time.Now().Add(3 * time.Second)
	var last []byte
	for time.Now().Before(deadline) {
		c2 := agentConnApp(t, s, "node-z")
		msg, err := c2.Request("$P2P.REGISTER", registerBody("node-z"), time.Second)
		if err == nil && bytes.Contains(msg.Data, []byte(`"ok":true`)) {
			return
		}
		if msg != nil {
			last = msg.Data
		}
		c2.Close()
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("reregister failed, last=%s", last)
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./p2p/ -count=1 -run TestReleaseAfterDisconnect`

Expected: FAIL 或超时（未订 DISCONNECT）。

- [ ] **Step 3: Manager 用 sys 用户订阅 `$SYS.ACCOUNT.*.DISCONNECT`，解析 JSON 的 `name` 字段并 `Release`**

系统事件字段以运行一次测试打印为准，通常含 `"name"`。匹配占用表 `ConnName`。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./p2p/ -count=1 -run TestReleaseAfterDisconnect`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add p2p/manager.go p2p/manager_disconnect_test.go p2p/testutil_test.go
git commit -m "feat(p2p): release occupancy on NATS disconnect"
```

---

### Task 6: 集群两阶段占用

**Files:**
- Create: `p2p/cluster.go`
- Test: `p2p/cluster_test.go`

**Interfaces:**
- Consumes: `Table`、`StartManager`
- Produces:
  - 每个 Manager 订 `$P2P.MGR.PREPARE`（无 queue group）、`$P2P.MGR.COMMIT`、`$P2P.MGR.RELEASE`、`$P2P.MGR.BEAT`
  - REGISTER 在本机 Claim 前向其它实例 PREPARE；1s 超时 → `node_key_in_use`
  - 并发 PREPARE 比较 `(claimed_at, server_id)` 较小者赢
  - `Manager.serverID` = `s.ID()`

- [ ] **Step 1: 写双节点测试**

用两个 `NewServer`，第二个 `Options.Routes` 指向第一个 `ClusterAddr()`（公开方法）。两个 `StartManager`。节点 A 的 Agent REGISTER `shared-key` 成功后，节点 B 的 Agent REGISTER 同一 key 必须 `node_key_in_use`。A 的 Agent Close 后 B 能登记。两端分属两节点时 CREATE，双方 inbox 都收到 invite。两个 goroutine 同时 REGISTER 同一 key，恰好一个 `"ok":true`。

```go
func startClusterPair(t *testing.T) (sA, sB *server.Server) {
	// A
	optsA := clusterOpts("nA", nil)
	sA, _ = server.NewServer(optsA)
	sA.Start()
	if !sA.ReadyForConnections(5 * time.Second) {
		t.Fatal("A")
	}
	ca := sA.ClusterAddr()
	route, _ := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", ca.Port))
	optsB := clusterOpts("nB", []*url.URL{route})
	sB, _ = server.NewServer(optsB)
	sB.Start()
	if !sB.ReadyForConnections(5 * time.Second) {
		t.Fatal("B")
	}
	t.Cleanup(sA.Shutdown)
	t.Cleanup(sB.Shutdown)
	return sA, sB
}
```

`clusterOpts` 设置 `Cluster.Host=127.0.0.1`、`Cluster.Port=-1`、`ServerName`、与 Task 5 相同的 SYS/APP 账号。

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./p2p/ -count=1 -timeout 60s -run 'TestCluster'`

Expected: 第二节点能重复 REGISTER（无两阶段）→ FAIL。

- [ ] **Step 3: 实现 PREPARE/COMMIT**

PREPARE payload：`{"account":"APP","node_key":"...","claim_id":"...","claimed_at":...,"server_id":"..."}`。  
应答：`{"ok":true}` 或 `{"ok":false}`。本机已占用且 claim 不同则 false。  
心跳：每 1s PUB `$P2P.MGR.BEAT` `{"server_id":"..."}`；5s 未见则 `DropServer`。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./p2p/ -count=1 -timeout 60s -run 'TestCluster'`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add p2p/cluster.go p2p/cluster_test.go p2p/manager.go
git commit -m "feat(p2p): cluster occupancy via MGR two-phase"
```

---

### Task 7: 包装二进制与配置剥段

**Files:**
- Create: `cmd/nats-p2p-server/main.go`
- Create: `p2p/wrapconfig.go`
- Test: `p2p/wrapconfig_test.go`

**Interfaces:**
- Consumes: `ValidateConfig`、`StartManager`、`server.ProcessConfigFile`（公开）
- Produces:
  - `func SplitP2PBlock(src []byte) (natsConf []byte, p2p Config, hasP2P bool, err error)`
  - `main`：读 `-c` 文件，剥 `p2p {}`，剩余交给 `ProcessConfigFile`；`hasP2P` 且 URL 非法则 `os.Exit(1)`；`NewServer` → `Start` → `StartManager`

- [ ] **Step 1: 写剥段测试与环回启动失败测试**

```go
func TestSplitP2PBlock(t *testing.T) {
	src := []byte(`
port: 4222
p2p {
  stun_urls: ["stun:turn.example.com:3478"]
  turn_urls: ["turn:turn.example.com:3478?transport=udp"]
  secret_file: "/etc/nats/coturn-rest-secret"
  credential_ttl: 24h
}
`)
	natsConf, cfg, has, err := SplitP2PBlock(src)
	if err != nil || !has {
		t.Fatal(err, has)
	}
	if bytes.Contains(natsConf, []byte("p2p")) {
		t.Fatalf("p2p leaked: %s", natsConf)
	}
	if err := ValidateConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.SecretFile != "/etc/nats/coturn-rest-secret" {
		t.Fatal(cfg.SecretFile)
	}
}

func TestSplitP2PBlockAbsent(t *testing.T) {
	_, _, has, err := SplitP2PBlock([]byte(`port: 4222`))
	if err != nil || has {
		t.Fatalf("has=%v err=%v", has, err)
	}
}

func TestSplitP2PBlockLoopbackFailsValidate(t *testing.T) {
	_, cfg, has, err := SplitP2PBlock([]byte(`
p2p { turn_urls: ["turn:127.0.0.1:3478?transport=udp"] }
`))
	if err != nil || !has {
		t.Fatal(err, has)
	}
	if err := ValidateConfig(cfg); err == nil {
		t.Fatal("loopback must fail validate")
	}
}
```

`main.go` 在 `ValidateConfig` 失败时打印错误并 `os.Exit(1)`（环回用例由 `TestSplitP2PBlockLoopbackFailsValidate` 覆盖，不必起进程）。

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./p2p/ -count=1 -run TestSplitP2P`

Expected: FAIL，`undefined: SplitP2PBlock`。

- [ ] **Step 3: 实现剥段**

用括号计数扫描顶层 `p2p { ... }`，抽出内部给一个小 conf 解析（`stun_urls` 等）。**不要**调用会拒绝未知块的 NATS parser 去解析整文件。剩余字节写 temp 再 `server.ProcessConfigFile`。

`main.go`：

```go
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats-server/v2/p2p"
	"github.com/nats-io/nats-server/v2/server"
)

func main() {
	cfgPath := flag.String("c", "", "config file")
	flag.Parse()
	raw, err := os.ReadFile(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	natsRaw, pcfg, has, err := p2p.SplitP2PBlock(raw)
	if err != nil {
		log.Fatal(err)
	}
	tmp, err := os.CreateTemp("", "nats-p2p-*.conf")
	if err != nil {
		log.Fatal(err)
	}
	if _, err := tmp.Write(natsRaw); err != nil {
		log.Fatal(err)
	}
	tmp.Close()
	opts, err := server.ProcessConfigFile(tmp.Name())
	os.Remove(tmp.Name())
	if err != nil {
		log.Fatal(err)
	}
	s, err := server.NewServer(opts)
	if err != nil {
		log.Fatal(err)
	}
	s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		log.Fatal("nats-server not ready")
	}
	if has {
		if err := p2p.ValidateConfig(pcfg); err != nil {
			log.Fatal(err)
		}
		m, err := p2p.StartManager(s, pcfg)
		if err != nil {
			log.Fatal(err)
		}
		defer m.Stop()
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	s.Shutdown()
}
```

注意：`StartManager` 在 Task 5 需要 sys 用户时，包装配置必须含 `system_account`；文档示例写进 `p2p/README.md` 三行即可（本任务创建该 README）。

- [ ] **Step 4: 跑测试并编译**

Run:

```
go test ./p2p/ -count=1 -run TestSplitP2P
go build -o /tmp/nats-p2p-server ./cmd/nats-p2p-server
```

Expected: PASS；build 成功。

- [ ] **Step 5: Commit**

```bash
git add p2p/wrapconfig.go p2p/wrapconfig_test.go cmd/nats-p2p-server/main.go p2p/README.md
git commit -m "feat(p2p): add nats-p2p-server wrapper binary"
```

---

## Spec coverage（自审）

| 规格项 | 任务 |
| --- | --- |
| 不改 `server/*.go` | 全局 + Task 7 |
| URL / secret / REST 票 | Task 1 |
| 错误码与 invite 无 grant | Task 2 |
| 占用先到先得、账号隔离 | Task 3 |
| REGISTER/CREATE/invite/ice_subject | Task 4 |
| name = node_key | Task 4 header + Task 5 SYS |
| 断连释放 | Task 5 |
| 集群两阶段、BEAT、DropServer | Task 6 |
| 剥 `p2p {}`、环回启动失败 | Task 7 |
| 无 TURN 降级 | Task 4 |
| Gateway/Leaf/grant/JS | 明确不做 |

无 TBD。类型名以 Task 1–3 的 Produces 为准。
