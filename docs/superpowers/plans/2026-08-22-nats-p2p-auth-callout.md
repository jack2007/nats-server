# nats-p2p Auth Callout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 `nats-p2p-server` 同进程内用官方 Auth Callout 验 CONNECT：代码常量核对临时用户名密码，回签仅含 `$P2P` 业务 subject + `_INBOX.>` 的 user JWT。

**Architecture:** 不改 `server/*.go`。`p2p` 包新增 auth 模块，`InProcessConn` 订 `$SYS.REQ.USER.AUTH`。包装进程在 `ReadyForConnections` 之后、`StartManager` 之前启动 auth。无 `auth {}` 配置块；issuer 种子与 Agent 用户名密码写在代码常量。`authorization.auth_callout` 仍走标准 nats conf。

**Tech Stack:** Go 1.25、`github.com/nats-io/nats.go`、`github.com/nats-io/jwt/v2`、`github.com/nats-io/nkeys`（`go.mod` 已有）。

**规格：** `docs/superpowers/specs/2026-08-22-nats-p2p-auth-callout-design.md`  
**Agent 计划：** `~/src/raypx2/docs/superpowers/plans/2026-08-22-nats-auth-callout.md`（本计划合并进 master 后再做）

## Global Constraints

- 禁止修改 `server/*.go`。
- 禁止新增顶层 `auth { allowed_user / allowed_password / issuer_seed_file }`。
- 临时 Agent 用户名密码、issuer 种子只出现在 `p2p/auth_callout.go` 常量中；不要写进 README、示例 conf 注释、日志或 `$SYS` 业务事件。
- Agent↔NATS 不升 TLS，不开 xkey。
- 进程内 `auth-internal` / `p2p-internal` 必须列入 `auth_users`。
- JWT publish：`$P2P.REGISTER`、`$P2P.CREATE`、`$P2P.UNREGISTER`、`$P2P.ICE.>`。
- JWT subscribe：`$P2P.NODE.>`、`$P2P.ICE.>`、`_INBOX.>`（request-reply 必需）。
- 不给 `$P2P.MGR.>`、`$SYS.>`、`>`。
- 无 `auth_callout` 时不启 auth 协程（旧 conf 兼容）。
- 测试：`go test ./p2p/...`；不要调用 `server` 包测试辅助函数。
- 现有无 Callout 的 `startEmbedded` 测试必须继续通过。

## File map

| 路径 | 职责 |
| --- | --- |
| `p2p/auth_callout.go` | 常量、验密、签 JWT、`StartAuthCallout` |
| `p2p/auth_callout_test.go` | 纯函数 + 嵌入式 Callout 集成 |
| `cmd/nats-p2p-server/main.go` | issuer 校验 → 先 auth 再 Manager |
| `p2p/README.md` | 如何开 Callout（不写 Agent 密码） |

---

### Task 1: 验密与 JWT 纯函数

**Files:**
- Create: `p2p/auth_callout.go`
- Test: `p2p/auth_callout_test.go`

**Interfaces:**
- Consumes: 无
- Produces:
  - `const AgentUser`、`AgentPassword`、`IssuerSeed`、`AuthInternalName = "auth-internal"`
  - `func IssuerPublic() string`
  - `func CheckIssuer(public string) error`
  - `func MatchAgentCreds(user, password string) bool`
  - `func EncodeAgentUserJWT(userNkey string) (string, error)`
  - `func EncodeAuthResponse(userNkey, serverID, userJWT, errMsg string) ([]byte, error)`

- [ ] **Step 1: 写失败测试**

```go
package p2p

import (
	"strings"
	"testing"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

func TestMatchAgentCreds(t *testing.T) {
	if !MatchAgentCreds(AgentUser, AgentPassword) {
		t.Fatal("expected match")
	}
	if MatchAgentCreds(AgentUser, "wrong") {
		t.Fatal("wrong password")
	}
	if MatchAgentCreds("", AgentPassword) {
		t.Fatal("empty user")
	}
	if MatchAgentCreds(AgentUser, "") {
		t.Fatal("empty password")
	}
}

func TestCheckIssuerMatchesSeed(t *testing.T) {
	if err := CheckIssuer(IssuerPublic()); err != nil {
		t.Fatal(err)
	}
	if err := CheckIssuer("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"); err == nil {
		t.Fatal("expected mismatch")
	}
}

func TestEncodeAgentUserJWTPermissions(t *testing.T) {
	kp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := kp.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeAgentUserJWT(pub)
	if err != nil {
		t.Fatal(err)
	}
	uc, err := jwt.DecodeUserClaims(raw)
	if err != nil {
		t.Fatal(err)
	}
	if uc.Subject != pub {
		t.Fatalf("sub %s", uc.Subject)
	}
	if uc.Audience != "$G" {
		t.Fatalf("aud %s", uc.Audience)
	}
	allowPub := strings.Join(uc.Pub.Allow, ",")
	allowSub := strings.Join(uc.Sub.Allow, ",")
	for _, s := range []string{"$P2P.REGISTER", "$P2P.CREATE", "$P2P.UNREGISTER", "$P2P.ICE.>"} {
		if !strings.Contains(allowPub, s) {
			t.Fatalf("missing pub %s in %s", s, allowPub)
		}
	}
	for _, s := range []string{"$P2P.NODE.>", "$P2P.ICE.>", "_INBOX.>"} {
		if !strings.Contains(allowSub, s) {
			t.Fatalf("missing sub %s in %s", s, allowSub)
		}
	}
	if strings.Contains(allowPub, ">") && allowPub == ">" {
		t.Fatal("pub must not be >")
	}
}

func TestEncodeAuthResponseError(t *testing.T) {
	kp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := kp.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeAuthResponse(pub, "TEST-SERVER", "", "authorization denied")
	if err != nil {
		t.Fatal(err)
	}
	rc, err := jwt.DecodeAuthorizationResponseClaims(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	if rc.Error == "" {
		t.Fatal("expected error")
	}
	if rc.Jwt != "" {
		t.Fatal("error response must not carry user jwt")
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./p2p/ -count=1 -run 'TestMatchAgentCreds|TestCheckIssuerMatchesSeed|TestEncodeAgentUserJWTPermissions|TestEncodeAuthResponseError' -v`

Expected: FAIL，未定义 `MatchAgentCreds` 等。

- [ ] **Step 3: 最小实现**

`p2p/auth_callout.go`（issuer 种子复用 nats-server 自身 `auth_callout_test.go` 里那把公开测试账号种子，公钥必须与 `IssuerPublic()` 一致）：

```go
package p2p

import (
	"crypto/subtle"
	"fmt"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

const (
	AgentUser        = "raypx2"
	AgentPassword    = "raypx2_2026"
	IssuerSeed       = "SAANDLKMXL6CUS3CP52WIXBEDN6YJ545GDKC65U5JZPPV6WH6ESWUA6YAI"
	AuthInternalName = "auth-internal"
	agentJWTTTL      = 4 * time.Hour
)

func issuerKeyPair() (nkeys.KeyPair, error) {
	return nkeys.FromSeed([]byte(IssuerSeed))
}

func IssuerPublic() string {
	kp, err := issuerKeyPair()
	if err != nil {
		panic(err)
	}
	pub, err := kp.PublicKey()
	if err != nil {
		panic(err)
	}
	return pub
}

func CheckIssuer(public string) error {
	want := IssuerPublic()
	if public != want {
		return fmt.Errorf("auth_callout issuer does not match embedded seed")
	}
	return nil
}

func MatchAgentCreds(user, password string) bool {
	if len(user) != len(AgentUser) || len(password) != len(AgentPassword) {
		return false
	}
	u := subtle.ConstantTimeCompare([]byte(user), []byte(AgentUser))
	p := subtle.ConstantTimeCompare([]byte(password), []byte(AgentPassword))
	return u == 1 && p == 1
}

func EncodeAgentUserJWT(userNkey string) (string, error) {
	kp, err := issuerKeyPair()
	if err != nil {
		return "", err
	}
	uc := jwt.NewUserClaims(userNkey)
	uc.Audience = "$G"
	uc.Expires = time.Now().Add(agentJWTTTL).Unix()
	uc.Pub.Allow.Add("$P2P.REGISTER")
	uc.Pub.Allow.Add("$P2P.CREATE")
	uc.Pub.Allow.Add("$P2P.UNREGISTER")
	uc.Pub.Allow.Add("$P2P.ICE.>")
	uc.Sub.Allow.Add("$P2P.NODE.>")
	uc.Sub.Allow.Add("$P2P.ICE.>")
	uc.Sub.Allow.Add("_INBOX.>")
	return uc.Encode(kp)
}

func EncodeAuthResponse(userNkey, serverID, userJWT, errMsg string) ([]byte, error) {
	kp, err := issuerKeyPair()
	if err != nil {
		return nil, err
	}
	cr := jwt.NewAuthorizationResponseClaims(userNkey)
	cr.Audience = serverID
	cr.Error = errMsg
	cr.Jwt = userJWT
	tok, err := cr.Encode(kp)
	if err != nil {
		return nil, err
	}
	return []byte(tok), nil
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./p2p/ -count=1 -run 'TestMatchAgentCreds|TestCheckIssuerMatchesSeed|TestEncodeAgentUserJWTPermissions|TestEncodeAuthResponseError' -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add p2p/auth_callout.go p2p/auth_callout_test.go
git commit -m "$(cat <<'EOF'
Add in-process auth callout JWT helpers.

EOF
)"
```

---

### Task 2: InProcessConn 订阅并应答 Callout

**Files:**
- Modify: `p2p/auth_callout.go`（追加 `StartAuthCallout` / `Stop` / `AuthInternalCredentials`）
- Test: `p2p/auth_callout_test.go`

**Interfaces:**
- Consumes: Task 1 的编码函数与常量
- Produces:
  - `type AuthCalloutService struct`
  - `func StartAuthCallout(s *server.Server, username, password string) (*AuthCalloutService, error)`
  - `func (a *AuthCalloutService) Stop()`
  - `func AuthInternalCredentials(opts *server.Options) (user, pass string, err error)`

- [ ] **Step 1: 写失败测试**

追加到 `p2p/auth_callout_test.go`：

```go
import (
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats-server/v2/server"
)

func startEmbeddedCallout(t *testing.T) *server.Server {
	t.Helper()
	opts := &server.Options{
		Host:        "127.0.0.1",
		Port:        -1,
		NoLog:       true,
		NoSigs:      true,
		AuthTimeout: 2.0,
		Users: []*server.User{
			{Username: AuthInternalName, Password: "auth-internal"},
			{Username: "p2p-internal", Password: "p2p-internal"},
		},
		AuthCallout: &server.AuthCallout{
			Issuer:    IssuerPublic(),
			AuthUsers: []string{AuthInternalName, "p2p-internal"},
		},
	}
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

func TestAuthCalloutRejectsWrongPassword(t *testing.T) {
	s := startEmbeddedCallout(t)
	svc, err := StartAuthCallout(s, AuthInternalName, "auth-internal")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(svc.Stop)
	_, err = nats.Connect("", nats.InProcessServer(s), nats.UserInfo(AgentUser, "wrong"), nats.Name("client-a"))
	if err == nil {
		t.Fatal("expected authorization failure")
	}
}

func TestAuthCalloutAcceptsAgentAndBlocksMgr(t *testing.T) {
	s := startEmbeddedCallout(t)
	svc, err := StartAuthCallout(s, AuthInternalName, "auth-internal")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(svc.Stop)
	m, err := StartManager(s, Config{
		STUNURLs:      []string{"stun:turn.example.com:3478"},
		AgentUsername: "p2p-internal",
		AgentPassword: "p2p-internal",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Stop)

	nc, err := nats.Connect("", nats.InProcessServer(s), nats.UserInfo(AgentUser, AgentPassword), nats.Name("client-a"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	if err := nc.Publish("$P2P.ICE.sess", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := nc.Subscribe("$P2P.NODE.client-a", func(*nats.Msg) {}); err != nil {
		t.Fatal(err)
	}
	got := requestP2P(t, nc, "$P2P.REGISTER", "client-a", []byte(`{"node_key":"client-a"}`))
	if !bytes.Contains(got.Data, []byte(`"ok":true`)) {
		t.Fatalf("register: %s", got.Data)
	}
	if err := nc.Publish("$P2P.MGR.PREPARE", []byte("x")); err == nil {
		// 权限拒绝可能在异步错误里；等一下
		time.Sleep(100 * time.Millisecond)
	}
	async := make(chan error, 1)
	nc.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, e error) { async <- e })
	_ = nc.Publish("$P2P.MGR.PREPARE", []byte("x"))
	select {
	case <-async:
	case <-time.After(time.Second):
		t.Fatal("expected permissions violation on $P2P.MGR")
	}
}

func TestAuthInternalCredentials(t *testing.T) {
	opts := &server.Options{
		Users: []*server.User{{Username: AuthInternalName, Password: "x"}},
		AuthCallout: &server.AuthCallout{
			Issuer:    IssuerPublic(),
			AuthUsers: []string{AuthInternalName},
		},
	}
	u, p, err := AuthInternalCredentials(opts)
	if err != nil || u != AuthInternalName || p != "x" {
		t.Fatalf("%s %s %v", u, p, err)
	}
	_, _, err = AuthInternalCredentials(&server.Options{})
	if err == nil {
		t.Fatal("expected error without callout")
	}
}
```

在文件顶部 import 补上 `"bytes"`（若尚无）。

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./p2p/ -count=1 -run 'TestAuthCallout|TestAuthInternalCredentials' -v`

Expected: FAIL，未定义 `StartAuthCallout`。

- [ ] **Step 3: 实现订阅与应答**

追加到 `p2p/auth_callout.go`：

```go
import (
	"errors"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats-server/v2/server"
)

var errNoAuthCallout = errors.New("auth_callout is not configured")

type AuthCalloutService struct {
	nc *nats.Conn
	sub *nats.Subscription
}

func AuthInternalCredentials(opts *server.Options) (string, string, error) {
	if opts == nil || opts.AuthCallout == nil {
		return "", "", errNoAuthCallout
	}
	for _, u := range opts.Users {
		if u != nil && u.Username == AuthInternalName {
			return u.Username, u.Password, nil
		}
	}
	return "", "", fmt.Errorf("authorization user %q not found", AuthInternalName)
}

func StartAuthCallout(s *server.Server, username, password string) (*AuthCalloutService, error) {
	if s == nil {
		return nil, errors.New("nil server")
	}
	opts := []nats.Option{
		nats.InProcessServer(s),
		nats.Name("p2p-auth-callout"),
		nats.UserInfo(username, password),
	}
	nc, err := nats.Connect("", opts...)
	if err != nil {
		return nil, err
	}
	svc := &AuthCalloutService{nc: nc}
	sub, err := nc.Subscribe(server.AuthCalloutSubject, svc.handle)
	if err != nil {
		nc.Close()
		return nil, err
	}
	svc.sub = sub
	if err := nc.Flush(); err != nil {
		svc.Stop()
		return nil, err
	}
	return svc, nil
}

func (a *AuthCalloutService) Stop() {
	if a == nil {
		return
	}
	if a.sub != nil {
		_ = a.sub.Unsubscribe()
	}
	if a.nc != nil {
		_ = a.nc.Drain()
	}
}

func (a *AuthCalloutService) handle(msg *nats.Msg) {
	ac, err := jwt.DecodeAuthorizationRequestClaims(string(msg.Data))
	if err != nil {
		return
	}
	userNkey := ac.UserNkey
	serverID := ac.Server.ID
	if !MatchAgentCreds(ac.ConnectOptions.Username, ac.ConnectOptions.Password) {
		raw, err := EncodeAuthResponse(userNkey, serverID, "", "authorization denied")
		if err == nil {
			_ = msg.Respond(raw)
		}
		return
	}
	ujwt, err := EncodeAgentUserJWT(userNkey)
	if err != nil {
		raw, encErr := EncodeAuthResponse(userNkey, serverID, "", "authorization denied")
		if encErr == nil {
			_ = msg.Respond(raw)
		}
		return
	}
	raw, err := EncodeAuthResponse(userNkey, serverID, ujwt, "")
	if err != nil {
		return
	}
	_ = msg.Respond(raw)
}
```

不要 `log` 用户名或密码。

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./p2p/ -count=1 -v`

Expected: 全包 PASS（含原有无 Callout 测试）。

若 `Publish($P2P.MGR)` 的异步错误不稳定：改为 `nc.Request("$P2P.MGR.PREPARE", []byte("x"), 200*time.Millisecond)` 并断言 `err != nil`。

- [ ] **Step 5: Commit**

```bash
git add p2p/auth_callout.go p2p/auth_callout_test.go
git commit -m "$(cat <<'EOF'
Handle $SYS.REQ.USER.AUTH in-process for nats-p2p.

EOF
)"
```

---

### Task 3: 包装进程启动顺序

**Files:**
- Modify: `cmd/nats-p2p-server/main.go`

**Interfaces:**
- Consumes: `CheckIssuer`、`AuthInternalCredentials`、`StartAuthCallout`、`(*AuthCalloutService).Stop`
- Produces: 仅当 `opts.AuthCallout != nil` 时启动 auth；先于 `StartManager`

- [ ] **Step 1: 写失败测试（包装启动路径用 `go test` 覆盖 issuer 查找，main 用编译检查）**

`p2p/auth_callout_test.go` 已覆盖 `AuthInternalCredentials`。本任务确认 `main.go` 在 `ReadyForConnections` 之后插入：

```go
	var auth *p2p.AuthCalloutService
	if opts.AuthCallout != nil {
		if err := p2p.CheckIssuer(opts.AuthCallout.Issuer); err != nil {
			log.Fatal(err)
		}
		user, pass, err := p2p.AuthInternalCredentials(opts)
		if err != nil {
			log.Fatal(err)
		}
		auth, err = p2p.StartAuthCallout(s, user, pass)
		if err != nil {
			log.Fatal(err)
		}
		defer auth.Stop()
	}
	if has {
		m, err := p2p.StartManager(s, pcfg)
		// 保持原有错误处理
```

`defer auth.Stop()` 必须在 `StartManager` 之前执行完启动；`Stop` 在进程退出时先于 `s.Shutdown()` 即可。

- [ ] **Step 2: 编译包装进程**

Run: `go build -o /tmp/nats-p2p-server ./cmd/nats-p2p-server`

Expected: 成功，exit 0。

- [ ] **Step 3: 手工环回（可选，不作为 CI）**

写临时 conf（**不要提交**），`authorization.auth_callout.issuer` 设为 `IssuerPublic()` 的打印值。用 `go test` 已覆盖主路径则本步可跳过。

打印公钥：`go test ./p2p/ -count=1 -run TestCheckIssuerMatchesSeed -v` 不打印公钥。需要时在测试里 `t.Log(IssuerPublic())` 仅本地看，**不要把 t.Log 留在提交里**。

- [ ] **Step 4: 全包回归**

Run: `go test ./p2p/ -count=1`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add cmd/nats-p2p-server/main.go
git commit -m "$(cat <<'EOF'
Start auth callout before the P2P manager.

EOF
)"
```

---

### Task 4: README（不写 Agent 密码）

**Files:**
- Modify: `p2p/README.md`

**Interfaces:**
- Consumes: 无新 API
- Produces: 运维说明

- [ ] **Step 1: 追加段落**

在 `p2p/README.md` 末尾追加（**不要**写 `AgentUser` / `AgentPassword` 字面量）：

```markdown
## Auth Callout

To delegate CONNECT authentication, add a standard NATS `authorization { auth_callout { ... } }` block (not a top-level `auth {}`).

Required:

- `issuer`: public account nkey that matches the embedded seed in `p2p/auth_callout.go` (`IssuerPublic()`).
- `auth_users`: in-process clients only (`auth-internal`, and `p2p-internal` / `sys` if used).
- `users`: passwords for those internal names. `p2p { agent_username / agent_password }` must equal `p2p-internal`.

The wrapper starts the in-process callout subscriber after `ReadyForConnections` and before `StartManager`. Agent username/password are temporary constants in `p2p/auth_callout.go`; do not put them in this README. Without `auth_callout`, behavior stays on the static user list.
```

- [ ] **Step 2: 确认文件不含 Agent 临时密码字面量**

Run: `rg -n 'raypx2_2026|AgentPassword' p2p/README.md && exit 1 || true`

Expected: 无匹配。

- [ ] **Step 3: Commit**

```bash
git add p2p/README.md
git commit -m "$(cat <<'EOF'
Document nats-p2p auth callout config without agent secrets.

EOF
)"
```

---

## Spec coverage

| 规格项 | 任务 |
| --- | --- |
| 同进程 InProcessConn，不改 `server/*.go` | 2、3 |
| 无 `auth {}`，常量验密 | 1 |
| issuer 公钥与种子核对 | 1、3 |
| JWT subject + `_INBOX.>` | 1、2 |
| 错/空密码失败 | 1、2 |
| 先 auth 再 Manager | 3 |
| 无 callout 兼容 | 3（不启动）、现有测试 |
| 文档不写 Agent 密码 | 4 |
| 公网切换 / Agent 脚本 | raypx2 计划 |

集群「各节点本地应答」：每个进程各自 `StartAuthCallout`；不在 `$SYS.REQ.USER.AUTH` 上建 queue group。v1 不另写 cluster route 用例。
