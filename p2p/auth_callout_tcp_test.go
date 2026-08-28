package p2p

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/nats-io/nats-server/v2/server"
)

func writeCalloutConf(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "nats.conf")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func defaultCalloutConf() string {
	return fmt.Sprintf(`
listen: "127.0.0.1:-1"
authorization {
  timeout: 2s
  users: [
    { user: auth-internal, password: "auth-internal" }
    { user: p2p-internal, password: "p2p-internal" }
  ]
  auth_callout {
    issuer: "%s"
    auth_users: [ auth-internal, p2p-internal ]
  }
}
`, IssuerPublic())
}

func restrictedCalloutConf() string {
	return fmt.Sprintf(`
listen: "127.0.0.1:-1"
authorization {
  timeout: 2s
  users: [
    { user: auth-internal, password: "auth-internal", permissions: { publish: ["_INBOX.>"], subscribe: ["$SYS.REQ.USER.AUTH"] } }
    { user: p2p-internal, password: "p2p-internal" }
  ]
  auth_callout {
    issuer: "%s"
    auth_users: [ auth-internal, p2p-internal ]
  }
}
`, IssuerPublic())
}

func startTCPCalloutFromConf(t *testing.T, conf string, withManager bool, mgr Config) (*server.Server, *AuthCalloutService, *Manager) {
	t.Helper()
	opts, err := server.ProcessConfigFile(writeCalloutConf(t, conf))
	if err != nil {
		t.Fatal(err)
	}
	opts.NoLog = true
	opts.NoSigs = true
	if opts.AuthCallout == nil {
		t.Fatal("auth_callout missing")
	}
	if err := CheckIssuer(opts.AuthCallout.Issuer); err != nil {
		t.Fatal(err)
	}
	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("not ready")
	}
	user, pass, err := AuthInternalCredentials(opts)
	if err != nil {
		s.Shutdown()
		t.Fatal(err)
	}
	auth, err := StartAuthCallout(s, user, pass)
	if err != nil {
		s.Shutdown()
		t.Fatal(err)
	}
	var m *Manager
	if withManager {
		if mgr.AgentUsername == "" {
			mgr.AgentUsername = "p2p-internal"
			mgr.AgentPassword = "p2p-internal"
		}
		if len(mgr.STUNURLs) == 0 {
			mgr.STUNURLs = []string{"stun:turn.example.com:3478"}
		}
		m, err = StartManager(s, mgr)
		if err != nil {
			auth.Stop()
			s.Shutdown()
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		if m != nil {
			m.Stop()
		}
		auth.Stop()
		s.Shutdown()
	})
	return s, auth, m
}

func connectTCP(s *server.Server, user, pass, name string) (*nats.Conn, error) {
	opts := []nats.Option{nats.Timeout(4 * time.Second)}
	if name != "" {
		opts = append(opts, nats.Name(name))
	}
	opts = append(opts, nats.UserInfo(user, pass))
	return nats.Connect(s.ClientURL(), opts...)
}

func mustConnectAgent(t *testing.T, s *server.Server, name string) *nats.Conn {
	t.Helper()
	nc, err := connectTCP(s, AgentUser, AgentPassword, name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })
	return nc
}

func TestTCPCalloutAcceptsAgentRejectsBadAndEmpty(t *testing.T) {
	s, _, _ := startTCPCalloutFromConf(t, defaultCalloutConf(), true, Config{})

	if _, err := connectTCP(s, "app", "app", "legacy"); err == nil {
		t.Fatal("legacy app/app must fail")
	}
	if _, err := connectTCP(s, AgentUser, "wrong", "bad-pass"); err == nil {
		t.Fatal("wrong password must fail")
	}
	if _, err := connectTCP(s, "", AgentPassword, "empty-user"); err == nil {
		t.Fatal("empty user must fail")
	}
	if _, err := connectTCP(s, AgentUser, "", "empty-pass"); err == nil {
		t.Fatal("empty password must fail")
	}
	if _, err := nats.Connect(s.ClientURL(), nats.Token("SECRET"), nats.Timeout(4*time.Second)); err == nil {
		t.Fatal("token-only CONNECT must fail")
	}
	nc, err := connectTCP(s, AgentUser, AgentPassword, "client-a")
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()
}

func TestTCPCalloutRestrictedInboxPermsTimesOut(t *testing.T) {
	s, _, _ := startTCPCalloutFromConf(t, restrictedCalloutConf(), true, Config{})
	start := time.Now()
	_, err := connectTCP(s, AgentUser, AgentPassword, "client-a")
	if err == nil {
		t.Fatal("restricted auth-internal must drop Callout replies")
	}
	if time.Since(start) < time.Second {
		t.Fatalf("expected timeout-class failure, got %v after %s", err, time.Since(start))
	}
}

func TestTCPCalloutDeniesSysAndWildcard(t *testing.T) {
	s, _, _ := startTCPCalloutFromConf(t, defaultCalloutConf(), true, Config{})
	nc := mustConnectAgent(t, s, "client-a")

	if _, err := nc.Request("$SYS.REQ.SERVER.PING", nil, time.Second); err == nil {
		t.Fatal("SYS ping must be denied")
	}

	async := make(chan error, 2)
	nc.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, e error) { async <- e })
	if _, err := nc.Subscribe(">", func(*nats.Msg) {}); err != nil && !strings.Contains(strings.ToLower(err.Error()), "permission") {
		t.Fatal(err)
	}
	_ = nc.Publish("$P2P.MGR.PREPARE", []byte("x"))
	seen := 0
	deadline := time.After(2 * time.Second)
	for seen < 1 {
		select {
		case <-async:
			seen++
		case <-deadline:
			t.Fatal("expected permissions violation on $P2P.MGR or wildcard sub")
		}
	}
}

func TestTCPCalloutCreateIncludesTurn(t *testing.T) {
	dir := t.TempDir()
	secretPath := filepath.Join(dir, "secret")
	secret := []byte("test-static-auth-secret")
	if err := os.WriteFile(secretPath, secret, 0o600); err != nil {
		t.Fatal(err)
	}
	s, _, _ := startTCPCalloutFromConf(t, defaultCalloutConf(), true, Config{
		STUNURLs:      []string{"stun:turn.example.com:3478"},
		TURNURLs:      []string{"turn:turn.example.com:3478?transport=udp"},
		SecretFile:    secretPath,
		CredentialTTL: DefaultCredentialTTL,
	})
	client := mustConnectAgent(t, s, "client-a")
	peer := mustConnectAgent(t, s, "server-b")
	if _, err := client.Subscribe("$P2P.NODE.client-a", func(*nats.Msg) {}); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.Subscribe("$P2P.NODE.server-b", func(*nats.Msg) {}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if msg := requestP2P(t, client, "$P2P.REGISTER", "client-a", []byte(`{"node_key":"client-a"}`)); !bytes.Contains(msg.Data, []byte(`"ok":true`)) {
		t.Fatalf("register client: %s", msg.Data)
	}
	if msg := requestP2P(t, peer, "$P2P.REGISTER", "server-b", []byte(`{"node_key":"server-b"}`)); !bytes.Contains(msg.Data, []byte(`"ok":true`)) {
		t.Fatalf("register peer: %s", msg.Data)
	}
	got := requestP2P(t, client, "$P2P.CREATE", "client-a", []byte(`{"peer_node_key":"server-b"}`))
	var resp struct {
		OK       bool     `json:"ok"`
		StunURLs []string `json:"stun_urls"`
		Turn     TurnCred `json:"turn"`
	}
	if err := json.Unmarshal(got.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.OK || resp.Turn.Username == "" || len(resp.StunURLs) == 0 {
		t.Fatalf("create missing turn/stun: %s", got.Data)
	}
}

func TestTCPCalloutHardDisconnectWithoutSysKeepsOccupancy(t *testing.T) {
	s, _, _ := startTCPCalloutFromConf(t, defaultCalloutConf(), true, Config{})
	nc := mustConnectAgent(t, s, "node-z")
	if msg := requestP2P(t, nc, "$P2P.REGISTER", "node-z", []byte(`{"node_key":"node-z"}`)); !bytes.Contains(msg.Data, []byte(`"ok":true`)) {
		t.Fatalf("register: %s", msg.Data)
	}
	nc.Close()
	nc2, err := connectTCP(s, AgentUser, AgentPassword, "node-z")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc2.Close() })
	msg := requestP2P(t, nc2, "$P2P.REGISTER", "node-z", []byte(`{"node_key":"node-z"}`))
	if !bytes.Contains(msg.Data, []byte(`"ok":true`)) {
		t.Fatalf("connz reclaim without sys must allow reregister, got %s", msg.Data)
	}
}

func TestTCPCalloutStopsRejectingNewConnects(t *testing.T) {
	s, auth, _ := startTCPCalloutFromConf(t, defaultCalloutConf(), true, Config{})
	auth.Stop()
	if _, err := connectTCP(s, AgentUser, AgentPassword, "after-stop"); err == nil {
		t.Fatal("CONNECT after auth.Stop must fail")
	}
}

func TestCalloutStartFailsClosed(t *testing.T) {
	wrongIssuer := strings.ReplaceAll(defaultCalloutConf(), IssuerPublic(), "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	opts, err := server.ProcessConfigFile(writeCalloutConf(t, wrongIssuer))
	if err != nil {
		t.Fatal(err)
	}
	if opts.AuthCallout == nil {
		t.Fatal("expected parsed auth_callout")
	}
	if err := CheckIssuer(opts.AuthCallout.Issuer); err == nil {
		t.Fatal("wrong issuer must fail CheckIssuer")
	}

	_, _, err = AuthInternalCredentials(&server.Options{AuthCallout: &server.AuthCallout{Issuer: IssuerPublic()}})
	if err == nil {
		t.Fatal("missing auth-internal must fail")
	}

	s, _, _ := startTCPCalloutFromConf(t, defaultCalloutConf(), false, Config{})
	if _, err := StartAuthCallout(s, AuthInternalName, "wrong-internal"); err == nil {
		t.Fatal("wrong internal password must fail StartAuthCallout")
	}
}

func TestTCPCalloutTwoNodeKeysShareCreds(t *testing.T) {
	s, _, _ := startTCPCalloutFromConf(t, defaultCalloutConf(), true, Config{})
	a := mustConnectAgent(t, s, "key-a")
	b := mustConnectAgent(t, s, "key-b")
	if msg := requestP2P(t, a, "$P2P.REGISTER", "key-a", []byte(`{"node_key":"key-a"}`)); !bytes.Contains(msg.Data, []byte(`"ok":true`)) {
		t.Fatalf("%s", msg.Data)
	}
	if msg := requestP2P(t, b, "$P2P.REGISTER", "key-b", []byte(`{"node_key":"key-b"}`)); !bytes.Contains(msg.Data, []byte(`"ok":true`)) {
		t.Fatalf("%s", msg.Data)
	}
}

func TestTCPCalloutDuplicateNodeKeyInUse(t *testing.T) {
	s, _, _ := startTCPCalloutFromConf(t, defaultCalloutConf(), true, Config{})
	a := mustConnectAgent(t, s, "shared-key")
	if msg := requestP2P(t, a, "$P2P.REGISTER", "shared-key", []byte(`{"node_key":"shared-key"}`)); !bytes.Contains(msg.Data, []byte(`"ok":true`)) {
		t.Fatalf("%s", msg.Data)
	}
	dup := mustConnectAgent(t, s, "shared-key")
	msg := requestP2P(t, dup, "$P2P.REGISTER", "shared-key", []byte(`{"node_key":"shared-key"}`))
	if !bytes.Contains(msg.Data, []byte(`node_key_in_use`)) {
		t.Fatalf("%s", msg.Data)
	}
}

// TestAuthCalloutTCPConnectNameSubjectScopeV2 proves an already-authenticated
// connection cannot use another CONNECT name's V2 subjects. Shared-credential
// holders reconnecting with a different CONNECT name are a trust-boundary
// leftover of this release and are out of scope; this is not an
// anti-impersonation test.
func TestAuthCalloutTCPConnectNameSubjectScopeV2(t *testing.T) {
	s, _, _ := startTCPCalloutFromConf(t, defaultCalloutConf(), false, Config{})
	a := mustConnectAgent(t, s, "node-a")
	b := mustConnectAgent(t, s, "node-b")

	ownCMD, err := CommandSubjectV2("node-a", string(FrameKindCreateV2))
	if err != nil {
		t.Fatal(err)
	}
	ownEVENT, err := EventSubjectV2("node-a", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Publish(ownCMD, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Subscribe(ownEVENT, func(*nats.Msg) {}); err != nil {
		t.Fatal(err)
	}
	if err := a.Flush(); err != nil {
		t.Fatal(err)
	}

	otherCMD, err := CommandSubjectV2("node-b", string(FrameKindCreateV2))
	if err != nil {
		t.Fatal(err)
	}
	otherEVENT, err := EventSubjectV2("node-b", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if err != nil {
		t.Fatal(err)
	}
	expectPermissionsViolation(t, a, func() error {
		return a.Publish(otherCMD, []byte("x"))
	})
	expectPermissionsViolation(t, a, func() error {
		_, err := a.Subscribe(otherEVENT, func(*nats.Msg) {})
		return err
	})
	expectPermissionsViolation(t, b, func() error {
		return b.Publish(ownCMD, []byte("x"))
	})
	expectPermissionsViolation(t, a, func() error {
		return a.Publish("$P2P.V2.MGR.COMMAND", []byte("x"))
	})
	expectPermissionsViolation(t, a, func() error {
		_, err := a.Subscribe("$P2P.V2.MGR.>", func(*nats.Msg) {})
		return err
	})
	expectPermissionsViolation(t, a, func() error {
		return a.Publish("$P2P.REGISTER", []byte("x"))
	})
	expectPermissionsViolation(t, a, func() error {
		_, err := a.Subscribe("$P2P.NODE.node-a", func(*nats.Msg) {})
		return err
	})
}

func expectPermissionsViolation(t *testing.T, nc *nats.Conn, op func() error) {
	t.Helper()
	async := make(chan error, 4)
	nc.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, e error) { async <- e })
	t.Cleanup(func() { nc.SetErrorHandler(nil) })
	if err := op(); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "permission") {
			return
		}
		t.Fatal(err)
	}
	select {
	case err := <-async:
		if err == nil || !strings.Contains(strings.ToLower(err.Error()), "permission") {
			t.Fatalf("expected permissions violation, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected permissions violation")
	}
}

func TestTCPCalloutLeftoverAppAccountStaysOnGlobal(t *testing.T) {
	conf := fmt.Sprintf(`
listen: "127.0.0.1:-1"
accounts {
  APP {
    users: [ { user: leftover, password: leftover } ]
  }
}
authorization {
  timeout: 2s
  users: [
    { user: auth-internal, password: "auth-internal" }
    { user: p2p-internal, password: "p2p-internal" }
  ]
  auth_callout {
    issuer: "%s"
    auth_users: [ auth-internal, p2p-internal ]
  }
}
`, IssuerPublic())
	path := writeCalloutConf(t, conf)
	opts, err := server.ProcessConfigFile(path)
	if err != nil {
		return
	}
	if opts.AuthCallout == nil {
		t.Fatal("parsed without auth_callout")
	}
	s, _, _ := startTCPCalloutFromConf(t, conf, true, Config{})
	nc := mustConnectAgent(t, s, "client-a")
	if msg := requestP2P(t, nc, "$P2P.REGISTER", "client-a", []byte(`{"node_key":"client-a"}`)); !bytes.Contains(msg.Data, []byte(`"ok":true`)) {
		t.Fatalf("register on leftover APP conf: %s", msg.Data)
	}
}
