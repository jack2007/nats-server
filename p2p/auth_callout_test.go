package p2p

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"

	"github.com/nats-io/nats-server/v2/server"
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
