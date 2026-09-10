package p2p

import (
	"fmt"
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

func TestMatchAgentSharedKeyCreds(t *testing.T) {
	key, user := uint32(1000003), uint32(0x123456)
	password := uint64(user) * uint64(key)
	if !MatchAgentSharedKeyCreds(fmt.Sprintf("%08x", user), fmt.Sprintf("%016x", password), fmt.Sprintf("%08x", key)) {
		t.Fatal("expected matching shared-key credentials")
	}
	if !MatchAgentSharedKeyCreds("123456", fmt.Sprintf("%x", password), "f4243") {
		t.Fatal("short hex values should be left-padded with zeroes")
	}
	for _, value := range [][3]string{{"000f4240", fmt.Sprintf("%016x", uint64(1000000)*uint64(key)), fmt.Sprintf("%08x", key)},
		{fmt.Sprintf("%08x", user), "10000000000000000", fmt.Sprintf("%08x", key)},
		{fmt.Sprintf("%08x", user), fmt.Sprintf("%016x", password+1), fmt.Sprintf("%08x", key)},
		{fmt.Sprintf("%08x", user), fmt.Sprintf("%016x", password), "000f4240"}} {
		if MatchAgentSharedKeyCreds(value[0], value[1], value[2]) {
			t.Fatalf("accepted %#v", value)
		}
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
	raw, err := EncodeAgentUserJWT(pub, "client-a")
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
	wantCMD, wantEVENT := v2AllowSubjects(t, "client-a")
	if !hasExactSubject(uc.Pub.Allow, wantCMD) {
		t.Fatalf("missing pub %s in %v", wantCMD, []string(uc.Pub.Allow))
	}
	if !hasExactSubject(uc.Sub.Allow, wantEVENT) || !hasExactSubject(uc.Sub.Allow, "_INBOX.>") {
		t.Fatalf("missing sub %s or _INBOX.> in %v", wantEVENT, []string(uc.Sub.Allow))
	}
}

func TestAgentPermissionsV2(t *testing.T) {
	kp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := kp.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeAgentUserJWT(pub, "node-a")
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
	wantCMD, wantEVENT := v2AllowSubjects(t, "node-a")
	if !hasExactSubject(uc.Pub.Allow, wantCMD) {
		t.Fatalf("pub allow %v, want %s", []string(uc.Pub.Allow), wantCMD)
	}
	if len(uc.Pub.Allow) != 1 || !hasExactSubject(uc.Pub.Allow, wantCMD) {
		t.Fatalf("pub allow must be exactly %s, got %v", wantCMD, []string(uc.Pub.Allow))
	}
	if hasExactSubject(uc.Pub.Allow, "$P2P.V2.CMD.node-b.>") ||
		hasExactSubject(uc.Pub.Allow, "$P2P.V2.MGR.>") {
		t.Fatalf("pub allow must not grant other-node CMD or MGR: %v", []string(uc.Pub.Allow))
	}
	if !hasExactSubject(uc.Sub.Allow, wantEVENT) || !hasExactSubject(uc.Sub.Allow, "_INBOX.>") {
		t.Fatalf("sub allow %v, want %s and _INBOX.>", []string(uc.Sub.Allow), wantEVENT)
	}
	if len(uc.Sub.Allow) != 2 {
		t.Fatalf("sub allow must be exactly EVENT + _INBOX.>, got %v", []string(uc.Sub.Allow))
	}
	if hasExactSubject(uc.Sub.Allow, "$P2P.V2.EVENT.node-b.>") ||
		hasExactSubject(uc.Sub.Allow, "$P2P.V2.MGR.>") ||
		hasExactSubject(uc.Sub.Allow, "$P2P.NODE.>") {
		t.Fatalf("sub allow must not grant other-node EVENT, MGR, or node inbox: %v", []string(uc.Sub.Allow))
	}
	if _, err := EncodeAgentUserJWT(pub, ""); err == nil {
		t.Fatal("empty node key must be rejected")
	}
	if _, err := EncodeAgentUserJWT(pub, "bad node"); err == nil {
		t.Fatal("invalid node key must be rejected")
	}
}

func v2AllowSubjects(t *testing.T, nodeKey string) (cmdAllow, eventAllow string) {
	t.Helper()
	sample, err := CommandSubjectV2(nodeKey, string(FrameKindCreateV2))
	if err != nil {
		t.Fatal(err)
	}
	cmdAllow = strings.TrimSuffix(sample, string(FrameKindCreateV2)) + ">"
	const zeroReg = "0123456789abcdef0123456789abcdef"
	sample, err = EventSubjectV2(nodeKey, zeroReg)
	if err != nil {
		t.Fatal(err)
	}
	eventAllow = strings.TrimSuffix(sample, zeroReg) + ">"
	return cmdAllow, eventAllow
}

func hasExactSubject(list jwt.StringList, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
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
	_, err = connectTCPWithDynamicCredentials(s, "client-a", func(user, _ string) (string, string) {
		return user, "wrong"
	})
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

	nc, err := connectTCPWithDynamicCredentials(s, "client-a", func(user, pass string) (string, string) {
		return user, pass
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	cmd, err := CommandSubjectV2("client-a", string(FrameKindCreateV2))
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Publish(cmd, []byte("x")); err != nil {
		t.Fatal(err)
	}
	ev, err := EventSubjectV2("client-a", "0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nc.Subscribe(ev, func(*nats.Msg) {}); err != nil {
		t.Fatal(err)
	}
	if err := nc.Publish("$P2P.MGR.PREPARE", []byte("x")); err == nil {
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

func TestAuthCalloutRejectsInvalidConnectNameV2(t *testing.T) {
	s := startEmbeddedCallout(t)
	svc, err := StartAuthCallout(s, AuthInternalName, "auth-internal")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(svc.Stop)
	_, err = connectTCPWithDynamicCredentials(s, "bad node", func(user, pass string) (string, string) {
		return user, pass
	})
	if err == nil {
		t.Fatal("invalid CONNECT name must be denied")
	}
}
