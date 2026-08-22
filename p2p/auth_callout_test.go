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
