package p2p

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"testing"
)

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
