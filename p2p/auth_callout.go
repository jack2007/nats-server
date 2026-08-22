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
