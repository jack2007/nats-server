package p2p

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"

	"github.com/nats-io/nats-server/v2/server"
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

var errNoAuthCallout = errors.New("auth_callout is not configured")

type AuthCalloutService struct {
	nc  *nats.Conn
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
