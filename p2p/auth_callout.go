package p2p

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"strconv"
	"strings"
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

// MatchAgentSharedKeyCreds verifies the one-shot credentials bound to the
// shared key advertised in this connection's INFO. Hex values may omit leading
// zeroes, but are bounded to the uint32/uint64 widths.
func MatchAgentSharedKeyCreds(user, password, sharedKey string) bool {
	if len(user) == 0 || len(user) > 8 ||
		len(password) == 0 || len(password) > 16 ||
		len(sharedKey) == 0 || len(sharedKey) > 8 {
		return false
	}
	u, err := strconv.ParseUint(user, 16, 32)
	if err != nil {
		return false
	}
	p, err := strconv.ParseUint(password, 16, 64)
	if err != nil {
		return false
	}
	k, err := strconv.ParseUint(sharedKey, 16, 32)
	if err != nil || k <= 1_000_000 {
		return false
	}
	return u > 1_000_000 && uint64(uint32(u))*uint64(uint32(k)) == p
}

func agentV2AllowSubjects(nodeKey string) (cmdAllow, eventAllow string, err error) {
	if err := ValidateNodeKeyV2(nodeKey); err != nil {
		return "", "", err
	}
	sample, err := CommandSubjectV2(nodeKey, string(FrameKindCreateV2))
	if err != nil {
		return "", "", err
	}
	cmdAllow = strings.TrimSuffix(sample, string(FrameKindCreateV2)) + ">"
	const sampleReg = "0123456789abcdef0123456789abcdef"
	sample, err = EventSubjectV2(nodeKey, sampleReg)
	if err != nil {
		return "", "", err
	}
	eventAllow = strings.TrimSuffix(sample, sampleReg) + ">"
	return cmdAllow, eventAllow, nil
}

func EncodeAgentUserJWT(userNKey, nodeKey string) (string, error) {
	kp, err := issuerKeyPair()
	if err != nil {
		return "", err
	}
	cmdAllow, eventAllow, err := agentV2AllowSubjects(nodeKey)
	if err != nil {
		return "", err
	}
	uc := jwt.NewUserClaims(userNKey)
	uc.Audience = "$G"
	uc.Expires = time.Now().Add(agentJWTTTL).Unix()
	uc.Pub.Allow.Add(cmdAllow)
	uc.Sub.Allow.Add(eventAllow)
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
	if !MatchAgentSharedKeyCreds(ac.ConnectOptions.Username, ac.ConnectOptions.Password, ac.ClientInformation.Nonce) {
		raw, err := EncodeAuthResponse(userNkey, serverID, "", "authorization denied")
		if err == nil {
			_ = msg.Respond(raw)
		}
		return
	}
	nodeKey := ac.ConnectOptions.Name
	if err := ValidateNodeKeyV2(nodeKey); err != nil {
		raw, err := EncodeAuthResponse(userNkey, serverID, "", "authorization denied")
		if err == nil {
			_ = msg.Respond(raw)
		}
		return
	}
	ujwt, err := EncodeAgentUserJWT(userNkey, nodeKey)
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
