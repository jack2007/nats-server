package p2p

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"regexp"
)

const (
	ErrInvalidNodeKey    = "invalid_node_key"
	ErrNameMismatch      = "name_mismatch"
	ErrAlreadyRegistered = "already_registered"
	CodeNodeKeyInUse     = "node_key_in_use"
	ErrNotRegistered     = "not_registered"
	ErrPeerNotRegistered = "peer_not_registered"
	ErrPeerIsSelf        = "peer_is_self"
	ErrInvalidRequest    = "invalid_request"
)

var nodeKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

type RegisterRequest struct {
	NodeKey string `json:"node_key"`
}

type CreateRequest struct {
	PeerNodeKey  string `json:"peer_node_key"`
	ConnectionID string `json:"connection_id"`
}

type TurnCred struct {
	URLs      []string `json:"urls"`
	Username  string   `json:"username"`
	Password  string   `json:"password"`
	ExpiresAt int64    `json:"expires_at"`
}

func ValidNodeKey(s string) bool {
	return nodeKeyPattern.MatchString(s)
}

func EncodeError(code string) []byte {
	b, _ := json.Marshal(map[string]any{
		"ok":    false,
		"error": code,
	})
	return b
}

type inviteFrame struct {
	ID           string        `json:"id"`
	SessionID    string        `json:"session_id"`
	ConnectionID string        `json:"connection_id"`
	Epoch        uint64        `json:"epoch"`
	Seq          int           `json:"seq"`
	Type         string        `json:"type"`
	Payload      invitePayload `json:"payload"`
}

type invitePayload struct {
	StunURLs   []string  `json:"stun_urls,omitempty"`
	Turn       *TurnCred `json:"turn,omitempty"`
	ICESubject string    `json:"ice_subject"`
}

func EncodeInvite(sessionID, connectionID string, epoch uint64, stun []string, turn *TurnCred, iceSubject string) ([]byte, error) {
	id, err := newUUID()
	if err != nil {
		return nil, err
	}
	frame := inviteFrame{
		ID:           id,
		SessionID:    sessionID,
		ConnectionID: connectionID,
		Epoch:        epoch,
		Seq:          1,
		Type:         "invite",
		Payload: invitePayload{
			StunURLs:   stun,
			Turn:       turn,
			ICESubject: iceSubject,
		},
	}
	return json.Marshal(frame)
}

func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}
