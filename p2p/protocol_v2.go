package p2p

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/nats-io/nkeys"
)

const ProtocolVersionV2 = 2

const (
	MaxFrameBytesV2       = 256 * 1024
	MaxDescriptionBytesV2 = 128 * 1024
	MaxCandidateBytesV2   = 8 * 1024
)

type SessionStateV2 string
type ConnectionStateV2 string
type SessionCommandKindV2 string
type ConnectionCommandKindV2 string
type SignalKindV2 string
type FrameKindV2 string
type ErrorCodeV2 string

const (
	SessionStateAllocatedV2 SessionStateV2 = "allocated"
	SessionStatePreparingV2 SessionStateV2 = "preparing"
	SessionStateActiveV2    SessionStateV2 = "active"
	SessionStateDrainingV2  SessionStateV2 = "draining"
	SessionStateClosedV2    SessionStateV2 = "closed"
	SessionStateFailedV2    SessionStateV2 = "failed"
)

const (
	ConnectionStatePreparingV2  ConnectionStateV2 = "preparing"
	ConnectionStateActiveV2     ConnectionStateV2 = "active"
	ConnectionStateRestartingV2 ConnectionStateV2 = "restarting"
	ConnectionStateClosedV2     ConnectionStateV2 = "closed"
)

const (
	SessionCommandBindV2   SessionCommandKindV2 = "BIND"
	SessionCommandResumeV2 SessionCommandKindV2 = "RESUME"
	SessionCommandCloseV2  SessionCommandKindV2 = "CLOSE"
)

const (
	ConnectionCommandOpenV2    ConnectionCommandKindV2 = "OPEN"
	ConnectionCommandRestartV2 ConnectionCommandKindV2 = "RESTART"
	ConnectionCommandBindV2    ConnectionCommandKindV2 = "BIND"
	ConnectionCommandReadyV2   ConnectionCommandKindV2 = "READY"
	ConnectionCommandRejectV2  ConnectionCommandKindV2 = "REJECT"
	ConnectionCommandCloseV2   ConnectionCommandKindV2 = "CLOSE"
)

const (
	SignalKindDescriptionV2     SignalKindV2 = "description"
	SignalKindCandidateV2       SignalKindV2 = "candidate"
	SignalKindEndOfCandidatesV2 SignalKindV2 = "end_of_candidates"
)

const (
	FrameKindRegisterV2          FrameKindV2 = "REGISTER"
	FrameKindRegisterReplyV2     FrameKindV2 = "REGISTER.REPLY"
	FrameKindCreateV2            FrameKindV2 = "SESSION.CREATE"
	FrameKindSessionCommandV2    FrameKindV2 = "SESSION.COMMAND"
	FrameKindConnectionCommandV2 FrameKindV2 = "CONNECTION.COMMAND"
	FrameKindSignalSendV2        FrameKindV2 = "SIGNAL.SEND"
	FrameKindSignalAckV2         FrameKindV2 = "SIGNAL.ACK"
	FrameKindAllocatedV2         FrameKindV2 = "ALLOCATED"
	FrameKindPrepareV2           FrameKindV2 = "PREPARE"
	FrameKindStartV2             FrameKindV2 = "START"
	FrameKindErrorV2             FrameKindV2 = "ERROR"
)

const (
	ErrInvalidRequestV2         ErrorCodeV2 = "invalid_request"
	ErrNotRegisteredV2          ErrorCodeV2 = "not_registered"
	ErrPeerNotRegisteredV2      ErrorCodeV2 = "peer_not_registered"
	ErrNotSessionMemberV2       ErrorCodeV2 = "not_session_member"
	ErrSessionNotFoundV2        ErrorCodeV2 = "session_not_found"
	ErrConnectionNotFoundV2     ErrorCodeV2 = "connection_not_found"
	ErrConnectionLimitV2        ErrorCodeV2 = "connection_limit"
	ErrStaleEpochV2             ErrorCodeV2 = "stale_epoch"
	ErrFutureEpochV2            ErrorCodeV2 = "future_epoch"
	ErrSequenceGapV2            ErrorCodeV2 = "sequence_gap"
	ErrStaleRevisionV2          ErrorCodeV2 = "stale_revision"
	ErrInvalidStateV2           ErrorCodeV2 = "invalid_state"
	ErrSetupTimeoutV2           ErrorCodeV2 = "setup_timeout"
	ErrBusyV2                   ErrorCodeV2 = "busy"
	ErrCoordinatorUnavailableV2 ErrorCodeV2 = "coordinator_unavailable"
	ErrInternalErrorV2          ErrorCodeV2 = "internal_error"
)

type EnvelopeV2 struct {
	Version        int             `json:"version"`
	RequestID      string          `json:"request_id,omitempty"`
	MessageID      string          `json:"message_id,omitempty"`
	RegistrationID string          `json:"registration_id,omitempty"`
	Payload        json.RawMessage `json:"payload"`
}

type IdentityV2 struct {
	SessionID    string `json:"session_id"`
	ConnectionID string `json:"connection_id,omitempty"`
	Epoch        uint64 `json:"epoch,omitempty"`
}

type TurnCredV2 struct {
	URLs      []string `json:"urls"`
	Username  string   `json:"username"`
	Password  string   `json:"password"`
	ExpiresAt int64    `json:"expires_at"`
}

type SignalPayloadV2 struct {
	SDP       string `json:"sdp,omitempty"`
	Candidate string `json:"candidate,omitempty"`
}

type ProtocolErrorV2 struct {
	Code ErrorCodeV2
}

func (e *ProtocolErrorV2) Error() string {
	if e == nil {
		return ""
	}
	return string(e.Code)
}

func invalidRequestV2() error {
	return &ProtocolErrorV2{Code: ErrInvalidRequestV2}
}

type RegisterCommandV2 struct {
	RequestID      string
	RegistrationID string
	NodeKey        string
}

type RegisterReplyV2 struct {
	RequestID         string
	RegistrationID    string
	RegistrationEpoch uint64
}

type CreateSessionCommandV2 struct {
	RequestID      string
	ServerNodeKey  string
	TotalTimeoutMs int64 // omitted means DefaultSetupTimeoutV2
}

type SessionCommandV2 struct {
	RequestID string
	Command   SessionCommandKindV2
	IdentityV2
	Revision uint64
}

type ConnectionCommandV2 struct {
	RequestID string
	Command   ConnectionCommandKindV2
	IdentityV2
	Revision uint64
}

type SignalSendCommandV2 struct {
	RequestID string
	MessageID string
	IdentityV2
	Seq           uint64
	Type          SignalKindV2
	Payload       SignalPayloadV2
	SenderNodeKey string
}

type SignalAckCommandV2 struct {
	RequestID string
	IdentityV2
	AckSeq uint64
}

type AllocatedReplyV2 struct {
	OK            bool
	RequestID     string
	SessionID     string
	ConnectionID  string
	Epoch         uint64
	State         string
	Revision      uint64
	OwnerServerID string
}

type PrepareEventV2 struct {
	MessageID      string
	RegistrationID string
	IdentityV2
	Revision        uint64
	Role            string
	PeerNodeKey     string
	SenderNodeKey   string
	StunURLs        []string
	Turn            *TurnCredV2
	SetupDeadlineMs int64
	ProtocolVersion int
	FeatureBits     uint64
}

type StartEventV2 struct {
	MessageID      string
	RegistrationID string
	IdentityV2
	Revision      uint64
	SenderNodeKey string
}

type ErrorPayloadV2 struct {
	Code         ErrorCodeV2 `json:"error"`
	RetryAfterMs *int64      `json:"retry_after_ms,omitempty"`
	Revision     *uint64     `json:"revision,omitempty"`
	SessionID    string      `json:"session_id,omitempty"`
	ConnectionID string      `json:"connection_id,omitempty"`
	Limit        *int        `json:"limit,omitempty"`
}

type DecodedV2 struct {
	Envelope      EnvelopeV2
	Kind          FrameKindV2
	Register      *RegisterCommandV2
	RegisterReply *RegisterReplyV2
	Create        *CreateSessionCommandV2
	Session       *SessionCommandV2
	Connection    *ConnectionCommandV2
	SignalSend    *SignalSendCommandV2
	SignalAck     *SignalAckCommandV2
	Allocated     *AllocatedReplyV2
	Prepare       *PrepareEventV2
	Start         *StartEventV2
	Error         *ErrorPayloadV2
}

var (
	nodeKeyPatternV2        = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	registrationIDPatternV2 = regexp.MustCompile(`^[0-9a-f]{32}$`)
	uuidPatternV2           = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	connectionIDPatternV2   = regexp.MustCompile(`^conn-(0|[1-9][0-9]{0,9})$`)
)

var commandSuffixesV2 = map[string]struct{}{
	"SESSION.CREATE":     {},
	"SESSION.COMMAND":    {},
	"CONNECTION.COMMAND": {},
	"SIGNAL.SEND":        {},
	"SIGNAL.ACK":         {},
}

var knownErrorCodesV2 = map[ErrorCodeV2]struct{}{
	ErrInvalidRequestV2:         {},
	ErrNotRegisteredV2:          {},
	ErrPeerNotRegisteredV2:      {},
	ErrNotSessionMemberV2:       {},
	ErrSessionNotFoundV2:        {},
	ErrConnectionNotFoundV2:     {},
	ErrConnectionLimitV2:        {},
	ErrStaleEpochV2:             {},
	ErrFutureEpochV2:            {},
	ErrSequenceGapV2:            {},
	ErrStaleRevisionV2:          {},
	ErrInvalidStateV2:           {},
	ErrSetupTimeoutV2:           {},
	ErrBusyV2:                   {},
	ErrCoordinatorUnavailableV2: {},
	ErrInternalErrorV2:          {},
}

func ValidateNodeKeyV2(nodeKey string) error {
	if !nodeKeyPatternV2.MatchString(nodeKey) {
		return invalidRequestV2()
	}
	return nil
}

func ValidateServerIDV2(serverID string) error {
	if !nkeys.IsValidPublicServerKey(serverID) {
		return invalidRequestV2()
	}
	return nil
}

func ValidateRegistrationIDV2(registrationID string) error {
	if !registrationIDPatternV2.MatchString(registrationID) {
		return invalidRequestV2()
	}
	return nil
}

func ValidateUUIDV2(id string) error {
	if !uuidPatternV2.MatchString(id) {
		return invalidRequestV2()
	}
	return nil
}

func ValidateConnectionIDV2(id string) error {
	if !connectionIDPatternV2.MatchString(id) {
		return invalidRequestV2()
	}
	return nil
}

func NewUUIDV2() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

func CommandSubjectV2(nodeKey, suffix string) (string, error) {
	if err := ValidateNodeKeyV2(nodeKey); err != nil {
		return "", err
	}
	if _, ok := commandSuffixesV2[suffix]; !ok {
		return "", invalidRequestV2()
	}
	return "$P2P.V2.CMD." + nodeKey + "." + suffix, nil
}

func RegisterSubjectV2(nodeKey, serverID string) (string, error) {
	if err := ValidateNodeKeyV2(nodeKey); err != nil {
		return "", err
	}
	if err := ValidateServerIDV2(serverID); err != nil {
		return "", err
	}
	return "$P2P.V2.CMD." + nodeKey + ".REGISTER." + serverID, nil
}

func EventSubjectV2(nodeKey, registrationID string) (string, error) {
	if err := ValidateNodeKeyV2(nodeKey); err != nil {
		return "", err
	}
	if err := ValidateRegistrationIDV2(registrationID); err != nil {
		return "", err
	}
	return "$P2P.V2.EVENT." + nodeKey + "." + registrationID, nil
}

func ParseCommandSubjectV2(subject string) (nodeKey, suffix string, err error) {
	if subject == "" || strings.ContainsAny(subject, "*>") {
		return "", "", invalidRequestV2()
	}
	parts := strings.Split(subject, ".")
	if len(parts) != 6 {
		return "", "", invalidRequestV2()
	}
	for _, p := range parts {
		if p == "" {
			return "", "", invalidRequestV2()
		}
	}
	if parts[0] != "$P2P" || parts[1] != "V2" || parts[2] != "CMD" {
		return "", "", invalidRequestV2()
	}
	if err := ValidateNodeKeyV2(parts[3]); err != nil {
		return "", "", err
	}
	suffix = parts[4] + "." + parts[5]
	if parts[4] == "REGISTER" {
		if err := ValidateServerIDV2(parts[5]); err != nil {
			return "", "", err
		}
		return parts[3], suffix, nil
	}
	if _, ok := commandSuffixesV2[suffix]; !ok {
		return "", "", invalidRequestV2()
	}
	return parts[3], suffix, nil
}

func DecodeFrameV2(kind FrameKindV2, data []byte) (*DecodedV2, error) {
	if len(data) > MaxFrameBytesV2 {
		return nil, invalidRequestV2()
	}
	var env EnvelopeV2
	if err := decodeStrictV2(data, &env); err != nil {
		return nil, err
	}
	if env.Version != ProtocolVersionV2 {
		return nil, invalidRequestV2()
	}
	if !jsonObjectV2(env.Payload) {
		return nil, invalidRequestV2()
	}
	out := &DecodedV2{Envelope: env, Kind: kind}
	switch kind {
	case FrameKindRegisterV2:
		cmd, err := decodeRegisterV2(env)
		if err != nil {
			return nil, err
		}
		out.Register = cmd
	case FrameKindRegisterReplyV2:
		cmd, err := decodeRegisterReplyV2(env)
		if err != nil {
			return nil, err
		}
		out.RegisterReply = cmd
	case FrameKindCreateV2:
		cmd, err := decodeCreateV2(env)
		if err != nil {
			return nil, err
		}
		out.Create = cmd
	case FrameKindSessionCommandV2:
		cmd, err := decodeSessionCommandV2(env)
		if err != nil {
			return nil, err
		}
		out.Session = cmd
	case FrameKindConnectionCommandV2:
		cmd, err := decodeConnectionCommandV2(env)
		if err != nil {
			return nil, err
		}
		out.Connection = cmd
	case FrameKindSignalSendV2:
		cmd, err := decodeSignalSendV2(env)
		if err != nil {
			return nil, err
		}
		out.SignalSend = cmd
	case FrameKindSignalAckV2:
		cmd, err := decodeSignalAckV2(env)
		if err != nil {
			return nil, err
		}
		out.SignalAck = cmd
	case FrameKindAllocatedV2:
		cmd, err := decodeAllocatedV2(env)
		if err != nil {
			return nil, err
		}
		out.Allocated = cmd
	case FrameKindPrepareV2:
		cmd, err := decodePrepareV2(env)
		if err != nil {
			return nil, err
		}
		out.Prepare = cmd
	case FrameKindStartV2:
		cmd, err := decodeStartV2(env)
		if err != nil {
			return nil, err
		}
		out.Start = cmd
	case FrameKindErrorV2:
		cmd, err := decodeErrorV2(env)
		if err != nil {
			return nil, err
		}
		out.Error = cmd
	default:
		return nil, invalidRequestV2()
	}
	return out, nil
}

func decodeRegisterV2(env EnvelopeV2) (*RegisterCommandV2, error) {
	if err := requireRequestIDV2(env.RequestID); err != nil {
		return nil, err
	}
	if err := ValidateRegistrationIDV2(env.RegistrationID); err != nil {
		return nil, err
	}
	var payload struct{}
	if err := decodeStrictV2(env.Payload, &payload); err != nil {
		return nil, err
	}
	return &RegisterCommandV2{RequestID: env.RequestID, RegistrationID: env.RegistrationID}, nil
}

func decodeRegisterReplyV2(env EnvelopeV2) (*RegisterReplyV2, error) {
	if err := requireRequestIDV2(env.RequestID); err != nil {
		return nil, err
	}
	if err := ValidateRegistrationIDV2(env.RegistrationID); err != nil {
		return nil, err
	}
	var payload struct {
		RegistrationEpoch uint64 `json:"registration_epoch"`
	}
	if err := decodeStrictV2(env.Payload, &payload); err != nil {
		return nil, err
	}
	if payload.RegistrationEpoch == 0 {
		return nil, invalidRequestV2()
	}
	return &RegisterReplyV2{
		RequestID:         env.RequestID,
		RegistrationID:    env.RegistrationID,
		RegistrationEpoch: payload.RegistrationEpoch,
	}, nil
}

func decodeCreateV2(env EnvelopeV2) (*CreateSessionCommandV2, error) {
	if err := requireRequestIDV2(env.RequestID); err != nil {
		return nil, err
	}
	var payload struct {
		ServerNodeKey  string `json:"server_node_key"`
		TotalTimeoutMs *int64 `json:"total_timeout_ms"`
	}
	if err := decodeStrictV2(env.Payload, &payload); err != nil {
		return nil, err
	}
	if err := ValidateNodeKeyV2(payload.ServerNodeKey); err != nil {
		return nil, err
	}
	cmd := &CreateSessionCommandV2{RequestID: env.RequestID, ServerNodeKey: payload.ServerNodeKey}
	if payload.TotalTimeoutMs != nil {
		if _, err := ValidateTotalTimeoutMsV2(*payload.TotalTimeoutMs); err != nil {
			return nil, err
		}
		cmd.TotalTimeoutMs = *payload.TotalTimeoutMs
	}
	return cmd, nil
}

func decodeSessionCommandV2(env EnvelopeV2) (*SessionCommandV2, error) {
	if err := requireRequestIDV2(env.RequestID); err != nil {
		return nil, err
	}
	var payload struct {
		Command SessionCommandKindV2 `json:"command"`
		IdentityV2
		Revision uint64 `json:"revision,omitempty"`
	}
	if err := decodeStrictV2(env.Payload, &payload); err != nil {
		return nil, err
	}
	switch payload.Command {
	case SessionCommandBindV2, SessionCommandResumeV2, SessionCommandCloseV2:
	default:
		return nil, invalidRequestV2()
	}
	if err := validateIdentityV2(payload.IdentityV2); err != nil {
		return nil, err
	}
	return &SessionCommandV2{
		RequestID:  env.RequestID,
		Command:    payload.Command,
		IdentityV2: payload.IdentityV2,
		Revision:   payload.Revision,
	}, nil
}

func decodeConnectionCommandV2(env EnvelopeV2) (*ConnectionCommandV2, error) {
	if err := requireRequestIDV2(env.RequestID); err != nil {
		return nil, err
	}
	var payload struct {
		Command ConnectionCommandKindV2 `json:"command"`
		IdentityV2
		Revision uint64 `json:"revision,omitempty"`
	}
	if err := decodeStrictV2(env.Payload, &payload); err != nil {
		return nil, err
	}
	switch payload.Command {
	case ConnectionCommandOpenV2, ConnectionCommandRestartV2, ConnectionCommandBindV2,
		ConnectionCommandReadyV2, ConnectionCommandRejectV2, ConnectionCommandCloseV2:
	default:
		return nil, invalidRequestV2()
	}
	if err := validateConnectionIdentityV2(payload.Command, payload.IdentityV2); err != nil {
		return nil, err
	}
	return &ConnectionCommandV2{
		RequestID:  env.RequestID,
		Command:    payload.Command,
		IdentityV2: payload.IdentityV2,
		Revision:   payload.Revision,
	}, nil
}

func decodeSignalSendV2(env EnvelopeV2) (*SignalSendCommandV2, error) {
	if err := ValidateUUIDV2(env.MessageID); err != nil {
		return nil, err
	}
	var payload struct {
		IdentityV2
		Seq           uint64          `json:"seq"`
		Type          SignalKindV2    `json:"type"`
		Payload       SignalPayloadV2 `json:"payload"`
		SenderNodeKey string          `json:"sender_node_key,omitempty"`
	}
	if err := decodeStrictV2(env.Payload, &payload); err != nil {
		return nil, err
	}
	if payload.Seq == 0 {
		return nil, invalidRequestV2()
	}
	if err := validateIdentityV2(payload.IdentityV2); err != nil {
		return nil, err
	}
	if err := validateSignalPayloadV2(payload.Type, payload.Payload); err != nil {
		return nil, err
	}
	if env.RegistrationID != "" {
		if err := ValidateRegistrationIDV2(env.RegistrationID); err != nil {
			return nil, err
		}
		if err := ValidateNodeKeyV2(payload.SenderNodeKey); err != nil {
			return nil, err
		}
	} else {
		if err := requireRequestIDV2(env.RequestID); err != nil {
			return nil, err
		}
		if payload.SenderNodeKey != "" {
			return nil, invalidRequestV2()
		}
	}
	return &SignalSendCommandV2{
		RequestID:     env.RequestID,
		MessageID:     env.MessageID,
		IdentityV2:    payload.IdentityV2,
		Seq:           payload.Seq,
		Type:          payload.Type,
		Payload:       payload.Payload,
		SenderNodeKey: payload.SenderNodeKey,
	}, nil
}

func decodeSignalAckV2(env EnvelopeV2) (*SignalAckCommandV2, error) {
	if err := requireRequestIDV2(env.RequestID); err != nil {
		return nil, err
	}
	var payload struct {
		IdentityV2
		AckSeq uint64 `json:"ack_seq"`
	}
	if err := decodeStrictV2(env.Payload, &payload); err != nil {
		return nil, err
	}
	if payload.AckSeq == 0 {
		return nil, invalidRequestV2()
	}
	if err := validateIdentityV2(payload.IdentityV2); err != nil {
		return nil, err
	}
	return &SignalAckCommandV2{
		RequestID:  env.RequestID,
		IdentityV2: payload.IdentityV2,
		AckSeq:     payload.AckSeq,
	}, nil
}

func decodeAllocatedV2(env EnvelopeV2) (*AllocatedReplyV2, error) {
	if err := requireRequestIDV2(env.RequestID); err != nil {
		return nil, err
	}
	var payload struct {
		OK            bool   `json:"ok"`
		SessionID     string `json:"session_id"`
		ConnectionID  string `json:"connection_id"`
		Epoch         uint64 `json:"epoch"`
		State         string `json:"state"`
		Revision      uint64 `json:"revision"`
		OwnerServerID string `json:"owner_server_id"`
	}
	if err := decodeStrictV2(env.Payload, &payload); err != nil {
		return nil, err
	}
	if !payload.OK || payload.Epoch == 0 || payload.Revision == 0 || payload.State == "" {
		return nil, invalidRequestV2()
	}
	if err := ValidateUUIDV2(payload.SessionID); err != nil {
		return nil, err
	}
	if err := ValidateConnectionIDV2(payload.ConnectionID); err != nil {
		return nil, err
	}
	if err := ValidateServerIDV2(payload.OwnerServerID); err != nil {
		return nil, err
	}
	return &AllocatedReplyV2{
		OK:            true,
		RequestID:     env.RequestID,
		SessionID:     payload.SessionID,
		ConnectionID:  payload.ConnectionID,
		Epoch:         payload.Epoch,
		State:         payload.State,
		Revision:      payload.Revision,
		OwnerServerID: payload.OwnerServerID,
	}, nil
}

func decodePrepareV2(env EnvelopeV2) (*PrepareEventV2, error) {
	if err := requireEventIDsV2(env); err != nil {
		return nil, err
	}
	var payload struct {
		IdentityV2
		Revision        uint64      `json:"revision"`
		Role            string      `json:"role"`
		PeerNodeKey     string      `json:"peer_node_key"`
		SenderNodeKey   string      `json:"sender_node_key"`
		StunURLs        []string    `json:"stun_urls"`
		Turn            *TurnCredV2 `json:"turn,omitempty"`
		SetupDeadlineMs int64       `json:"setup_deadline_ms"`
		ProtocolVersion int         `json:"protocol_version"`
		FeatureBits     uint64      `json:"feature_bits"`
	}
	if err := decodeStrictV2(env.Payload, &payload); err != nil {
		return nil, err
	}
	if err := validateIdentityV2(payload.IdentityV2); err != nil {
		return nil, err
	}
	if payload.Revision == 0 || payload.SetupDeadlineMs <= 0 || payload.ProtocolVersion != ProtocolVersionV2 {
		return nil, invalidRequestV2()
	}
	if payload.Role != "client" && payload.Role != "server" {
		return nil, invalidRequestV2()
	}
	if err := ValidateNodeKeyV2(payload.PeerNodeKey); err != nil {
		return nil, err
	}
	if err := ValidateNodeKeyV2(payload.SenderNodeKey); err != nil {
		return nil, err
	}
	return &PrepareEventV2{
		MessageID:       env.MessageID,
		RegistrationID:  env.RegistrationID,
		IdentityV2:      payload.IdentityV2,
		Revision:        payload.Revision,
		Role:            payload.Role,
		PeerNodeKey:     payload.PeerNodeKey,
		SenderNodeKey:   payload.SenderNodeKey,
		StunURLs:        payload.StunURLs,
		Turn:            payload.Turn,
		SetupDeadlineMs: payload.SetupDeadlineMs,
		ProtocolVersion: payload.ProtocolVersion,
		FeatureBits:     payload.FeatureBits,
	}, nil
}

func decodeStartV2(env EnvelopeV2) (*StartEventV2, error) {
	if err := requireEventIDsV2(env); err != nil {
		return nil, err
	}
	var payload struct {
		IdentityV2
		Revision      uint64 `json:"revision"`
		SenderNodeKey string `json:"sender_node_key"`
	}
	if err := decodeStrictV2(env.Payload, &payload); err != nil {
		return nil, err
	}
	if err := validateIdentityV2(payload.IdentityV2); err != nil {
		return nil, err
	}
	if payload.Revision == 0 {
		return nil, invalidRequestV2()
	}
	if err := ValidateNodeKeyV2(payload.SenderNodeKey); err != nil {
		return nil, err
	}
	return &StartEventV2{
		MessageID:      env.MessageID,
		RegistrationID: env.RegistrationID,
		IdentityV2:     payload.IdentityV2,
		Revision:       payload.Revision,
		SenderNodeKey:  payload.SenderNodeKey,
	}, nil
}

func decodeErrorV2(env EnvelopeV2) (*ErrorPayloadV2, error) {
	if env.RequestID != "" {
		if err := requireRequestIDV2(env.RequestID); err != nil {
			return nil, err
		}
	} else if err := requireEventIDsV2(env); err != nil {
		return nil, err
	}
	var payload struct {
		ErrorPayloadV2
		Epoch         uint64 `json:"epoch,omitempty"`
		SenderNodeKey string `json:"sender_node_key,omitempty"`
	}
	if err := decodeStrictV2(env.Payload, &payload); err != nil {
		return nil, err
	}
	if _, ok := knownErrorCodesV2[payload.Code]; !ok {
		return nil, invalidRequestV2()
	}
	if payload.SessionID != "" {
		if err := ValidateUUIDV2(payload.SessionID); err != nil {
			return nil, err
		}
	}
	if payload.ConnectionID != "" {
		if err := ValidateConnectionIDV2(payload.ConnectionID); err != nil {
			return nil, err
		}
	}
	if payload.Limit != nil && *payload.Limit != 128 {
		return nil, invalidRequestV2()
	}
	if payload.SenderNodeKey != "" {
		if err := ValidateNodeKeyV2(payload.SenderNodeKey); err != nil {
			return nil, err
		}
	}
	out := payload.ErrorPayloadV2
	return &out, nil
}

func requireRequestIDV2(id string) error {
	return ValidateUUIDV2(id)
}

func requireEventIDsV2(env EnvelopeV2) error {
	if err := ValidateUUIDV2(env.MessageID); err != nil {
		return err
	}
	return ValidateRegistrationIDV2(env.RegistrationID)
}

func validateIdentityV2(id IdentityV2) error {
	if err := ValidateUUIDV2(id.SessionID); err != nil {
		return err
	}
	if err := ValidateConnectionIDV2(id.ConnectionID); err != nil {
		return err
	}
	if id.Epoch == 0 {
		return invalidRequestV2()
	}
	return nil
}

func validateConnectionIdentityV2(kind ConnectionCommandKindV2, id IdentityV2) error {
	if err := ValidateUUIDV2(id.SessionID); err != nil {
		return err
	}
	switch kind {
	case ConnectionCommandOpenV2:
		if id.ConnectionID == "" {
			return nil
		}
		return ValidateConnectionIDV2(id.ConnectionID)
	case ConnectionCommandRestartV2:
		return ValidateConnectionIDV2(id.ConnectionID)
	default:
		return validateIdentityV2(id)
	}
}

func validateSignalPayloadV2(kind SignalKindV2, payload SignalPayloadV2) error {
	switch kind {
	case SignalKindDescriptionV2:
		if payload.Candidate != "" || len(payload.SDP) > MaxDescriptionBytesV2 {
			return invalidRequestV2()
		}
	case SignalKindCandidateV2:
		if payload.SDP != "" || len(payload.Candidate) > MaxCandidateBytesV2 {
			return invalidRequestV2()
		}
	case SignalKindEndOfCandidatesV2:
		if payload.SDP != "" || payload.Candidate != "" {
			return invalidRequestV2()
		}
	default:
		return invalidRequestV2()
	}
	return nil
}

func jsonObjectV2(raw json.RawMessage) bool {
	s := bytes.TrimSpace(raw)
	return len(s) >= 2 && s[0] == '{' && s[len(s)-1] == '}'
}

func decodeStrictV2(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return invalidRequestV2()
	}
	if dec.More() {
		return invalidRequestV2()
	}
	return nil
}
