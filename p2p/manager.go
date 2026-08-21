package p2p

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/nats-io/nats-server/v2/server"
)

const (
	p2pQueueGroup      = "p2p"
	subjectRegister    = "$P2P.REGISTER"
	subjectCreate      = "$P2P.CREATE"
	subjectUnreg         = "$P2P.UNREGISTER"
	subjectDisconnect    = "$SYS.ACCOUNT.*.DISCONNECT"
	headerP2PName        = "Nats-P2P-Name"
	defaultConnID        = "conn-0"
)

var errConnzUnavailable = errors.New("connz unavailable")

type Manager struct {
	s      *server.Server
	cfg    Config
	nc     *nats.Conn
	sysNC  *nats.Conn
	table  *Table
	secret []byte
	now    func() time.Time

	mu    sync.Mutex
	regMu sync.Mutex
	bound map[string]string

	countNamed func(name string) (int, error)
}

type createReply struct {
	OK           bool      `json:"ok"`
	SessionID    string    `json:"session_id"`
	ConnectionID string    `json:"connection_id"`
	Epoch        uint64    `json:"epoch"`
	ICESubject   string    `json:"ice_subject"`
	StunURLs     []string  `json:"stun_urls"`
	Turn         *TurnCred `json:"turn,omitempty"`
}

func StartManager(s *server.Server, cfg Config) (*Manager, error) {
	if err := ValidateConfig(cfg); err != nil {
		return nil, err
	}
	opts := []nats.Option{nats.InProcessServer(s), nats.Name("p2p-manager"), nats.UseOldRequestStyle()}
	if cfg.AgentUsername != "" {
		opts = append(opts, nats.UserInfo(cfg.AgentUsername, cfg.AgentPassword))
	}
	nc, err := nats.Connect("", opts...)
	if err != nil {
		return nil, err
	}
	m := &Manager{
		s:     s,
		cfg:   cfg,
		nc:    nc,
		table: NewTable(),
		now:   time.Now,
		bound: make(map[string]string),
	}
	if cfg.SecretFile != "" {
		if secret, err := LoadSecret(cfg.SecretFile); err == nil {
			m.secret = secret
		}
	}
	if _, err := nc.QueueSubscribe(subjectRegister, p2pQueueGroup, m.handleRegister); err != nil {
		nc.Close()
		return nil, err
	}
	if _, err := nc.QueueSubscribe(subjectCreate, p2pQueueGroup, m.handleCreate); err != nil {
		nc.Close()
		return nil, err
	}
	if _, err := nc.QueueSubscribe(subjectUnreg, p2pQueueGroup, m.handleUnregister); err != nil {
		nc.Close()
		return nil, err
	}
	if cfg.SysUsername != "" {
		sysNC, err := nats.Connect("",
			nats.InProcessServer(s),
			nats.Name("p2p-manager-sys"),
			nats.UserInfo(cfg.SysUsername, cfg.SysPassword),
		)
		if err != nil {
			nc.Close()
			return nil, err
		}
		m.sysNC = sysNC
		if _, err := sysNC.Subscribe(subjectDisconnect, m.handleDisconnect); err != nil {
			nc.Close()
			sysNC.Close()
			return nil, err
		}
		if err := sysNC.Flush(); err != nil {
			nc.Close()
			sysNC.Close()
			return nil, err
		}
	}
	if err := nc.Flush(); err != nil {
		if m.sysNC != nil {
			m.sysNC.Close()
		}
		nc.Close()
		return nil, err
	}
	return m, nil
}

func (m *Manager) Stop() {
	if m == nil {
		return
	}
	if m.sysNC != nil {
		_ = m.sysNC.Drain()
	}
	if m.nc != nil {
		_ = m.nc.Drain()
	}
}

func (m *Manager) handleRegister(msg *nats.Msg) {
	headerName := msg.Header.Get(headerP2PName)
	var req RegisterRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil || req.NodeKey == "" {
		_ = msg.Respond(EncodeError(ErrInvalidRequest))
		return
	}
	if headerName == "" || headerName != req.NodeKey {
		_ = msg.Respond(EncodeError(ErrNameMismatch))
		return
	}
	if !ValidNodeKey(req.NodeKey) {
		_ = msg.Respond(EncodeError(ErrInvalidNodeKey))
		return
	}

	account := m.agentAccount()
	inbox := "$P2P.NODE." + req.NodeKey

	m.regMu.Lock()
	count, err := m.countConnsNamed(req.NodeKey)
	if err != nil || count == 0 {
		m.regMu.Unlock()
		_ = msg.Respond(EncodeError(CodeNodeKeyInUse))
		return
	}
	_, claimed := m.table.Get(account, req.NodeKey)
	if count > 1 && claimed {
		m.regMu.Unlock()
		_ = msg.Respond(EncodeError(CodeNodeKeyInUse))
		return
	}
	if claimed {
		m.regMu.Unlock()
		m.setBound(headerName, req.NodeKey)
		_ = msg.Respond(encodeRegisterOK(req.NodeKey, inbox))
		return
	}

	claimID, err := newUUID()
	if err != nil {
		m.regMu.Unlock()
		_ = msg.Respond(EncodeError(ErrInvalidRequest))
		return
	}
	rec := Record{
		ServerID:  m.s.ID(),
		ConnName:  req.NodeKey,
		Inbox:     inbox,
		ClaimID:   claimID,
		ClaimedAt: m.now().UnixNano(),
	}
	if err := m.table.Claim(account, req.NodeKey, rec); err != nil {
		m.regMu.Unlock()
		if errors.Is(err, ErrNodeKeyInUse) {
			_ = msg.Respond(EncodeError(CodeNodeKeyInUse))
			return
		}
		_ = msg.Respond(EncodeError(ErrInvalidRequest))
		return
	}
	m.regMu.Unlock()
	m.setBound(headerName, req.NodeKey)
	_ = msg.Respond(encodeRegisterOK(req.NodeKey, inbox))
}

func (m *Manager) handleCreate(msg *nats.Msg) {
	headerName := msg.Header.Get(headerP2PName)
	self, ok := m.getBound(headerName)
	if !ok || headerName == "" {
		_ = msg.Respond(EncodeError(ErrNotRegistered))
		return
	}
	account := m.agentAccount()
	if _, ok := m.table.Get(account, self); !ok {
		_ = msg.Respond(EncodeError(ErrNotRegistered))
		return
	}

	var req CreateRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil || req.PeerNodeKey == "" {
		_ = msg.Respond(EncodeError(ErrInvalidRequest))
		return
	}
	if !ValidNodeKey(req.PeerNodeKey) {
		_ = msg.Respond(EncodeError(ErrInvalidNodeKey))
		return
	}
	if req.PeerNodeKey == self {
		_ = msg.Respond(EncodeError(ErrPeerIsSelf))
		return
	}
	if _, ok := m.table.Get(account, req.PeerNodeKey); !ok {
		_ = msg.Respond(EncodeError(ErrPeerNotRegistered))
		return
	}

	connID := req.ConnectionID
	if connID == "" {
		connID = defaultConnID
	}
	sessionID, err := newUUID()
	if err != nil {
		_ = msg.Respond(EncodeError(ErrInvalidRequest))
		return
	}
	epoch := uint64(1)
	iceSubject := "$P2P.ICE." + sessionID
	turn := m.issueTurn(sessionID, connID, epoch)

	body, err := json.Marshal(createReply{
		OK:           true,
		SessionID:    sessionID,
		ConnectionID: connID,
		Epoch:        epoch,
		ICESubject:   iceSubject,
		StunURLs:     m.cfg.STUNURLs,
		Turn:         turn,
	})
	if err != nil {
		_ = msg.Respond(EncodeError(ErrInvalidRequest))
		return
	}
	invite, err := EncodeInvite(sessionID, connID, epoch, m.cfg.STUNURLs, turn, iceSubject)
	if err != nil {
		_ = msg.Respond(EncodeError(ErrInvalidRequest))
		return
	}
	_ = msg.Respond(body)
	_ = m.nc.Publish("$P2P.NODE."+self, invite)
	_ = m.nc.Publish("$P2P.NODE."+req.PeerNodeKey, invite)
}

func (m *Manager) handleUnregister(msg *nats.Msg) {
	headerName := msg.Header.Get(headerP2PName)
	var req RegisterRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil || req.NodeKey == "" {
		_ = msg.Respond(EncodeError(ErrInvalidRequest))
		return
	}
	if headerName == "" || headerName != req.NodeKey {
		_ = msg.Respond(EncodeError(ErrNameMismatch))
		return
	}
	if !m.table.Release(m.agentAccount(), req.NodeKey, req.NodeKey) {
		_ = msg.Respond(EncodeError(ErrNotRegistered))
		return
	}
	m.deleteBound(headerName)
	b, _ := json.Marshal(map[string]any{"ok": true})
	_ = msg.Respond(b)
}

func (m *Manager) issueTurn(sessionID, connectionID string, epoch uint64) *TurnCred {
	if len(m.secret) == 0 || len(m.cfg.TURNURLs) == 0 {
		return nil
	}
	ttl := m.cfg.CredentialTTL
	if ttl <= 0 {
		ttl = DefaultCredentialTTL
	}
	now := m.now()
	ttlSec := int64(ttl / time.Second)
	user, pass := IssueREST(m.secret, sessionID, connectionID, epoch, now.Unix(), ttlSec)
	return &TurnCred{
		URLs:      append([]string(nil), m.cfg.TURNURLs...),
		Username:  user,
		Password:  pass,
		ExpiresAt: now.Unix() + ttlSec,
	}
}

func (m *Manager) countConnsNamed(name string) (int, error) {
	if m.countNamed != nil {
		return m.countNamed(name)
	}
	cz, err := m.s.Connz(&server.ConnzOptions{Limit: server.DefaultConnListSize})
	if err != nil {
		return 0, err
	}
	if cz.Total > len(cz.Conns) {
		return 0, errConnzUnavailable
	}
	n := 0
	for _, c := range cz.Conns {
		if c.Name == name {
			n++
		}
	}
	return n, nil
}

func (m *Manager) setBound(name, nodeKey string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bound[name] = nodeKey
}

func (m *Manager) getBound(name string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k, ok := m.bound[name]
	return k, ok
}

func (m *Manager) deleteBound(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.bound, name)
}

func (m *Manager) agentAccount() string {
	if m.cfg.AgentAccount != "" {
		return m.cfg.AgentAccount
	}
	return server.DEFAULT_GLOBAL_ACCOUNT
}

func (m *Manager) handleDisconnect(msg *nats.Msg) {
	parts := strings.Split(msg.Subject, ".")
	if len(parts) < 4 {
		return
	}
	account := parts[2]

	var ev struct {
		Client struct {
			Name string `json:"name"`
		} `json:"client"`
	}
	if err := json.Unmarshal(msg.Data, &ev); err != nil {
		return
	}
	connName := ev.Client.Name
	if connName == "" {
		return
	}

	m.regMu.Lock()
	released := m.table.Release(account, connName, connName)
	m.regMu.Unlock()
	if released {
		m.deleteBound(connName)
	}
}

func encodeRegisterOK(nodeKey, inbox string) []byte {
	b, _ := json.Marshal(map[string]any{
		"ok":       true,
		"node_key": nodeKey,
		"inbox":    inbox,
	})
	return b
}
