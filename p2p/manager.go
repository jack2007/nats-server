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
	subjectRegister   = "$P2P.REGISTER"
	subjectCreate     = "$P2P.CREATE"
	subjectUnreg      = "$P2P.UNREGISTER"
	subjectDisconnect = "$SYS.ACCOUNT.*.DISCONNECT"
	headerP2PName     = "Nats-P2P-Name"
	defaultConnID     = "conn-0"
)

var errConnzUnavailable = errors.New("connz unavailable")

type Manager struct {
	s        *server.Server
	cfg      Config
	nc       *nats.Conn
	sysNC    *nats.Conn
	table    *Table
	secret   []byte
	now      func() time.Time
	serverID string
	peers    *peerTracker

	mu          sync.Mutex
	regMu       sync.Mutex
	keyMu       sync.Map
	bound       map[string]string
	clusterStop chan struct{}

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
		s:        s,
		cfg:      cfg,
		nc:       nc,
		table:    NewTable(),
		now:      time.Now,
		serverID: s.ID(),
		bound:    make(map[string]string),
	}
	if cfg.SecretFile != "" {
		if secret, err := LoadSecret(cfg.SecretFile); err == nil {
			m.secret = secret
		}
	}
	if _, err := nc.Subscribe(subjectRegister, m.handleRegister); err != nil {
		nc.Close()
		return nil, err
	}
	if _, err := nc.Subscribe(subjectCreate, m.handleCreate); err != nil {
		nc.Close()
		return nil, err
	}
	if _, err := nc.Subscribe(subjectUnreg, m.handleUnregister); err != nil {
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
	if err := m.startCluster(); err != nil {
		m.stopCluster()
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
	m.stopCluster()
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

	unlockKey := m.lockNodeKey(req.NodeKey)
	defer unlockKey()

	account := m.agentAccount()
	inbox := "$P2P.NODE." + req.NodeKey

	m.regMu.Lock()
	count, err := m.countConnsNamed(req.NodeKey)
	existing, claimed := m.table.Get(account, req.NodeKey)
	if count == 0 {
		m.regMu.Unlock()
		if err != nil {
			_ = msg.Respond(EncodeError(CodeNodeKeyInUse))
			return
		}
		if !claimed && !m.inCluster() {
			_ = msg.Respond(EncodeError(ErrNameMismatch))
		}
		return
	}
	if count > 1 && claimed {
		m.regMu.Unlock()
		_ = msg.Respond(EncodeError(CodeNodeKeyInUse))
		return
	}
	if claimed {
		if existing.ServerID != m.serverID {
			m.regMu.Unlock()
			if !m.inCluster() {
				_ = msg.Respond(EncodeError(CodeNodeKeyInUse))
			}
			return
		}
		if err != nil {
			m.regMu.Unlock()
			_ = msg.Respond(EncodeError(CodeNodeKeyInUse))
			return
		}
		if _, bound := m.getBound(headerName); bound {
			m.regMu.Unlock()
			if m.inCluster() {
				_ = msg.Respond(EncodeError(CodeNodeKeyInUse))
				return
			}
			m.setBound(headerName, req.NodeKey)
			_ = msg.Respond(encodeRegisterOK(req.NodeKey, inbox))
			return
		}
		staleClaimID := existing.ClaimID
		staleConn := existing.ConnName
		m.table.Release(account, req.NodeKey, staleConn)
		m.regMu.Unlock()
		m.publishRelease(account, req.NodeKey, staleConn, staleClaimID)
	} else {
		m.regMu.Unlock()
	}

	claimID, err := newUUID()
	if err != nil {
		_ = msg.Respond(EncodeError(ErrInvalidRequest))
		return
	}
	rec := Record{
		ServerID:  m.serverID,
		ConnName:  req.NodeKey,
		Inbox:     inbox,
		ClaimID:   claimID,
		ClaimedAt: m.now().UnixNano(),
	}
	if !m.clusterPreparePhase(account, req.NodeKey, claimID, rec.ClaimedAt) {
		if m.inCluster() {
			deadline := time.Now().Add(clusterPrepareTimeout)
			for time.Now().Before(deadline) {
				m.regMu.Lock()
				held, ok := m.table.Get(account, req.NodeKey)
				m.regMu.Unlock()
				if ok && held.ClaimID != claimID {
					return
				}
				time.Sleep(20 * time.Millisecond)
			}
		}
		_ = msg.Respond(EncodeError(CodeNodeKeyInUse))
		return
	}
	m.clusterCommitPhase(account, req.NodeKey, rec)
	m.regMu.Lock()
	held, ok := m.table.Get(account, req.NodeKey)
	stillOwner := ok && held.ClaimID == claimID
	m.regMu.Unlock()
	if !stillOwner {
		return
	}
	m.setBound(headerName, req.NodeKey)
	_ = msg.Respond(encodeRegisterOK(req.NodeKey, inbox))
}

func (m *Manager) handleCreate(msg *nats.Msg) {
	headerName := msg.Header.Get(headerP2PName)
	if headerName == "" {
		return
	}
	self, ok := m.getBound(headerName)
	account := m.agentAccount()
	if !ok {
		if rec, exists := m.table.Get(account, headerName); exists {
			if rec.ServerID != m.serverID {
				return
			}
			self = rec.ConnName
			if self == "" {
				self = headerName
			}
			ok = true
		}
	}
	if !ok {
		count, err := m.countConnsNamed(headerName)
		if err != nil {
			_ = msg.Respond(EncodeError(CodeNodeKeyInUse))
			return
		}
		if count == 0 {
			if m.inCluster() {
				return
			}
			_ = msg.Respond(EncodeError(ErrNotRegistered))
			return
		}
		_ = msg.Respond(EncodeError(ErrNotRegistered))
		return
	}
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
	if _, ok := m.getBound(headerName); !ok {
		if rec, exists := m.table.Get(m.agentAccount(), req.NodeKey); exists {
			if rec.ServerID != m.serverID {
				return
			}
		} else {
			count, err := m.countConnsNamed(headerName)
			if err != nil {
				_ = msg.Respond(EncodeError(CodeNodeKeyInUse))
				return
			}
			if count == 0 {
				if m.inCluster() {
					return
				}
				_ = msg.Respond(EncodeError(ErrNotRegistered))
				return
			}
			_ = msg.Respond(EncodeError(ErrNotRegistered))
			return
		}
	}
	account := m.agentAccount()
	m.regMu.Lock()
	existing, ok := m.table.Get(account, req.NodeKey)
	if !ok || existing.ServerID != m.serverID {
		m.regMu.Unlock()
		_ = msg.Respond(EncodeError(ErrNotRegistered))
		return
	}
	claimID := existing.ClaimID
	released := m.table.Release(account, req.NodeKey, existing.ConnName)
	m.regMu.Unlock()
	if !released {
		_ = msg.Respond(EncodeError(ErrNotRegistered))
		return
	}
	m.deleteBound(headerName)
	m.publishRelease(account, req.NodeKey, req.NodeKey, claimID)
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
	n := 0
	offset := 0
	for {
		limit := server.DefaultConnListSize
		cz, err := m.s.Connz(&server.ConnzOptions{Offset: offset, Limit: limit})
		if err != nil {
			return 0, err
		}
		if cz.Total > 0 && cz.Total > len(cz.Conns) && offset == 0 {
			cz, err = m.s.Connz(&server.ConnzOptions{Offset: 0, Limit: cz.Total})
			if err != nil {
				return 0, err
			}
			n = 0
			for _, c := range cz.Conns {
				if c.Name == name {
					n++
				}
			}
			return n, nil
		}
		for _, c := range cz.Conns {
			if c.Name == name {
				n++
			}
		}
		if len(cz.Conns) == 0 || offset+len(cz.Conns) >= cz.Total {
			return n, nil
		}
		offset += len(cz.Conns)
	}
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
	existing, ok := m.table.Get(account, connName)
	if !ok || existing.ServerID != m.serverID {
		m.regMu.Unlock()
		return
	}
	claimID := existing.ClaimID
	released := m.table.Release(account, connName, existing.ConnName)
	m.regMu.Unlock()
	if released {
		m.deleteBound(connName)
		m.publishRelease(account, connName, connName, claimID)
	}
}

func (m *Manager) lockNodeKey(nodeKey string) func() {
	v, _ := m.keyMu.LoadOrStore(nodeKey, &sync.Mutex{})
	mu := v.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

func encodeRegisterOK(nodeKey, inbox string) []byte {
	b, _ := json.Marshal(map[string]any{
		"ok":       true,
		"node_key": nodeKey,
		"inbox":    inbox,
	})
	return b
}
