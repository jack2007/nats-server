package p2p

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	subjectMgrPrepare = "$P2P.MGR.PREPARE"
	subjectMgrCommit  = "$P2P.MGR.COMMIT"
	subjectMgrRelease = "$P2P.MGR.RELEASE"
	subjectMgrBeat    = "$P2P.MGR.BEAT"

	clusterPrepareTimeout = time.Second
	clusterBeatInterval   = time.Second
	clusterBeatTTL        = 5 * time.Second
)

type preparePayload struct {
	Account   string `json:"account"`
	NodeKey   string `json:"node_key"`
	ClaimID   string `json:"claim_id"`
	ClaimedAt int64  `json:"claimed_at"`
	ServerID  string `json:"server_id"`
}

type commitPayload struct {
	Account   string `json:"account"`
	NodeKey   string `json:"node_key"`
	ClaimID   string `json:"claim_id"`
	ClaimedAt int64  `json:"claimed_at"`
	ServerID  string `json:"server_id"`
	ConnName  string `json:"conn_name"`
	Inbox     string `json:"inbox"`
}

type releasePayload struct {
	Account  string `json:"account"`
	NodeKey  string `json:"node_key"`
	ConnName string `json:"conn_name"`
	ClaimID  string `json:"claim_id,omitempty"`
}

type beatPayload struct {
	ServerID string `json:"server_id"`
}

type mgrBoolReply struct {
	OK bool `json:"ok"`
}

type peerTracker struct {
	mu       sync.Mutex
	lastBeat map[string]time.Time
	selfID   string
}

func newPeerTracker(selfID string) *peerTracker {
	return &peerTracker{
		lastBeat: make(map[string]time.Time),
		selfID:   selfID,
	}
}

func (p *peerTracker) note(serverID string, now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastBeat[serverID] = now
}

func (p *peerTracker) alive(now time.Time) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	cutoff := now.Add(-clusterBeatTTL)
	ids := []string{p.selfID}
	for id, ts := range p.lastBeat {
		if id == p.selfID {
			continue
		}
		if !ts.Before(cutoff) {
			ids = append(ids, id)
		}
	}
	return ids
}

func (p *peerTracker) knownOthers() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for id := range p.lastBeat {
		if id != p.selfID {
			return true
		}
	}
	return false
}

func (p *peerTracker) prune(now time.Time) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	cutoff := now.Add(-clusterBeatTTL)
	var dropped []string
	for id, ts := range p.lastBeat {
		if id == p.selfID {
			continue
		}
		if ts.Before(cutoff) {
			delete(p.lastBeat, id)
			dropped = append(dropped, id)
		}
	}
	return dropped
}

func claimWins(atA int64, idA string, atB int64, idB string) bool {
	if atA != atB {
		return atA < atB
	}
	return idA < idB
}

func (m *Manager) mgrConn() *nats.Conn {
	if m.sysNC != nil {
		return m.sysNC
	}
	return m.nc
}

func (m *Manager) inCluster() bool {
	if m.s != nil && m.s.NumRoutes() > 0 {
		return true
	}
	return m.peers != nil && m.peers.knownOthers()
}

func (m *Manager) startCluster() error {
	m.peers = newPeerTracker(m.serverID)
	m.peers.note(m.serverID, m.now())

	nc := m.mgrConn()
	if _, err := nc.Subscribe(subjectMgrPrepare, m.handleMgrPrepare); err != nil {
		return err
	}
	if _, err := nc.Subscribe(subjectMgrCommit, m.handleMgrCommit); err != nil {
		return err
	}
	if _, err := nc.Subscribe(subjectMgrRelease, m.handleMgrRelease); err != nil {
		return err
	}
	if _, err := nc.Subscribe(subjectMgrBeat, m.handleMgrBeat); err != nil {
		return err
	}
	if err := nc.Flush(); err != nil {
		return err
	}

	m.clusterStop = make(chan struct{})
	m.publishBeat()
	go m.clusterBeatLoop()
	go m.clusterPruneLoop()
	return nil
}

func (m *Manager) publishBeat() {
	if m.peers != nil {
		m.peers.note(m.serverID, m.now())
	}
	b, _ := json.Marshal(beatPayload{ServerID: m.serverID})
	_ = m.mgrConn().Publish(subjectMgrBeat, b)
}

func (m *Manager) stopCluster() {
	if m.clusterStop != nil {
		close(m.clusterStop)
		m.clusterStop = nil
	}
}

func (m *Manager) clusterBeatLoop() {
	ticker := time.NewTicker(clusterBeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.clusterStop:
			return
		case <-ticker.C:
			m.publishBeat()
		}
	}
}

func (m *Manager) clusterPruneLoop() {
	ticker := time.NewTicker(clusterBeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.clusterStop:
			return
		case <-ticker.C:
			now := m.now()
			for _, id := range m.peers.prune(now) {
				m.regMu.Lock()
				m.table.DropServer(id)
				m.regMu.Unlock()
			}
		}
	}
}

func (m *Manager) handleMgrBeat(msg *nats.Msg) {
	var bp beatPayload
	if err := json.Unmarshal(msg.Data, &bp); err != nil || bp.ServerID == "" {
		return
	}
	m.peers.note(bp.ServerID, m.now())
}

func (m *Manager) handleMgrPrepare(msg *nats.Msg) {
	var req preparePayload
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		if msg.Reply != "" {
			_ = msg.Respond(m.encodeMgrReply(false))
		}
		return
	}
	ok := m.prepareAccept(req)
	if msg.Reply != "" {
		_ = msg.Respond(m.encodeMgrReply(ok))
	}
}

func (m *Manager) prepareAccept(req preparePayload) bool {
	m.regMu.Lock()
	defer m.regMu.Unlock()

	existing, ok := m.table.Get(req.Account, req.NodeKey)
	if !ok {
		rec := Record{
			ServerID:  req.ServerID,
			ConnName:  req.NodeKey,
			Inbox:     "$P2P.NODE." + req.NodeKey,
			ClaimID:   req.ClaimID,
			ClaimedAt: req.ClaimedAt,
			Committed: false,
		}
		_ = m.table.Claim(req.Account, req.NodeKey, rec)
		return true
	}
	if existing.ClaimID == req.ClaimID {
		return true
	}
	if claimWins(req.ClaimedAt, req.ServerID, existing.ClaimedAt, existing.ServerID) {
		rec := Record{
			ServerID:  req.ServerID,
			ConnName:  req.NodeKey,
			Inbox:     "$P2P.NODE." + req.NodeKey,
			ClaimID:   req.ClaimID,
			ClaimedAt: req.ClaimedAt,
			Committed: false,
		}
		m.table.Release(req.Account, req.NodeKey, existing.ConnName)
		_ = m.table.Claim(req.Account, req.NodeKey, rec)
		return true
	}
	return false
}

func (m *Manager) encodeMgrReply(ok bool) []byte {
	b, _ := json.Marshal(mgrBoolReply{OK: ok})
	return b
}

func (m *Manager) handleMgrCommit(msg *nats.Msg) {
	var req commitPayload
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		if msg.Reply != "" {
			_ = msg.Respond(m.encodeMgrReply(false))
		}
		return
	}
	rec := Record{
		ServerID:  req.ServerID,
		ConnName:  req.ConnName,
		Inbox:     req.Inbox,
		ClaimID:   req.ClaimID,
		ClaimedAt: req.ClaimedAt,
		Committed: true,
	}
	m.applyCommit(req.Account, req.NodeKey, rec)
	if msg.Reply != "" {
		_ = msg.Respond(m.encodeMgrReply(true))
	}
}

func (m *Manager) applyCommit(account, nodeKey string, rec Record) {
	rec.Committed = true
	m.regMu.Lock()
	defer m.regMu.Unlock()

	existing, ok := m.table.Get(account, nodeKey)
	if ok {
		if existing.ClaimID == rec.ClaimID {
			existing.Committed = true
			if rec.Inbox != "" {
				existing.Inbox = rec.Inbox
				existing.ConnName = rec.ConnName
			}
			_ = m.table.Claim(account, nodeKey, existing)
			return
		}
		if !claimWins(rec.ClaimedAt, rec.ServerID, existing.ClaimedAt, existing.ServerID) {
			return
		}
		m.table.Release(account, nodeKey, existing.ConnName)
	}
	_ = m.table.Claim(account, nodeKey, rec)
}

func (m *Manager) handleMgrRelease(msg *nats.Msg) {
	var req releasePayload
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		return
	}
	m.releaseIfMatch(req)
}

func (m *Manager) releaseIfMatch(req releasePayload) bool {
	m.regMu.Lock()
	defer m.regMu.Unlock()

	existing, ok := m.table.Get(req.Account, req.NodeKey)
	if !ok {
		return false
	}
	if req.ClaimID != "" && existing.ClaimID != req.ClaimID {
		return false
	}
	if req.ConnName != "" && existing.ConnName != req.ConnName {
		return false
	}
	return m.table.Release(req.Account, req.NodeKey, existing.ConnName)
}

func (m *Manager) clusterPreparePhase(account, nodeKey, claimID string, claimedAt int64) bool {
	payload := preparePayload{
		Account:   account,
		NodeKey:   nodeKey,
		ClaimID:   claimID,
		ClaimedAt: claimedAt,
		ServerID:  m.serverID,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return false
	}

	alive := m.peers.alive(m.now())
	if !m.inCluster() && len(alive) <= 1 {
		return m.prepareAccept(payload)
	}
	need := len(alive)
	if m.inCluster() {
		if need < 2 {
			need = 2
		}
	} else if need < 1 {
		need = 1
	}

	nc := m.mgrConn()
	inbox := nc.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		return false
	}
	defer sub.Unsubscribe()

	if err := nc.PublishRequest(subjectMgrPrepare, inbox, data); err != nil {
		return false
	}

	deadline := m.now().Add(clusterPrepareTimeout)
	okCount := 0
	gotNACK := false
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		wait := remaining
		if okCount >= need {
			wait = 25 * time.Millisecond
			if wait > remaining {
				wait = remaining
			}
		}
		msg, err := sub.NextMsg(wait)
		if err != nil {
			break
		}
		var rep mgrBoolReply
		if err := json.Unmarshal(msg.Data, &rep); err != nil || !rep.OK {
			gotNACK = true
			break
		}
		okCount++
	}
	if gotNACK || okCount < need {
		m.clusterPrepareRollback(account, nodeKey, claimID)
		return false
	}
	return true
}

func (m *Manager) clusterPrepareRollback(account, nodeKey, claimID string) {
	req := releasePayload{
		Account:  account,
		NodeKey:  nodeKey,
		ConnName: nodeKey,
		ClaimID:  claimID,
	}
	m.releaseIfMatch(req)
	m.publishRelease(account, nodeKey, nodeKey, claimID)
}

func (m *Manager) clusterCommitPhase(account, nodeKey string, rec Record) {
	m.applyCommit(account, nodeKey, rec)
	payload := commitPayload{
		Account:   account,
		NodeKey:   nodeKey,
		ClaimID:   rec.ClaimID,
		ClaimedAt: rec.ClaimedAt,
		ServerID:  rec.ServerID,
		ConnName:  rec.ConnName,
		Inbox:     rec.Inbox,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}
	nc := m.mgrConn()
	if !m.inCluster() {
		_ = nc.Publish(subjectMgrCommit, data)
		return
	}

	inbox := nc.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		return
	}
	defer sub.Unsubscribe()

	if err := nc.PublishRequest(subjectMgrCommit, inbox, data); err != nil {
		return
	}

	alive := m.peers.alive(m.now())
	need := len(alive)
	if need < 2 {
		need = 2
	}
	deadline := m.now().Add(clusterPrepareTimeout)
	okCount := 0
	for okCount < need {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		msg, err := sub.NextMsg(remaining)
		if err != nil {
			break
		}
		var rep mgrBoolReply
		if err := json.Unmarshal(msg.Data, &rep); err != nil || !rep.OK {
			break
		}
		okCount++
	}
}

func (m *Manager) publishRelease(account, nodeKey, connName, claimID string) {
	b, err := json.Marshal(releasePayload{
		Account:  account,
		NodeKey:  nodeKey,
		ConnName: connName,
		ClaimID:  claimID,
	})
	if err != nil {
		return
	}
	_ = m.mgrConn().Publish(subjectMgrRelease, b)
}
