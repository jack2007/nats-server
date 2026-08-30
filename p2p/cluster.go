package p2p

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.clusterStop == nil {
		return
	}
	select {
	case <-m.clusterStop:
	default:
		close(m.clusterStop)
	}
}

func (m *Manager) clusterBeatLoop() {
	stop := m.clusterStop
	ticker := time.NewTicker(clusterBeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			m.publishBeat()
		}
	}
}

func (m *Manager) clusterPruneLoop() {
	stop := m.clusterStop
	ticker := time.NewTicker(clusterBeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
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

const (
	subjectV2MgrSync         = "$P2P.V2.MGR.SYNC"
	subjectV2MgrSnapshot     = "$P2P.V2.MGR.SNAPSHOT"
	subjectV2MgrLost         = "$P2P.V2.MGR.OWNERLOST"
	v2ClusterSyncTimeout     = time.Second
	v2CatchUpDiscoverTimeout = time.Second
	v2MutationRegister       = "register"
	v2MutationSession        = "session"
	v2MutationDelivery       = "delivery"
	v2MutationOwnerLost      = "owner_lost"
	v2MutationLeave          = "leave"
)

func NodeDisconnectGraceDuration(pingInterval time.Duration, pingMax int) time.Duration {
	d := pingInterval * time.Duration(pingMax+1)
	if d < 15*time.Second {
		return 15 * time.Second
	}
	return d
}

func TombstoneDuration(totalTimeout time.Duration) time.Duration {
	d := 2 * totalTimeout
	if d < 60*time.Second {
		return 60 * time.Second
	}
	return d
}

func v2MgrCommandSubject(ownerID string) string {
	return "$P2P.V2.MGR." + ownerID + ".COMMAND"
}

type v2MgrMutation struct {
	MutationID        string                 `json:"mutation_id"`
	MessageID         string                 `json:"message_id"`
	Kind              string                 `json:"kind"`
	SessionID         string                 `json:"session_id,omitempty"`
	Owner             string                 `json:"owner"`
	Revision          uint64                 `json:"revision"`
	Sender            string                 `json:"sender"`
	RequestID         string                 `json:"request_id,omitempty"`
	NodeKey           string                 `json:"node_key,omitempty"`
	RegistrationID    string                 `json:"registration_id,omitempty"`
	RegistrationEpoch uint64                 `json:"registration_epoch,omitempty"`
	CID               uint64                 `json:"cid,omitempty"`
	NatsServerID      string                 `json:"nats_server_id,omitempty"`
	ClientNodeKey     string                 `json:"client_node_key,omitempty"`
	ServerNodeKey     string                 `json:"server_node_key,omitempty"`
	State             string                 `json:"state,omitempty"`
	ConnectionID      string                 `json:"connection_id,omitempty"`
	ConnectionState   string                 `json:"connection_state,omitempty"`
	Connections       []v2ConnectionSnapshot `json:"connections,omitempty"`
	Epoch             uint64                 `json:"epoch,omitempty"`
	SetupTimeoutMs    int64                  `json:"setup_timeout_ms,omitempty"`
	Pending           bool                   `json:"pending,omitempty"`
	Requests          []v2RequestSnapshot    `json:"requests,omitempty"`
}

type v2RequestSnapshot struct {
	SenderNodeKey string               `json:"sender_node_key"`
	RequestID     string               `json:"request_id"`
	SessionID     string               `json:"session_id"`
	Node          NodeSnapshotV2       `json:"node"`
	Session       SessionSnapshotV2    `json:"session"`
	Connection    ConnectionSnapshotV2 `json:"connection"`
	Events        []EventV2            `json:"events,omitempty"`
	Pending       []EventV2            `json:"pending,omitempty"`
	DeliverySeq   uint64               `json:"delivery_seq"`
	HasNode       bool                 `json:"has_node"`
	HasSession    bool                 `json:"has_session"`
	HasConnection bool                 `json:"has_connection"`
	HasEvents     bool                 `json:"has_events"`
}

type v2ConnectionSnapshot struct {
	ConnectionID string `json:"connection_id"`
	Epoch        uint64 `json:"epoch"`
	State        string `json:"state"`
}

type v2MgrAck struct {
	OK         bool   `json:"ok"`
	MutationID string `json:"mutation_id"`
}

type v2ForwardedCmd struct {
	Subject string `json:"subject"`
	Data    []byte `json:"data"`
}

type v2SnapshotReply struct {
	Sender   string          `json:"sender,omitempty"`
	Nodes    []v2MgrMutation `json:"nodes"`
	Sessions []v2MgrMutation `json:"sessions"`
}

type v2SessionMeta struct {
	SessionID       string
	Owner           string
	Revision        uint64
	RequestID       string
	ClientNode      string
	ServerNode      string
	State           SessionStateV2
	ConnectionID    string
	ConnectionState ConnectionStateV2
	Connections     []v2ConnectionSnapshot
	Epoch           uint64
	SetupTimeoutMs  int64
	Lost            bool
}

func (m *Manager) startClusterV2() error {
	nc := m.mgrConn()
	if nc == nil {
		return nil
	}
	subs := []struct {
		subj string
		fn   func(*nats.Msg)
	}{
		{subjectV2MgrSync, m.handleV2MgrSync},
		{subjectV2MgrSnapshot, m.handleV2MgrSnapshot},
		{v2MgrCommandSubject(m.serverID), m.handleV2MgrCommand},
		{subjectV2MgrLost, m.handleV2OwnerLost},
	}
	for _, item := range subs {
		sub, err := nc.Subscribe(item.subj, item.fn)
		if err != nil {
			return err
		}
		m.v2.mgrSubs = append(m.v2.mgrSubs, sub)
	}
	if m.sysNC != nil {
		sub, err := m.sysNC.Subscribe(subjectDisconnect, m.handleDisconnectV2)
		if err != nil {
			return err
		}
		m.v2.mgrSubs = append(m.v2.mgrSubs, sub)
	}
	return nc.Flush()
}

func (m *Manager) catchUpPeerCountV2() int {
	if m.peers == nil {
		return 0
	}
	n := 0
	for _, id := range m.peers.alive(m.now()) {
		if id != m.serverID {
			n++
		}
	}
	return n
}

func (m *Manager) applySnapshotReplyV2(snap v2SnapshotReply) {
	for _, mut := range snap.Nodes {
		_ = m.applyMutationV2(mut)
	}
	for _, mut := range snap.Sessions {
		_ = m.applyMutationV2(mut)
	}
}

func (m *Manager) catchUpV2(ctx context.Context) error {
	if v2TestBeforeCatchUp != nil {
		v2TestBeforeCatchUp(m)
	}
	extra := v2TestCatchUpExtraNeed
	if m.peers == nil {
		return nil
	}
	nc := m.mgrConn()
	inbox := nc.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()
	if err := nc.PublishRequest(subjectV2MgrSnapshot, inbox, []byte(m.serverID)); err != nil {
		return err
	}
	discover := m.v2.catchUpTimeout
	if extra == 0 && !m.inCluster() {
		discover = min(discover, 50*time.Millisecond)
	}
	deadline := time.Now().Add(discover)
	if extra > 0 {
		deadline = time.Now().Add(m.v2.catchUpTimeout)
	}
	seen := make(map[string]struct{})
	peerSnap := false
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		waitCtx, cancel := context.WithTimeout(ctx, remaining)
		msg, err := sub.NextMsgWithContext(waitCtx)
		cancel()
		if err != nil {
			break
		}
		var snap v2SnapshotReply
		if json.Unmarshal(msg.Data, &snap) != nil {
			continue
		}
		if snap.Sender == m.serverID {
			continue
		}
		m.applySnapshotReplyV2(snap)
		seen[snap.Sender] = struct{}{}
		peerSnap = true
		need := m.catchUpPeerCountV2()
		if need < 1 {
			need = 1
		}
		need += extra
		if extra == 0 && len(seen) >= need {
			return nil
		}
	}
	others := m.catchUpPeerCountV2()
	if extra == 0 && others == 0 && !peerSnap {
		return nil
	}
	need := others
	if need < 1 {
		need = 1
	}
	need += extra
	if len(seen) < need {
		return errV2CatchUpIncomplete
	}
	return nil
}

var errV2CatchUpIncomplete = errors.New("v2 catch-up incomplete")

func (m *Manager) joinExternalQueueV2() error {
	m.v2.mu.Lock()
	if m.v2.joinedQueue {
		m.v2.mu.Unlock()
		return nil
	}
	m.v2.mu.Unlock()
	var joined []*nats.Subscription
	for _, suffix := range []string{
		"SESSION.CREATE",
		"SESSION.COMMAND",
		"CONNECTION.COMMAND",
		"SIGNAL.SEND",
		"SIGNAL.ACK",
	} {
		sub, err := m.nc.QueueSubscribe("$P2P.V2.CMD.*."+suffix, v2CoordinatorQueue, m.handleV2Command)
		if err != nil {
			for _, joinedSub := range joined {
				_ = joinedSub.Unsubscribe()
			}
			return err
		}
		joined = append(joined, sub)
	}
	if err := m.nc.Flush(); err != nil {
		for _, sub := range joined {
			_ = sub.Unsubscribe()
		}
		return err
	}
	m.v2.mu.Lock()
	m.v2.cmdSubs = append(m.v2.cmdSubs, joined...)
	m.v2.subs = append(m.v2.subs, joined...)
	m.v2.joinedQueue = true
	m.v2.mu.Unlock()
	return nil
}

func (m *Manager) syncMutationV2(mut v2MgrMutation) error {
	if mut.MutationID == "" {
		id, err := NewUUIDV2()
		if err != nil {
			return err
		}
		mut.MutationID = id
	}
	if mut.MessageID == "" {
		id, err := NewUUIDV2()
		if err != nil {
			return err
		}
		mut.MessageID = id
	}
	mut.Sender = m.serverID
	if !m.applyMutationV2(mut) {
		return errors.New("local apply rejected")
	}
	if v2TestFailDeliverySync.Load() && mut.Kind == v2MutationDelivery {
		return errV2SyncTimeout
	}
	if v2TestFailSessionSync.Load() && mut.Kind == v2MutationSession {
		return errV2SyncTimeout
	}
	if gate, _ := v2TestSessionSyncGate.Load().(*v2SessionSyncGate); gate != nil && mut.Kind == v2MutationSession {
		select {
		case gate.entered <- struct{}{}:
		default:
		}
		select {
		case <-gate.release:
		case <-m.v2.stop:
			return errors.New("stopped")
		}
	}
	alive := []string{m.serverID}
	if m.peers != nil {
		alive = m.peers.alive(m.now())
	}
	need := len(alive)
	if !m.inCluster() && need <= 1 {
		return nil
	}
	if m.inCluster() && need < 2 {
		need = 2
	}
	nc := m.mgrConn()
	data, err := json.Marshal(mut)
	if err != nil {
		return err
	}
	inbox := nc.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()
	if err := nc.PublishRequest(subjectV2MgrSync, inbox, data); err != nil {
		return err
	}
	deadline := m.now().Add(v2ClusterSyncTimeout)
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
		var ack v2MgrAck
		if json.Unmarshal(msg.Data, &ack) != nil || !ack.OK {
			break
		}
		okCount++
	}
	if okCount < need {
		return errV2SyncTimeout
	}
	if mut.Kind == v2MutationRegister && mut.Pending && v2TestHoldRegisterSync != nil &&
		(v2TestHoldRegisterNode == "" || mut.NodeKey == v2TestHoldRegisterNode) {
		select {
		case <-v2TestHoldRegisterSync:
		case <-m.v2.stop:
			return errors.New("stopped")
		}
	}
	return nil
}

var errV2SyncTimeout = errors.New("v2 sync timeout")

func (m *Manager) handleV2MgrSync(msg *nats.Msg) {
	var mut v2MgrMutation
	if json.Unmarshal(msg.Data, &mut) != nil {
		if msg.Reply != "" {
			_ = msg.Respond(m.encodeV2MgrAck(false, ""))
		}
		return
	}
	if v2TestDropSessionSync && mut.Kind == v2MutationSession && mut.Sender != m.serverID &&
		(v2TestDropSessionSyncNode == "" || v2TestDropSessionSyncNode == m.serverID) {
		if msg.Reply != "" {
			_ = msg.Respond(m.encodeV2MgrAck(true, mut.MutationID))
		}
		return
	}
	holdApply := v2TestHoldRegisterApply
	holdNode := v2TestHoldRegisterNode
	if mut.Kind == v2MutationRegister && mut.Sender != m.serverID && holdApply != nil &&
		(holdNode == "" || mut.NodeKey == holdNode) {
		m.noteRegisterPendingV2(mut)
		if msg.Reply != "" {
			_ = msg.Respond(m.encodeV2MgrAck(true, mut.MutationID))
		}
		select {
		case <-holdApply:
		case <-m.v2.stop:
			return
		}
		_ = m.applyMutationV2(mut)
		return
	}
	ok := m.applyMutationV2(mut)
	if msg.Reply != "" {
		_ = msg.Respond(m.encodeV2MgrAck(ok, mut.MutationID))
	}
}

func (m *Manager) noteRegisterPendingV2(mut v2MgrMutation) {
	if m.v2 == nil || mut.NodeKey == "" {
		return
	}
	m.v2.mu.Lock()
	defer m.v2.mu.Unlock()
	if existing, ok := m.v2.nodes[mut.NodeKey]; ok {
		existing.pending = true
		if mut.RegistrationID != "" {
			existing.registrationID = mut.RegistrationID
		}
		return
	}
	m.v2.nodes[mut.NodeKey] = &v2NodeBinding{
		registrationID: mut.RegistrationID,
		epoch:          mut.RegistrationEpoch,
		cid:            mut.CID,
		natsServerID:   mut.NatsServerID,
		requestID:      mut.RequestID,
		pending:        true,
	}
}

func (m *Manager) encodeV2MgrAck(ok bool, mutationID string) []byte {
	b, _ := json.Marshal(v2MgrAck{OK: ok, MutationID: mutationID})
	return b
}

func (m *Manager) handleV2MgrSnapshot(msg *nats.Msg) {
	if m.v2 == nil {
		return
	}
	if string(msg.Data) == m.serverID {
		return
	}
	var nodes []v2MgrMutation
	var sessions []v2MgrMutation
	m.v2.mu.Lock()
	for key, b := range m.v2.nodes {
		nodes = append(nodes, v2MgrMutation{
			MutationID:        "snap-node-" + key + "-" + b.registrationID,
			MessageID:         "snap-node-" + key,
			Kind:              v2MutationRegister,
			Owner:             m.serverID,
			Sender:            m.serverID,
			NodeKey:           key,
			RegistrationID:    b.registrationID,
			RegistrationEpoch: b.epoch,
			CID:               b.cid,
			NatsServerID:      b.natsServerID,
			RequestID:         b.requestID,
			Pending:           b.pending,
		})
	}
	for _, meta := range m.v2.owners {
		sessions = append(sessions, v2MgrMutation{
			MutationID:      "snap-sess-" + meta.SessionID,
			MessageID:       "snap-sess-" + meta.SessionID,
			Kind:            v2MutationSession,
			SessionID:       meta.SessionID,
			Owner:           meta.Owner,
			Revision:        meta.Revision,
			Sender:          m.serverID,
			RequestID:       meta.RequestID,
			ClientNodeKey:   meta.ClientNode,
			ServerNodeKey:   meta.ServerNode,
			State:           string(meta.State),
			ConnectionID:    meta.ConnectionID,
			ConnectionState: string(meta.ConnectionState),
			Connections:     cloneV2ConnectionSnapshots(meta.Connections),
			Epoch:           meta.Epoch,
			SetupTimeoutMs:  meta.SetupTimeoutMs,
		})
	}
	m.v2.mu.Unlock()
	for i := range sessions {
		storeSnapshot, ok := m.v2.store.SyncSnapshotV2(sessions[i].SessionID)
		if ok {
			sessions[i].Revision = storeSnapshot.Session.Revision
			sessions[i].ClientNodeKey = storeSnapshot.Session.ClientNodeKey
			sessions[i].ServerNodeKey = storeSnapshot.Session.ServerNodeKey
			sessions[i].State = string(storeSnapshot.Session.State)
			sessions[i].ConnectionID = storeSnapshot.Session.ConnectionID
			sessions[i].ConnectionState = string(storeSnapshot.Session.ConnectionState)
			sessions[i].Epoch = storeSnapshot.Session.Epoch
			sessions[i].Connections = v2ConnectionSnapshotsFromStore(storeSnapshot.Connections)
			sessions[i].Requests = storeSnapshot.Requests
		}
		id := snapshotSessionMutationIDV2(sessions[i].Sender, sessions[i].SessionID, sessions[i].Revision, sessions[i].Requests)
		sessions[i].MutationID = id
		sessions[i].MessageID = id
	}
	body, err := json.Marshal(v2SnapshotReply{Sender: m.serverID, Nodes: nodes, Sessions: sessions})
	if err != nil || msg.Reply == "" {
		return
	}
	_ = msg.Respond(body)
}

func snapshotSessionMutationIDV2(sender, sessionID string, revision uint64, requests []v2RequestSnapshot) string {
	raw, _ := json.Marshal(requests)
	digest := sha256.Sum256(raw)
	return fmt.Sprintf("snap-sess-%s-%s-%d-%x", sender, sessionID, revision, digest[:8])
}

func (m *Manager) handleV2MgrCommand(msg *nats.Msg) {
	if m.v2 == nil {
		return
	}
	m.v2.mu.Lock()
	blocked := m.v2.unreachable
	m.v2.mu.Unlock()
	if blocked {
		return
	}
	var fwd v2ForwardedCmd
	if json.Unmarshal(msg.Data, &fwd) != nil || fwd.Subject == "" {
		return
	}
	msg.Subject = fwd.Subject
	msg.Data = fwd.Data
	select {
	case m.v2.cmdQ <- msg:
	default:
		ms := RetryAfterMsV2(0)
		m.replyV2Error(msg, requestIDFromData(msg.Data), ErrBusyV2, &ms)
	}
}

func (m *Manager) handleV2OwnerLost(msg *nats.Msg) {
	var mut v2MgrMutation
	if json.Unmarshal(msg.Data, &mut) != nil {
		return
	}
	_ = m.applyMutationV2(mut)
}

func (m *Manager) applyMutationV2(mut v2MgrMutation) bool {
	if m.v2 == nil || mut.MutationID == "" {
		return false
	}
	m.v2.mu.Lock()
	if _, ok := m.v2.applied[mut.MutationID]; ok {
		m.v2.mu.Unlock()
		return true
	}
	switch mut.Kind {
	case v2MutationRegister:
		m.v2.mu.Unlock()
		if mut.NodeKey == "" || mut.RequestID == "" {
			return false
		}
		if _, err := m.v2.store.RegisterNode(RegisterCommandV2{
			RequestID:      mut.RequestID,
			RegistrationID: mut.RegistrationID,
			NodeKey:        mut.NodeKey,
		}); err != nil {
			return false
		}
		m.v2.mu.Lock()
		if existing, ok := m.v2.nodes[mut.NodeKey]; ok {
			if existing.registrationID == mut.RegistrationID && existing.cid != 0 && mut.CID != 0 && existing.cid != mut.CID {
				m.v2.mu.Unlock()
				return false
			}
		}
		m.v2.nodes[mut.NodeKey] = &v2NodeBinding{
			registrationID: mut.RegistrationID,
			epoch:          mut.RegistrationEpoch,
			cid:            mut.CID,
			natsServerID:   mut.NatsServerID,
			requestID:      mut.RequestID,
			pending:        mut.Pending,
		}
		m.v2.applied[mut.MutationID] = struct{}{}
		m.v2.mu.Unlock()
		return true
	case v2MutationSession:
		if meta, ok := m.v2.owners[mut.SessionID]; ok {
			if meta.Owner != "" && mut.Owner != "" && meta.Owner != mut.Owner {
				m.v2.mu.Unlock()
				return false
			}
			if mut.Revision < meta.Revision {
				m.v2.mu.Unlock()
				return false
			}
			if mut.Revision == meta.Revision && len(mut.Connections) == 0 {
				m.v2.applied[mut.MutationID] = struct{}{}
				m.v2.mu.Unlock()
				return true
			}
		}
		m.v2.mu.Unlock()
		m.v2.store.importSessionReplicaV2(mut)
		m.v2.mu.Lock()
		meta := m.v2.owners[mut.SessionID]
		if meta == nil {
			meta = &v2SessionMeta{}
			m.v2.owners[mut.SessionID] = meta
		}
		meta.SessionID = mut.SessionID
		meta.Owner = mut.Owner
		meta.Revision = mut.Revision
		meta.RequestID = mut.RequestID
		meta.ClientNode = mut.ClientNodeKey
		meta.ServerNode = mut.ServerNodeKey
		meta.State = SessionStateV2(mut.State)
		meta.ConnectionID = mut.ConnectionID
		meta.ConnectionState = ConnectionStateV2(mut.ConnectionState)
		meta.Connections = cloneV2ConnectionSnapshots(mutationConnectionsV2(mut))
		meta.Epoch = mut.Epoch
		meta.SetupTimeoutMs = mut.SetupTimeoutMs
		if mut.RequestID != "" && mut.ClientNodeKey != "" {
			m.v2.requests[RequestKeyV2{SenderNodeKey: mut.ClientNodeKey, RequestID: mut.RequestID}] = mut.SessionID
		}
		m.v2.applied[mut.MutationID] = struct{}{}
		m.v2.mu.Unlock()
		return true
	case v2MutationDelivery:
		if meta, ok := m.v2.owners[mut.SessionID]; ok && meta.Owner != "" && mut.Owner != "" && meta.Owner != mut.Owner {
			m.v2.mu.Unlock()
			return false
		}
		m.v2.mu.Unlock()
		m.v2.store.importRequestSnapshotsV2(mut.Requests)
		m.v2.mu.Lock()
		m.v2.applied[mut.MutationID] = struct{}{}
		m.v2.mu.Unlock()
		return true
	case v2MutationOwnerLost:
		m.v2.mu.Unlock()
		m.applyOwnerLostV2(mut)
		m.v2.mu.Lock()
		m.v2.applied[mut.MutationID] = struct{}{}
		m.v2.mu.Unlock()
		return true
	case v2MutationLeave:
		if mut.Sender != "" && mut.Sender != m.serverID {
			if m.peers != nil {
				m.peers.mu.Lock()
				delete(m.peers.lastBeat, mut.Sender)
				m.peers.mu.Unlock()
			}
			m.regMu.Lock()
			m.table.DropServer(mut.Sender)
			m.regMu.Unlock()
		}
		m.v2.applied[mut.MutationID] = struct{}{}
		m.v2.mu.Unlock()
		return true
	default:
		m.v2.mu.Unlock()
		return false
	}
}

func (m *Manager) applyOwnerLostV2(mut v2MgrMutation) {
	m.v2.mu.Lock()
	meta := m.v2.owners[mut.SessionID]
	if meta != nil {
		meta.Lost = true
		meta.Owner = ""
	}
	m.v2.mu.Unlock()
	events := m.v2.store.failSessionIfNegotiatingV2(mut.SessionID)
	if len(events) > 0 {
		_ = m.publishEventsV2(events)
	}
}

func (m *Manager) publishOwnerExitV2() {
	if m.v2 == nil {
		return
	}
	m.v2.mu.Lock()
	var lost []v2MgrMutation
	for _, meta := range m.v2.owners {
		if meta.Owner != m.serverID || meta.Lost {
			continue
		}
		id, _ := NewUUIDV2()
		mid, _ := NewUUIDV2()
		lost = append(lost, v2MgrMutation{
			MutationID: id,
			MessageID:  mid,
			Kind:       v2MutationOwnerLost,
			SessionID:  meta.SessionID,
			Owner:      m.serverID,
			Revision:   meta.Revision,
			Sender:     m.serverID,
			State:      string(meta.State),
		})
	}
	m.v2.mu.Unlock()
	nc := m.mgrConn()
	for _, mut := range lost {
		_ = m.applyMutationV2(mut)
		if nc == nil {
			continue
		}
		data, err := json.Marshal(mut)
		if err != nil {
			continue
		}
		_ = nc.Publish(subjectV2MgrLost, data)
	}
	if nc != nil {
		id, _ := NewUUIDV2()
		mid, _ := NewUUIDV2()
		leave, _ := json.Marshal(v2MgrMutation{
			MutationID: id,
			MessageID:  mid,
			Kind:       v2MutationLeave,
			Sender:     m.serverID,
			Owner:      m.serverID,
		})
		_ = nc.Publish(subjectV2MgrLost, leave)
	}
}

func (m *Manager) handleDisconnectV2(msg *nats.Msg) {
	var ev struct {
		Server struct {
			ID string `json:"id"`
		} `json:"server"`
		Client struct {
			Name string `json:"name"`
			ID   uint64 `json:"id"`
			Cid  uint64 `json:"cid"`
		} `json:"client"`
	}
	if json.Unmarshal(msg.Data, &ev) != nil {
		return
	}
	node := ev.Client.Name
	cid := ev.Client.Cid
	if cid == 0 {
		cid = ev.Client.ID
	}
	serverID := ev.Server.ID
	if node == "" || serverID == "" || cid == 0 {
		return
	}
	m.v2.mu.Lock()
	b, ok := m.v2.nodes[node]
	if !ok || b.natsServerID != serverID || b.cid != cid {
		m.v2.mu.Unlock()
		return
	}
	m.v2.mu.Unlock()
	m.v2.store.MarkNodeDisconnected(node)
}

func (m *Manager) syncSessionLockedV2(sessionID, requestID, sender string) error {
	storeSnapshot, ok := m.v2.store.SyncSnapshotV2(sessionID)
	if !ok {
		return nil
	}
	return m.syncSessionSnapshotV2(storeSnapshot, requestID, sender)
}

func (m *Manager) syncSessionSnapshotV2(storeSnapshot StoreSyncSnapshotV2, requestID, sender string) error {
	snap := storeSnapshot.Session
	m.v2.mu.Lock()
	meta := m.v2.owners[snap.SessionID]
	if meta == nil {
		meta = &v2SessionMeta{SessionID: snap.SessionID, Owner: m.serverID}
		m.v2.owners[snap.SessionID] = meta
	}
	meta.Revision = snap.Revision
	meta.State = snap.State
	meta.ClientNode = snap.ClientNodeKey
	meta.ServerNode = snap.ServerNodeKey
	meta.ConnectionID = snap.ConnectionID
	meta.ConnectionState = snap.ConnectionState
	meta.Epoch = snap.Epoch
	meta.Connections = v2ConnectionSnapshotsFromStore(storeSnapshot.Connections)
	if requestID != "" && meta.RequestID == "" {
		meta.RequestID = requestID
	}
	if sender != "" && meta.ClientNode == "" {
		meta.ClientNode = sender
	}
	mut := v2MgrMutation{
		Kind:            v2MutationSession,
		SessionID:       snap.SessionID,
		Owner:           meta.Owner,
		Revision:        meta.Revision,
		RequestID:       meta.RequestID,
		ClientNodeKey:   meta.ClientNode,
		ServerNodeKey:   meta.ServerNode,
		State:           string(meta.State),
		ConnectionID:    meta.ConnectionID,
		ConnectionState: string(meta.ConnectionState),
		Connections:     cloneV2ConnectionSnapshots(meta.Connections),
		Epoch:           meta.Epoch,
		SetupTimeoutMs:  meta.SetupTimeoutMs,
		Requests:        storeSnapshot.Requests,
	}
	m.v2.mu.Unlock()
	return m.syncMutationV2(mut)
}

func (m *Manager) syncRequestDeliveriesV2(sessionID string) error {
	requests := m.v2.store.requestSnapshotsV2(sessionID)
	m.v2.mu.Lock()
	meta := m.v2.owners[sessionID]
	owner := m.serverID
	if meta != nil && meta.Owner != "" {
		owner = meta.Owner
	}
	m.v2.mu.Unlock()
	return m.syncMutationV2(v2MgrMutation{
		Kind:      v2MutationDelivery,
		SessionID: sessionID,
		Owner:     owner,
		Requests:  requests,
	})
}

func v2ConnectionSnapshotsFromStore(connections []ConnectionSnapshotV2) []v2ConnectionSnapshot {
	out := make([]v2ConnectionSnapshot, 0, len(connections))
	for _, conn := range connections {
		out = append(out, v2ConnectionSnapshot{ConnectionID: conn.ConnectionID, Epoch: conn.Epoch, State: string(conn.State)})
	}
	return cloneV2ConnectionSnapshots(out)
}

func cloneV2ConnectionSnapshots(connections []v2ConnectionSnapshot) []v2ConnectionSnapshot {
	if len(connections) == 0 {
		return nil
	}
	out := append([]v2ConnectionSnapshot(nil), connections...)
	sort.Slice(out, func(i, j int) bool { return out[i].ConnectionID < out[j].ConnectionID })
	return out
}

func mutationConnectionsV2(mut v2MgrMutation) []v2ConnectionSnapshot {
	if len(mut.Connections) != 0 {
		return cloneV2ConnectionSnapshots(mut.Connections)
	}
	connectionID := mut.ConnectionID
	if connectionID == "" {
		connectionID = "conn-0"
	}
	epoch := mut.Epoch
	if epoch == 0 {
		epoch = 1
	}
	state := mut.ConnectionState
	if state == "" {
		state = string(ConnectionStateAllocatedV2)
	}
	return []v2ConnectionSnapshot{{ConnectionID: connectionID, Epoch: epoch, State: state}}
}

func (s *StoreV2) importSessionReplicaV2(mut v2MgrMutation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.importRequestSnapshotsLockedV2(mut.Requests)
	sess, ok := s.sessions[mut.SessionID]
	if !ok {
		if mut.SessionID == "" {
			return
		}
		timeout := DefaultSetupTimeoutV2
		if mut.SetupTimeoutMs > 0 {
			timeout = time.Duration(mut.SetupTimeoutMs) * time.Millisecond
		}
		sess = &sessionRecordV2{
			id:            mut.SessionID,
			state:         SessionStateV2(mut.State),
			revision:      mut.Revision,
			client:        mut.ClientNodeKey,
			server:        mut.ServerNodeKey,
			setupTimeout:  timeout,
			setupDeadline: s.now().Add(timeout),
			connections:   make(map[string]*connectionRecordV2),
		}
		if sess.state == "" {
			sess.state = SessionStateAllocatedV2
		}
		for _, snapshot := range mutationConnectionsV2(mut) {
			sess.connections[snapshot.ConnectionID] = replicaConnectionV2(snapshot, sess.setupDeadline)
		}
		s.sessions[mut.SessionID] = sess
		if mut.RequestID != "" && mut.ClientNodeKey != "" {
			s.requests[RequestKeyV2{SenderNodeKey: mut.ClientNodeKey, RequestID: mut.RequestID}] = requestRecordV2{
				session:   sessionSnapV2(sess),
				sessionID: sess.id,
				hasSess:   true,
			}
		}
		return
	}
	if mut.Revision >= sess.revision {
		sess.revision = mut.Revision
		if mut.State != "" {
			sess.state = SessionStateV2(mut.State)
		}
		for _, snapshot := range mutationConnectionsV2(mut) {
			conn := sess.connections[snapshot.ConnectionID]
			if conn == nil {
				sess.connections[snapshot.ConnectionID] = replicaConnectionV2(snapshot, sess.setupDeadline)
				continue
			}
			conn.epoch = snapshot.Epoch
			conn.state = ConnectionStateV2(snapshot.State)
		}
	}
}

func replicaConnectionV2(snapshot v2ConnectionSnapshot, deadline time.Time) *connectionRecordV2 {
	epoch := snapshot.Epoch
	if epoch == 0 {
		epoch = 1
	}
	state := ConnectionStateV2(snapshot.State)
	if state == "" {
		state = ConnectionStateAllocatedV2
	}
	return &connectionRecordV2{id: snapshot.ConnectionID, epoch: epoch, state: state, ready: make(map[string]bool), setupDeadline: deadline}
}

func (s *StoreV2) failSessionIfNegotiatingV2(id string) []EventV2 {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[id]
	if !ok {
		return nil
	}
	if sess.state == SessionStateActiveV2 {
		return nil
	}
	return s.failSessionLocked(sess)
}

func sessionIDFromV2Data(data []byte) string {
	var env EnvelopeV2
	if json.Unmarshal(data, &env) != nil {
		return ""
	}
	var payload struct {
		SessionID string `json:"session_id"`
	}
	if json.Unmarshal(env.Payload, &payload) != nil {
		return ""
	}
	return payload.SessionID
}
