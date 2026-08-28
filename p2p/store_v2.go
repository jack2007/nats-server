package p2p

import (
	"fmt"
	"sync"
	"time"
)

const (
	DefaultSetupTimeoutV2 = 10 * time.Second
	NodeDisconnectGraceV2 = 15 * time.Second
	DefaultTombstoneTTLV2 = 60 * time.Second

	ConnectionStateAllocatedV2 ConnectionStateV2 = "allocated"
	FrameKindCloseV2           FrameKindV2       = "CLOSE"
)

type GenerationKeyV2 struct {
	SessionID, ConnectionID string
	Epoch                   uint64
}

type DirectionKeyV2 struct {
	GenerationKeyV2
	SenderNodeKey string
}

type RequestKeyV2 struct {
	SenderNodeKey, RequestID string
}

type MessageKeyV2 struct {
	Direction DirectionKeyV2
	MessageID string
}

type NodeSnapshotV2 struct {
	NodeKey           string
	RegistrationID    string
	RegistrationEpoch uint64
}

type SessionSnapshotV2 struct {
	SessionID     string
	ConnectionID  string
	Epoch         uint64
	State         SessionStateV2
	Revision      uint64
	ClientNodeKey string
	ServerNodeKey string
}

type ConnectionSnapshotV2 struct {
	SessionID    string
	ConnectionID string
	Epoch        uint64
	State        ConnectionStateV2
	Revision     uint64
}

type EventV2 struct {
	TargetNodeKey  string
	RegistrationID string
	Kind           FrameKindV2
	MessageID      string
	Identity       IdentityV2
	Revision       uint64
	Role           string
	PeerNodeKey    string
	Prepare        *PrepareEventV2
	Start          *StartEventV2
}

type StoreV2 struct {
	mu       sync.Mutex
	now      func() time.Time
	maxConns int
	nodes    map[string]*nodeRecordV2
	sessions map[string]*sessionRecordV2
	tombs    map[string]*sessionRecordV2
	requests map[RequestKeyV2]requestRecordV2
	messages map[MessageKeyV2]struct{}
}

type nodeRecordV2 struct {
	key               string
	registrationID    string
	registrationEpoch uint64
	disconnectedAt    *time.Time
}

type connectionRecordV2 struct {
	id            string
	epoch         uint64
	state         ConnectionStateV2
	ready         map[string]bool
	setupDeadline time.Time
	bindEvents    []EventV2
	readyEvents   []EventV2
	closeEvents   []EventV2
}

type sessionRecordV2 struct {
	id            string
	state         SessionStateV2
	revision      uint64
	client        string
	server        string
	setupDeadline time.Time
	closedAt      time.Time
	connections   map[string]*connectionRecordV2
	closeEvents   []EventV2
}

type requestRecordV2 struct {
	node    NodeSnapshotV2
	session SessionSnapshotV2
	conn    ConnectionSnapshotV2
	events  []EventV2
	hasNode bool
	hasSess bool
	hasConn bool
	hasEv   bool
}

func NewStoreV2(now func() time.Time, maxConnectionsPerSession int) *StoreV2 {
	if now == nil {
		now = time.Now
	}
	if maxConnectionsPerSession <= 0 {
		maxConnectionsPerSession = 128
	}
	return &StoreV2{
		now:      now,
		maxConns: maxConnectionsPerSession,
		nodes:    make(map[string]*nodeRecordV2),
		sessions: make(map[string]*sessionRecordV2),
		tombs:    make(map[string]*sessionRecordV2),
		requests: make(map[RequestKeyV2]requestRecordV2),
		messages: make(map[MessageKeyV2]struct{}),
	}
}

func (s *StoreV2) RegisterNode(cmd RegisterCommandV2) (NodeSnapshotV2, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ValidateNodeKeyV2(cmd.NodeKey); err != nil {
		return NodeSnapshotV2{}, err
	}
	if err := ValidateUUIDV2(cmd.RequestID); err != nil {
		return NodeSnapshotV2{}, err
	}
	if err := ValidateRegistrationIDV2(cmd.RegistrationID); err != nil {
		return NodeSnapshotV2{}, err
	}
	key := RequestKeyV2{SenderNodeKey: cmd.NodeKey, RequestID: cmd.RequestID}
	if rec, ok := s.requests[key]; ok && rec.hasNode {
		return rec.node, nil
	}
	n, ok := s.nodes[cmd.NodeKey]
	if !ok {
		n = &nodeRecordV2{key: cmd.NodeKey}
		s.nodes[cmd.NodeKey] = n
	}
	n.registrationID = cmd.RegistrationID
	n.registrationEpoch++
	n.disconnectedAt = nil
	snap := NodeSnapshotV2{
		NodeKey:           n.key,
		RegistrationID:    n.registrationID,
		RegistrationEpoch: n.registrationEpoch,
	}
	s.requests[key] = requestRecordV2{node: snap, hasNode: true}
	return snap, nil
}

func (s *StoreV2) AllocateSession(sender string, cmd CreateSessionCommandV2) (SessionSnapshotV2, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ValidateUUIDV2(cmd.RequestID); err != nil {
		return SessionSnapshotV2{}, err
	}
	if err := ValidateNodeKeyV2(cmd.ServerNodeKey); err != nil {
		return SessionSnapshotV2{}, err
	}
	key := RequestKeyV2{SenderNodeKey: sender, RequestID: cmd.RequestID}
	if rec, ok := s.requests[key]; ok && rec.hasSess {
		return rec.session, nil
	}
	if _, err := s.requireNode(sender); err != nil {
		return SessionSnapshotV2{}, err
	}
	if _, err := s.requireNode(cmd.ServerNodeKey); err != nil {
		if pe, ok := err.(*ProtocolErrorV2); ok && pe.Code == ErrNotRegisteredV2 {
			return SessionSnapshotV2{}, codeErrV2(ErrPeerNotRegisteredV2)
		}
		return SessionSnapshotV2{}, err
	}
	if sender == cmd.ServerNodeKey {
		return SessionSnapshotV2{}, invalidRequestV2()
	}
	sid, err := NewUUIDV2()
	if err != nil {
		return SessionSnapshotV2{}, codeErrV2(ErrInternalErrorV2)
	}
	now := s.now()
	sess := &sessionRecordV2{
		id:            sid,
		state:         SessionStateAllocatedV2,
		revision:      1,
		client:        sender,
		server:        cmd.ServerNodeKey,
		setupDeadline: now.Add(DefaultSetupTimeoutV2),
		connections:   make(map[string]*connectionRecordV2),
	}
	sess.connections["conn-0"] = &connectionRecordV2{
		id:            "conn-0",
		epoch:         1,
		state:         ConnectionStateAllocatedV2,
		ready:         make(map[string]bool),
		setupDeadline: sess.setupDeadline,
	}
	s.sessions[sid] = sess
	snap := sessionSnapV2(sess)
	s.requests[key] = requestRecordV2{session: snap, hasSess: true}
	return snap, nil
}

func (s *StoreV2) BindSession(sender string, cmd SessionCommandV2) ([]EventV2, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ValidateUUIDV2(cmd.RequestID); err != nil {
		return nil, err
	}
	key := RequestKeyV2{SenderNodeKey: sender, RequestID: cmd.RequestID}
	if rec, ok := s.requests[key]; ok && rec.hasEv {
		return cloneEventsV2(rec.events), nil
	}
	if _, err := s.requireNode(sender); err != nil {
		return nil, err
	}
	sess, err := s.requireSession(cmd.SessionID)
	if err != nil {
		return nil, err
	}
	if !sess.member(sender) {
		return nil, codeErrV2(ErrNotSessionMemberV2)
	}
	switch cmd.Command {
	case SessionCommandBindV2:
		if err := s.checkEpoch(sess, cmd.ConnectionID, cmd.Epoch); err != nil {
			return nil, err
		}
		if sess.state != SessionStateAllocatedV2 {
			return nil, codeErrV2(ErrInvalidStateV2)
		}
		conn := sess.connections["conn-0"]
		if conn == nil {
			return nil, codeErrV2(ErrConnectionNotFoundV2)
		}
		if conn.state != ConnectionStateAllocatedV2 {
			return nil, codeErrV2(ErrInvalidStateV2)
		}
		sess.revision++
		sess.state = SessionStatePreparingV2
		conn.state = ConnectionStatePreparingV2
		events := s.makePrepareEvents(sess, conn)
		conn.bindEvents = events
		s.requests[key] = requestRecordV2{events: cloneEventsV2(events), hasEv: true}
		return events, nil
	case SessionCommandCloseV2:
		events := s.closeSessionLocked(sess)
		s.requests[key] = requestRecordV2{events: cloneEventsV2(events), hasEv: true}
		return events, nil
	case SessionCommandResumeV2:
		s.requests[key] = requestRecordV2{hasEv: true}
		return nil, nil
	default:
		return nil, invalidRequestV2()
	}
}

func (s *StoreV2) MarkConnectionReady(sender string, cmd ConnectionCommandV2) ([]EventV2, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ValidateUUIDV2(cmd.RequestID); err != nil {
		return nil, err
	}
	key := RequestKeyV2{SenderNodeKey: sender, RequestID: cmd.RequestID}
	if rec, ok := s.requests[key]; ok && rec.hasEv {
		return cloneEventsV2(rec.events), nil
	}
	if _, err := s.requireNode(sender); err != nil {
		return nil, err
	}
	sess, conn, err := s.requireMemberConn(sender, cmd)
	if err != nil {
		return nil, err
	}
	if conn.state != ConnectionStatePreparingV2 {
		return nil, codeErrV2(ErrInvalidStateV2)
	}
	conn.ready[sender] = true
	sess.revision++
	var events []EventV2
	if conn.ready[sess.client] && conn.ready[sess.server] {
		conn.state = ConnectionStateActiveV2
		if sess.state == SessionStatePreparingV2 || sess.state == SessionStateAllocatedV2 {
			sess.state = SessionStateActiveV2
		}
		events = s.makeStartEvents(sess, conn)
		conn.readyEvents = events
	}
	s.requests[key] = requestRecordV2{events: cloneEventsV2(events), hasEv: true}
	return events, nil
}

func (s *StoreV2) AllocateConnection(sender string, cmd ConnectionCommandV2) (ConnectionSnapshotV2, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ValidateUUIDV2(cmd.RequestID); err != nil {
		return ConnectionSnapshotV2{}, err
	}
	key := RequestKeyV2{SenderNodeKey: sender, RequestID: cmd.RequestID}
	if rec, ok := s.requests[key]; ok && rec.hasConn {
		return rec.conn, nil
	}
	if _, err := s.requireNode(sender); err != nil {
		return ConnectionSnapshotV2{}, err
	}
	sess, err := s.requireSession(cmd.SessionID)
	if err != nil {
		return ConnectionSnapshotV2{}, err
	}
	if !sess.member(sender) {
		return ConnectionSnapshotV2{}, codeErrV2(ErrNotSessionMemberV2)
	}
	if sess.state == SessionStateClosedV2 || sess.state == SessionStateFailedV2 {
		return ConnectionSnapshotV2{}, codeErrV2(ErrInvalidStateV2)
	}
	if len(sess.connections) >= s.maxConns {
		return ConnectionSnapshotV2{}, codeErrV2(ErrConnectionLimitV2)
	}
	id := cmd.ConnectionID
	if id == "" {
		id = nextConnIDV2(sess)
	} else if err := ValidateConnectionIDV2(id); err != nil {
		return ConnectionSnapshotV2{}, err
	}
	if _, exists := sess.connections[id]; exists {
		return ConnectionSnapshotV2{}, codeErrV2(ErrInvalidStateV2)
	}
	sess.revision++
	conn := &connectionRecordV2{
		id:            id,
		epoch:         1,
		state:         ConnectionStateAllocatedV2,
		ready:         make(map[string]bool),
		setupDeadline: s.now().Add(DefaultSetupTimeoutV2),
	}
	sess.connections[id] = conn
	snap := connSnapV2(sess, conn)
	s.requests[key] = requestRecordV2{conn: snap, hasConn: true}
	return snap, nil
}

func (s *StoreV2) AllocateRestart(sender string, cmd ConnectionCommandV2) (ConnectionSnapshotV2, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ValidateUUIDV2(cmd.RequestID); err != nil {
		return ConnectionSnapshotV2{}, err
	}
	key := RequestKeyV2{SenderNodeKey: sender, RequestID: cmd.RequestID}
	if rec, ok := s.requests[key]; ok && rec.hasConn {
		return rec.conn, nil
	}
	if _, err := s.requireNode(sender); err != nil {
		return ConnectionSnapshotV2{}, err
	}
	sess, err := s.requireSession(cmd.SessionID)
	if err != nil {
		return ConnectionSnapshotV2{}, err
	}
	if !sess.member(sender) {
		return ConnectionSnapshotV2{}, codeErrV2(ErrNotSessionMemberV2)
	}
	conn, ok := sess.connections[cmd.ConnectionID]
	if !ok {
		return ConnectionSnapshotV2{}, codeErrV2(ErrConnectionNotFoundV2)
	}
	if conn.state == ConnectionStateClosedV2 {
		return ConnectionSnapshotV2{}, codeErrV2(ErrInvalidStateV2)
	}
	if cmd.Epoch != 0 {
		if cmd.Epoch < conn.epoch {
			return ConnectionSnapshotV2{}, codeErrV2(ErrStaleEpochV2)
		}
		if cmd.Epoch > conn.epoch {
			return ConnectionSnapshotV2{}, codeErrV2(ErrFutureEpochV2)
		}
	}
	sess.revision++
	conn.epoch++
	conn.state = ConnectionStateAllocatedV2
	conn.ready = make(map[string]bool)
	conn.bindEvents = nil
	conn.readyEvents = nil
	conn.setupDeadline = s.now().Add(DefaultSetupTimeoutV2)
	snap := connSnapV2(sess, conn)
	s.requests[key] = requestRecordV2{conn: snap, hasConn: true}
	return snap, nil
}

func (s *StoreV2) BindConnection(sender string, cmd ConnectionCommandV2) ([]EventV2, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ValidateUUIDV2(cmd.RequestID); err != nil {
		return nil, err
	}
	key := RequestKeyV2{SenderNodeKey: sender, RequestID: cmd.RequestID}
	if rec, ok := s.requests[key]; ok && rec.hasEv {
		return cloneEventsV2(rec.events), nil
	}
	if _, err := s.requireNode(sender); err != nil {
		return nil, err
	}
	sess, conn, err := s.requireMemberConn(sender, cmd)
	if err != nil {
		return nil, err
	}
	if conn.state != ConnectionStateAllocatedV2 && conn.state != ConnectionStateRestartingV2 {
		return nil, codeErrV2(ErrInvalidStateV2)
	}
	sess.revision++
	conn.state = ConnectionStatePreparingV2
	events := s.makePrepareEvents(sess, conn)
	conn.bindEvents = events
	s.requests[key] = requestRecordV2{events: cloneEventsV2(events), hasEv: true}
	return events, nil
}

func (s *StoreV2) CloseConnection(sender string, cmd ConnectionCommandV2) ([]EventV2, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ValidateUUIDV2(cmd.RequestID); err != nil {
		return nil, err
	}
	key := RequestKeyV2{SenderNodeKey: sender, RequestID: cmd.RequestID}
	if rec, ok := s.requests[key]; ok && rec.hasEv {
		return cloneEventsV2(rec.events), nil
	}
	if _, err := s.requireNode(sender); err != nil {
		return nil, err
	}
	sess, conn, err := s.requireMemberConn(sender, cmd)
	if err != nil {
		return nil, err
	}
	if conn.state == ConnectionStateClosedV2 {
		s.requests[key] = requestRecordV2{events: cloneEventsV2(conn.closeEvents), hasEv: true}
		return cloneEventsV2(conn.closeEvents), nil
	}
	sess.revision++
	conn.state = ConnectionStateClosedV2
	events := s.notifyBoth(sess, conn, FrameKindCloseV2)
	conn.closeEvents = events
	s.requests[key] = requestRecordV2{events: cloneEventsV2(events), hasEv: true}
	return events, nil
}

func (s *StoreV2) Expire(now time.Time) []EventV2 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var events []EventV2
	for _, sess := range s.sessions {
		if sess.state == SessionStateAllocatedV2 || sess.state == SessionStatePreparingV2 {
			if !now.Before(sess.setupDeadline) {
				events = append(events, s.failSessionLocked(sess)...)
			}
		}
	}
	for key, n := range s.nodes {
		if n.disconnectedAt != nil && !now.Before(n.disconnectedAt.Add(NodeDisconnectGraceV2)) {
			delete(s.nodes, key)
		}
	}
	for id, sess := range s.tombs {
		if !sess.closedAt.IsZero() && !now.Before(sess.closedAt.Add(DefaultTombstoneTTLV2)) {
			s.dropSessionRequests(id)
			delete(s.tombs, id)
		}
	}
	return events
}

func (s *StoreV2) MarkNodeDisconnected(nodeKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[nodeKey]
	if !ok {
		return
	}
	t := s.now()
	n.disconnectedAt = &t
}

func (s *StoreV2) RememberMessage(key MessageKeyV2) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.messages[key]; ok {
		return false
	}
	s.messages[key] = struct{}{}
	return true
}

func (s *StoreV2) requireNode(key string) (*nodeRecordV2, error) {
	n, ok := s.nodes[key]
	if !ok {
		return nil, codeErrV2(ErrNotRegisteredV2)
	}
	return n, nil
}

func (s *StoreV2) requireSession(id string) (*sessionRecordV2, error) {
	if sess, ok := s.sessions[id]; ok {
		return sess, nil
	}
	return nil, codeErrV2(ErrSessionNotFoundV2)
}

func (s *StoreV2) requireMemberConn(sender string, cmd ConnectionCommandV2) (*sessionRecordV2, *connectionRecordV2, error) {
	sess, err := s.requireSession(cmd.SessionID)
	if err != nil {
		return nil, nil, err
	}
	if !sess.member(sender) {
		return nil, nil, codeErrV2(ErrNotSessionMemberV2)
	}
	conn, ok := sess.connections[cmd.ConnectionID]
	if !ok {
		return nil, nil, codeErrV2(ErrConnectionNotFoundV2)
	}
	if err := s.checkEpoch(sess, cmd.ConnectionID, cmd.Epoch); err != nil {
		return nil, nil, err
	}
	return sess, conn, nil
}

func (s *StoreV2) checkEpoch(sess *sessionRecordV2, connID string, epoch uint64) error {
	if epoch == 0 {
		return invalidRequestV2()
	}
	conn, ok := sess.connections[connID]
	if !ok {
		return codeErrV2(ErrConnectionNotFoundV2)
	}
	if epoch < conn.epoch {
		return codeErrV2(ErrStaleEpochV2)
	}
	if epoch > conn.epoch {
		return codeErrV2(ErrFutureEpochV2)
	}
	return nil
}

func (s *StoreV2) closeSessionLocked(sess *sessionRecordV2) []EventV2 {
	if sess.state == SessionStateClosedV2 {
		return cloneEventsV2(sess.closeEvents)
	}
	sess.revision++
	sess.state = SessionStateClosedV2
	for _, conn := range sess.connections {
		conn.state = ConnectionStateClosedV2
	}
	conn := sess.connections["conn-0"]
	events := s.notifyBoth(sess, conn, FrameKindCloseV2)
	sess.closeEvents = events
	s.buryLocked(sess)
	return events
}

func (s *StoreV2) failSessionLocked(sess *sessionRecordV2) []EventV2 {
	if _, ok := s.sessions[sess.id]; !ok {
		return nil
	}
	sess.state = SessionStateFailedV2
	for _, conn := range sess.connections {
		conn.state = ConnectionStateClosedV2
	}
	events := s.notifyBoth(sess, sess.connections["conn-0"], FrameKindErrorV2)
	s.buryLocked(sess)
	return events
}

func (s *StoreV2) buryLocked(sess *sessionRecordV2) {
	sess.closedAt = s.now()
	delete(s.sessions, sess.id)
	s.tombs[sess.id] = sess
}

func (s *StoreV2) dropSessionRequests(sessionID string) {
	for key, rec := range s.requests {
		if rec.hasSess && rec.session.SessionID == sessionID {
			delete(s.requests, key)
		}
	}
}

func (s *StoreV2) makePrepareEvents(sess *sessionRecordV2, conn *connectionRecordV2) []EventV2 {
	ident := IdentityV2{SessionID: sess.id, ConnectionID: conn.id, Epoch: conn.epoch}
	return []EventV2{
		s.prepareEvent(sess, sess.client, sess.server, "client", ident),
		s.prepareEvent(sess, sess.server, sess.client, "server", ident),
	}
}

func (s *StoreV2) prepareEvent(sess *sessionRecordV2, target, peer, role string, ident IdentityV2) EventV2 {
	msg, _ := NewUUIDV2()
	reg := ""
	if n := s.nodes[target]; n != nil {
		reg = n.registrationID
	}
	prep := &PrepareEventV2{
		MessageID:       msg,
		RegistrationID:  reg,
		IdentityV2:      ident,
		Revision:        sess.revision,
		Role:            role,
		PeerNodeKey:     peer,
		SenderNodeKey:   target,
		SetupDeadlineMs: DefaultSetupTimeoutV2.Milliseconds(),
		ProtocolVersion: ProtocolVersionV2,
	}
	return EventV2{
		TargetNodeKey:  target,
		RegistrationID: reg,
		Kind:           FrameKindPrepareV2,
		MessageID:      msg,
		Identity:       ident,
		Revision:       sess.revision,
		Role:           role,
		PeerNodeKey:    peer,
		Prepare:        prep,
	}
}

func (s *StoreV2) makeStartEvents(sess *sessionRecordV2, conn *connectionRecordV2) []EventV2 {
	ident := IdentityV2{SessionID: sess.id, ConnectionID: conn.id, Epoch: conn.epoch}
	return []EventV2{
		s.startEvent(sess, sess.client, ident),
		s.startEvent(sess, sess.server, ident),
	}
}

func (s *StoreV2) startEvent(sess *sessionRecordV2, target string, ident IdentityV2) EventV2 {
	msg, _ := NewUUIDV2()
	reg := ""
	if n := s.nodes[target]; n != nil {
		reg = n.registrationID
	}
	start := &StartEventV2{
		MessageID:      msg,
		RegistrationID: reg,
		IdentityV2:     ident,
		Revision:       sess.revision,
		SenderNodeKey:  target,
	}
	return EventV2{
		TargetNodeKey:  target,
		RegistrationID: reg,
		Kind:           FrameKindStartV2,
		MessageID:      msg,
		Identity:       ident,
		Revision:       sess.revision,
		Start:          start,
	}
}

func (s *StoreV2) notifyBoth(sess *sessionRecordV2, conn *connectionRecordV2, kind FrameKindV2) []EventV2 {
	ident := IdentityV2{}
	if conn != nil {
		ident = IdentityV2{SessionID: sess.id, ConnectionID: conn.id, Epoch: conn.epoch}
	} else {
		ident = IdentityV2{SessionID: sess.id, ConnectionID: "conn-0", Epoch: 1}
	}
	return []EventV2{
		s.notifyEvent(sess, sess.client, ident, kind),
		s.notifyEvent(sess, sess.server, ident, kind),
	}
}

func (s *StoreV2) notifyEvent(sess *sessionRecordV2, target string, ident IdentityV2, kind FrameKindV2) EventV2 {
	msg, _ := NewUUIDV2()
	reg := ""
	if n := s.nodes[target]; n != nil {
		reg = n.registrationID
	}
	return EventV2{
		TargetNodeKey:  target,
		RegistrationID: reg,
		Kind:           kind,
		MessageID:      msg,
		Identity:       ident,
		Revision:       sess.revision,
	}
}

func (sess *sessionRecordV2) member(node string) bool {
	return node == sess.client || node == sess.server
}

func sessionSnapV2(sess *sessionRecordV2) SessionSnapshotV2 {
	conn := sess.connections["conn-0"]
	epoch := uint64(1)
	connID := "conn-0"
	if conn != nil {
		epoch = conn.epoch
		connID = conn.id
	}
	return SessionSnapshotV2{
		SessionID:     sess.id,
		ConnectionID:  connID,
		Epoch:         epoch,
		State:         sess.state,
		Revision:      sess.revision,
		ClientNodeKey: sess.client,
		ServerNodeKey: sess.server,
	}
}

func connSnapV2(sess *sessionRecordV2, conn *connectionRecordV2) ConnectionSnapshotV2 {
	return ConnectionSnapshotV2{
		SessionID:    sess.id,
		ConnectionID: conn.id,
		Epoch:        conn.epoch,
		State:        conn.state,
		Revision:     sess.revision,
	}
}

func nextConnIDV2(sess *sessionRecordV2) string {
	for i := 0; ; i++ {
		id := fmt.Sprintf("conn-%d", i)
		if _, ok := sess.connections[id]; !ok {
			return id
		}
	}
}

func cloneEventsV2(in []EventV2) []EventV2 {
	if in == nil {
		return nil
	}
	out := make([]EventV2, len(in))
	copy(out, in)
	return out
}

func codeErrV2(code ErrorCodeV2) error {
	return &ProtocolErrorV2{Code: code}
}
