package p2p

import (
	"fmt"
	"sort"
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

var v2TestStoreSyncSnapshotHook func()

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
	SessionID       string
	ConnectionID    string
	Epoch           uint64
	State           SessionStateV2
	ConnectionState ConnectionStateV2
	Revision        uint64
	ClientNodeKey   string
	ServerNodeKey   string
}

type ConnectionSnapshotV2 struct {
	SessionID    string
	ConnectionID string
	Epoch        uint64
	State        ConnectionStateV2
	Revision     uint64
}

type StoreSyncSnapshotV2 struct {
	Session     SessionSnapshotV2
	Connections []ConnectionSnapshotV2
	Requests    []v2RequestSnapshot
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
	Signal         *SignalEventV2
	Error          *ErrorPayloadV2
}

type StoreV2 struct {
	mu         sync.Mutex
	now        func() time.Time
	maxConns   int
	nodeGrace  time.Duration
	tombTTL    time.Duration
	nodes      map[string]*nodeRecordV2
	sessions   map[string]*sessionRecordV2
	tombs      map[string]*sessionRecordV2
	requests   map[RequestKeyV2]requestRecordV2
	messages   map[MessageKeyV2]struct{}
	directions map[DirectionKeyV2]*directionRecordV2
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
	setupTimeout  time.Duration
	setupDeadline time.Time
	closedAt      time.Time
	connections   map[string]*connectionRecordV2
	closeEvents   []EventV2
}

type requestRecordV2 struct {
	node        NodeSnapshotV2
	session     SessionSnapshotV2
	conn        ConnectionSnapshotV2
	events      []EventV2
	pending     []EventV2
	sessionID   string
	deliverySeq uint64
	hasNode     bool
	hasSess     bool
	hasConn     bool
	hasEv       bool
}

func NewStoreV2(now func() time.Time, maxConnectionsPerSession int) *StoreV2 {
	if now == nil {
		now = time.Now
	}
	if maxConnectionsPerSession <= 0 {
		maxConnectionsPerSession = 128
	}
	return &StoreV2{
		now:        now,
		maxConns:   maxConnectionsPerSession,
		nodeGrace:  NodeDisconnectGraceV2,
		tombTTL:    DefaultTombstoneTTLV2,
		nodes:      make(map[string]*nodeRecordV2),
		sessions:   make(map[string]*sessionRecordV2),
		tombs:      make(map[string]*sessionRecordV2),
		requests:   make(map[RequestKeyV2]requestRecordV2),
		messages:   make(map[MessageKeyV2]struct{}),
		directions: make(map[DirectionKeyV2]*directionRecordV2),
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
	setupTimeout := DefaultSetupTimeoutV2
	if cmd.TotalTimeoutMs != 0 {
		d, err := ValidateTotalTimeoutMsV2(cmd.TotalTimeoutMs)
		if err != nil {
			return SessionSnapshotV2{}, err
		}
		setupTimeout = d
	}
	sess := &sessionRecordV2{
		id:            sid,
		state:         SessionStateAllocatedV2,
		revision:      1,
		client:        sender,
		server:        cmd.ServerNodeKey,
		setupTimeout:  setupTimeout,
		setupDeadline: now.Add(setupTimeout),
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
	s.requests[key] = requestRecordV2{session: snap, sessionID: sid, hasSess: true}
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
		s.requests[key] = requestRecordV2{events: cloneEventsV2(events), pending: cloneEventsV2(events), sessionID: sess.id, deliverySeq: 1, hasEv: true}
		return events, nil
	case SessionCommandCloseV2:
		events := s.closeSessionLocked(sess)
		s.requests[key] = requestRecordV2{events: cloneEventsV2(events), pending: cloneEventsV2(events), sessionID: sess.id, deliverySeq: 1, hasEv: true}
		return events, nil
	case SessionCommandResumeV2:
		if sess.state == SessionStateClosedV2 {
			return nil, codeErrV2(ErrInvalidStateV2)
		}
		s.requests[key] = requestRecordV2{sessionID: sess.id, deliverySeq: 1, hasEv: true}
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
	var events []EventV2
	if conn.ready[sess.client] && conn.ready[sess.server] {
		sess.revision++
		conn.state = ConnectionStateActiveV2
		if sess.state == SessionStatePreparingV2 || sess.state == SessionStateAllocatedV2 {
			sess.state = SessionStateActiveV2
		}
		events = s.makeStartEvents(sess, conn)
		conn.readyEvents = events
	}
	s.requests[key] = requestRecordV2{events: cloneEventsV2(events), pending: cloneEventsV2(events), sessionID: sess.id, deliverySeq: 1, hasEv: true}
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
		snap := closedConnSnapV2(sess, cmd.ConnectionID)
		s.requests[key] = requestRecordV2{conn: snap, sessionID: sess.id, hasConn: true}
		return snap, nil
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
	s.requests[key] = requestRecordV2{conn: snap, sessionID: sess.id, hasConn: true}
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
	if sess.state == SessionStateClosedV2 || sess.state == SessionStateFailedV2 || conn.state == ConnectionStateClosedV2 {
		snap := closedConnSnapV2(sess, cmd.ConnectionID)
		s.requests[key] = requestRecordV2{conn: snap, sessionID: sess.id, hasConn: true}
		return snap, nil
	}
	if cmd.Epoch != 0 {
		if cmd.Epoch < conn.epoch {
			return ConnectionSnapshotV2{}, codeErrV2(ErrStaleEpochV2)
		}
		if cmd.Epoch > conn.epoch {
			return ConnectionSnapshotV2{}, codeErrV2(ErrFutureEpochV2)
		}
	}
	s.dropGenerationLocked(sess.id, conn.id, conn.epoch)
	sess.revision++
	conn.epoch++
	conn.state = ConnectionStateAllocatedV2
	conn.ready = make(map[string]bool)
	conn.bindEvents = nil
	conn.readyEvents = nil
	conn.setupDeadline = s.now().Add(DefaultSetupTimeoutV2)
	snap := connSnapV2(sess, conn)
	s.requests[key] = requestRecordV2{conn: snap, sessionID: sess.id, hasConn: true}
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
	if cmd.Revision != 0 && cmd.Revision != sess.revision {
		return nil, codeErrV2(ErrStaleRevisionV2)
	}
	sess.revision++
	conn.state = ConnectionStatePreparingV2
	events := s.makePrepareEvents(sess, conn)
	conn.bindEvents = events
	s.requests[key] = requestRecordV2{events: cloneEventsV2(events), pending: cloneEventsV2(events), sessionID: sess.id, deliverySeq: 1, hasEv: true}
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
		s.requests[key] = requestRecordV2{events: cloneEventsV2(conn.closeEvents), pending: cloneEventsV2(conn.closeEvents), sessionID: sess.id, deliverySeq: 1, hasEv: true}
		return cloneEventsV2(conn.closeEvents), nil
	}
	sess.revision++
	conn.state = ConnectionStateClosedV2
	s.dropGenerationLocked(sess.id, conn.id, conn.epoch)
	events := s.notifyBoth(sess, conn, FrameKindCloseV2)
	conn.closeEvents = events
	s.requests[key] = requestRecordV2{events: cloneEventsV2(events), pending: cloneEventsV2(events), sessionID: sess.id, deliverySeq: 1, hasEv: true}
	return events, nil
}

func (s *StoreV2) RejectConnection(sender string, cmd ConnectionCommandV2) ([]EventV2, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ValidateUUIDV2(cmd.RequestID); err != nil {
		return nil, err
	}
	key := RequestKeyV2{SenderNodeKey: sender, RequestID: cmd.RequestID}
	if rec, ok := s.requests[key]; ok && rec.hasEv {
		return nil, nil
	}
	if _, err := s.requireNode(sender); err != nil {
		return nil, err
	}
	sess, conn, err := s.requireMemberConn(sender, cmd)
	if err != nil {
		return nil, err
	}
	if conn.state != ConnectionStateAllocatedV2 && conn.state != ConnectionStatePreparingV2 && conn.state != ConnectionStateRestartingV2 {
		return nil, codeErrV2(ErrInvalidStateV2)
	}
	sess.revision++
	conn.state = ConnectionStateClosedV2
	s.dropGenerationLocked(sess.id, conn.id, conn.epoch)
	events := s.notifyBoth(sess, conn, FrameKindCloseV2)
	conn.closeEvents = events
	s.requests[key] = requestRecordV2{pending: cloneEventsV2(events), sessionID: sess.id, deliverySeq: 1, hasEv: true}
	return events, nil
}

// PendingEvents returns the unconfirmed event deliveries for a cached request.
func (s *StoreV2) PendingEvents(sender, requestID string) []EventV2 {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.requests[RequestKeyV2{SenderNodeKey: sender, RequestID: requestID}]
	if !ok || !rec.hasEv {
		return nil
	}
	return cloneEventsV2(rec.pending)
}

func (s *StoreV2) HasStateRequest(sender, requestID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.requests[RequestKeyV2{SenderNodeKey: sender, RequestID: requestID}]
	return ok && rec.hasEv
}

// MarkEventsPublished confirms only the supplied message IDs and leaves every
// other delivery pending for a retry of the same request.
func (s *StoreV2) MarkEventsPublished(sender, requestID string, messageIDs []string) error {
	if err := ValidateNodeKeyV2(sender); err != nil {
		return err
	}
	if err := ValidateUUIDV2(requestID); err != nil {
		return err
	}
	if len(messageIDs) == 0 {
		return nil
	}
	confirmed := make(map[string]struct{}, len(messageIDs))
	for _, id := range messageIDs {
		if err := ValidateUUIDV2(id); err != nil {
			return err
		}
		confirmed[id] = struct{}{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := RequestKeyV2{SenderNodeKey: sender, RequestID: requestID}
	rec, ok := s.requests[key]
	if !ok || !rec.hasEv || len(rec.pending) == 0 {
		return nil
	}
	pending := rec.pending[:0]
	changed := false
	for _, event := range rec.pending {
		if _, ok := confirmed[event.MessageID]; ok {
			changed = true
			continue
		}
		pending = append(pending, event)
	}
	if changed {
		rec.pending = cloneEventsV2(pending)
		rec.deliverySeq++
		s.requests[key] = rec
	}
	return nil
}

func (s *StoreV2) requestSnapshotsV2(sessionID string) []v2RequestSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requestSnapshotsLockedV2(sessionID)
}

func (s *StoreV2) requestSnapshotsLockedV2(sessionID string) []v2RequestSnapshot {
	requests := make([]v2RequestSnapshot, 0)
	for key, rec := range s.requests {
		if rec.sessionID != sessionID {
			continue
		}
		requests = append(requests, v2RequestSnapshot{
			SenderNodeKey: key.SenderNodeKey,
			RequestID:     key.RequestID,
			SessionID:     rec.sessionID,
			Node:          rec.node,
			Session:       rec.session,
			Connection:    rec.conn,
			Events:        cloneEventsV2(rec.events),
			Pending:       cloneEventsV2(rec.pending),
			DeliverySeq:   rec.deliverySeq,
			HasNode:       rec.hasNode,
			HasSession:    rec.hasSess,
			HasConnection: rec.hasConn,
			HasEvents:     rec.hasEv,
		})
	}
	sort.Slice(requests, func(i, j int) bool {
		if requests[i].SenderNodeKey != requests[j].SenderNodeKey {
			return requests[i].SenderNodeKey < requests[j].SenderNodeKey
		}
		return requests[i].RequestID < requests[j].RequestID
	})
	return requests
}

func (s *StoreV2) SyncSnapshotV2(sessionID string) (StoreSyncSnapshotV2, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess := s.sessions[sessionID]
	if sess == nil {
		sess = s.tombs[sessionID]
	}
	if sess == nil {
		return StoreSyncSnapshotV2{}, false
	}
	snapshot := StoreSyncSnapshotV2{Session: sessionSnapV2(sess)}
	if v2TestStoreSyncSnapshotHook != nil {
		v2TestStoreSyncSnapshotHook()
	}
	connectionIDs := make([]string, 0, len(sess.connections))
	for connectionID := range sess.connections {
		connectionIDs = append(connectionIDs, connectionID)
	}
	sort.Strings(connectionIDs)
	snapshot.Connections = make([]ConnectionSnapshotV2, 0, len(connectionIDs))
	for _, connectionID := range connectionIDs {
		snapshot.Connections = append(snapshot.Connections, connSnapV2(sess, sess.connections[connectionID]))
	}
	snapshot.Requests = s.requestSnapshotsLockedV2(sessionID)
	return snapshot, true
}

func (s *StoreV2) importRequestSnapshotsV2(requests []v2RequestSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.importRequestSnapshotsLockedV2(requests)
}

func (s *StoreV2) importRequestSnapshotsLockedV2(requests []v2RequestSnapshot) {
	for _, snapshot := range requests {
		if snapshot.SenderNodeKey == "" || snapshot.RequestID == "" {
			continue
		}
		key := RequestKeyV2{SenderNodeKey: snapshot.SenderNodeKey, RequestID: snapshot.RequestID}
		if current, ok := s.requests[key]; ok && current.deliverySeq >= snapshot.DeliverySeq {
			continue
		}
		s.requests[key] = requestRecordV2{
			node:        snapshot.Node,
			session:     snapshot.Session,
			conn:        snapshot.Connection,
			events:      cloneEventsV2(snapshot.Events),
			pending:     cloneEventsV2(snapshot.Pending),
			sessionID:   snapshot.SessionID,
			deliverySeq: snapshot.DeliverySeq,
			hasNode:     snapshot.HasNode,
			hasSess:     snapshot.HasSession,
			hasConn:     snapshot.HasConnection,
			hasEv:       snapshot.HasEvents,
		}
	}
}

func (s *StoreV2) ExpirePendingSignals(now time.Time) []EventV2 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var events []EventV2
	for _, sess := range s.sessions {
		if sess.state != SessionStateActiveV2 {
			continue
		}
		for _, conn := range sess.connections {
			if conn.state != ConnectionStateActiveV2 {
				continue
			}
			if now.Before(conn.setupDeadline) {
				continue
			}
			if s.generationHasPendingLocked(sess.id, conn.id, conn.epoch) {
				events = append(events, s.failConnectionLocked(sess, conn)...)
			}
		}
	}
	return events
}

func (s *StoreV2) SetExpiryDurations(grace, tombstone time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if grace > 0 {
		s.nodeGrace = grace
	}
	if tombstone > 0 {
		s.tombTTL = tombstone
	}
}

func (s *StoreV2) disconnectGrace() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nodeGrace > 0 {
		return s.nodeGrace
	}
	return NodeDisconnectGraceV2
}

func (s *StoreV2) tombstoneTTL() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tombTTL > 0 {
		return s.tombTTL
	}
	return DefaultTombstoneTTLV2
}

func (s *StoreV2) Expire(now time.Time) []EventV2 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var events []EventV2
	grace := s.nodeGrace
	if grace <= 0 {
		grace = NodeDisconnectGraceV2
	}
	tomb := s.tombTTL
	if tomb <= 0 {
		tomb = DefaultTombstoneTTLV2
	}
	for _, sess := range s.sessions {
		if sess.state == SessionStateAllocatedV2 || sess.state == SessionStatePreparingV2 {
			if !now.Before(sess.setupDeadline) {
				events = append(events, s.failSessionLocked(sess)...)
			}
			continue
		}
		if sess.state != SessionStateActiveV2 {
			continue
		}
		for _, conn := range sess.connections {
			if conn.state == ConnectionStateClosedV2 {
				continue
			}
			if now.Before(conn.setupDeadline) {
				continue
			}
			if conn.state == ConnectionStateAllocatedV2 || conn.state == ConnectionStatePreparingV2 {
				events = append(events, s.failConnectionLocked(sess, conn)...)
				continue
			}
			if conn.state == ConnectionStateActiveV2 && s.generationHasPendingLocked(sess.id, conn.id, conn.epoch) {
				events = append(events, s.failConnectionLocked(sess, conn)...)
			}
		}
	}
	for key, n := range s.nodes {
		if n.disconnectedAt != nil && !now.Before(n.disconnectedAt.Add(grace)) {
			delete(s.nodes, key)
		}
	}
	for id, sess := range s.tombs {
		if !sess.closedAt.IsZero() && !now.Before(sess.closedAt.Add(tomb)) {
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
	if sess, ok := s.tombs[id]; ok && sess.state == SessionStateClosedV2 {
		return sess, nil
	}
	return nil, codeErrV2(ErrSessionNotFoundV2)
}

func (s *StoreV2) PeekSession(id string) (SessionSnapshotV2, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sess, ok := s.sessions[id]; ok {
		return sessionSnapV2(sess), true
	}
	if sess, ok := s.tombs[id]; ok {
		return sessionSnapV2(sess), true
	}
	return SessionSnapshotV2{}, false
}

func (s *StoreV2) PeekConnection(sessionID, connectionID string) (ConnectionSnapshotV2, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		sess, ok = s.tombs[sessionID]
	}
	if !ok {
		return ConnectionSnapshotV2{}, false
	}
	conn, ok := sess.connections[connectionID]
	if !ok {
		return ConnectionSnapshotV2{}, false
	}
	return connSnapV2(sess, conn), true
}

func (s *StoreV2) PeekConnections(sessionID string) ([]ConnectionSnapshotV2, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		sess, ok = s.tombs[sessionID]
	}
	if !ok {
		return nil, false
	}
	ids := make([]string, 0, len(sess.connections))
	for id := range sess.connections {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	snaps := make([]ConnectionSnapshotV2, 0, len(ids))
	for _, id := range ids {
		snaps = append(snaps, connSnapV2(sess, sess.connections[id]))
	}
	return snaps, true
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
		return nil
	}
	sess.revision++
	sess.state = SessionStateClosedV2
	for _, conn := range sess.connections {
		conn.state = ConnectionStateClosedV2
	}
	s.dropSessionDirectionsLocked(sess.id)
	connectionIDs := make([]string, 0, len(sess.connections))
	for connectionID := range sess.connections {
		connectionIDs = append(connectionIDs, connectionID)
	}
	sort.Strings(connectionIDs)
	events := make([]EventV2, 0, len(connectionIDs)*2)
	for _, connectionID := range connectionIDs {
		events = append(events, s.notifyBoth(sess, sess.connections[connectionID], FrameKindCloseV2)...)
	}
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
	s.dropSessionDirectionsLocked(sess.id)
	events := s.notifyBoth(sess, sess.connections["conn-0"], FrameKindErrorV2)
	for i := range events {
		events[i].Error = setupTimeoutErrorV2(sess.id, "conn-0")
	}
	s.buryLocked(sess)
	return events
}

func (s *StoreV2) failConnectionLocked(sess *sessionRecordV2, conn *connectionRecordV2) []EventV2 {
	sess.revision++
	conn.state = ConnectionStateClosedV2
	s.dropGenerationLocked(sess.id, conn.id, conn.epoch)
	events := s.notifyBoth(sess, conn, FrameKindErrorV2)
	for i := range events {
		events[i].Error = setupTimeoutErrorV2(sess.id, conn.id)
	}
	return events
}

func (s *StoreV2) buryLocked(sess *sessionRecordV2) {
	sess.closedAt = s.now()
	delete(s.sessions, sess.id)
	s.tombs[sess.id] = sess
	s.refreshSessionRequestSnapshots(sess)
}

func (s *StoreV2) refreshSessionRequestSnapshots(sess *sessionRecordV2) {
	snap := sessionSnapV2(sess)
	for key, rec := range s.requests {
		if rec.sessionID != sess.id && !(rec.hasSess && rec.session.SessionID == sess.id) {
			continue
		}
		rec.sessionID = sess.id
		if rec.hasSess {
			rec.session = snap
		}
		s.requests[key] = rec
	}
}

func setupTimeoutErrorV2(sessionID, connectionID string) *ErrorPayloadV2 {
	return &ErrorPayloadV2{Code: ErrSetupTimeoutV2, SessionID: sessionID, ConnectionID: connectionID}
}

func (s *StoreV2) dropGenerationLocked(sessionID, connectionID string, epoch uint64) {
	for key := range s.directions {
		if key.SessionID == sessionID && key.ConnectionID == connectionID && key.Epoch == epoch {
			delete(s.directions, key)
		}
	}
	for key := range s.messages {
		if key.Direction.SessionID == sessionID && key.Direction.ConnectionID == connectionID && key.Direction.Epoch == epoch {
			delete(s.messages, key)
		}
	}
}

func (s *StoreV2) dropSessionDirectionsLocked(sessionID string) {
	for key := range s.directions {
		if key.SessionID == sessionID {
			delete(s.directions, key)
		}
	}
	for key := range s.messages {
		if key.Direction.SessionID == sessionID {
			delete(s.messages, key)
		}
	}
}

func (s *StoreV2) generationDeadline(dir DirectionKeyV2) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess := s.sessions[dir.SessionID]
	if sess == nil {
		return time.Time{}, false
	}
	conn := sess.connections[dir.ConnectionID]
	if conn == nil || conn.epoch != dir.Epoch || conn.state == ConnectionStateClosedV2 {
		return time.Time{}, false
	}
	return conn.setupDeadline, true
}

func (s *StoreV2) generationHasPendingLocked(sessionID, connectionID string, epoch uint64) bool {
	for key, d := range s.directions {
		if key.SessionID == sessionID && key.ConnectionID == connectionID && key.Epoch == epoch && len(d.pending) > 0 {
			return true
		}
	}
	return false
}

func (s *StoreV2) dropSessionRequests(sessionID string) {
	for key, rec := range s.requests {
		if rec.sessionID == sessionID || (rec.hasSess && rec.session.SessionID == sessionID) || (rec.hasConn && rec.conn.SessionID == sessionID) {
			delete(s.requests, key)
			continue
		}
		if rec.hasEv {
			for _, ev := range rec.events {
				if ev.Identity.SessionID == sessionID {
					delete(s.requests, key)
					break
				}
			}
		}
	}
	for key := range s.messages {
		if key.Direction.SessionID == sessionID {
			delete(s.messages, key)
		}
	}
	for key := range s.directions {
		if key.SessionID == sessionID {
			delete(s.directions, key)
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
		SetupDeadlineMs: sess.setupTimeout.Milliseconds(),
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

func closeEventPayloadV2(event EventV2) map[string]any {
	return map[string]any{
		"session_id":      event.Identity.SessionID,
		"connection_id":   event.Identity.ConnectionID,
		"epoch":           event.Identity.Epoch,
		"revision":        event.Revision,
		"state":           string(SessionStateClosedV2),
		"sender_node_key": v2CoordinatorSender,
	}
}

func (sess *sessionRecordV2) member(node string) bool {
	return node == sess.client || node == sess.server
}

func sessionSnapV2(sess *sessionRecordV2) SessionSnapshotV2 {
	conn := sess.connections["conn-0"]
	epoch := uint64(1)
	connID := "conn-0"
	connState := ConnectionStateAllocatedV2
	if conn != nil {
		epoch = conn.epoch
		connID = conn.id
		connState = conn.state
	}
	return SessionSnapshotV2{
		SessionID:       sess.id,
		ConnectionID:    connID,
		Epoch:           epoch,
		State:           sess.state,
		ConnectionState: connState,
		Revision:        sess.revision,
		ClientNodeKey:   sess.client,
		ServerNodeKey:   sess.server,
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

func closedConnSnapV2(sess *sessionRecordV2, connID string) ConnectionSnapshotV2 {
	conn := sess.connections[connID]
	if conn == nil {
		conn = sess.connections["conn-0"]
	}
	if conn == nil {
		return ConnectionSnapshotV2{
			SessionID:    sess.id,
			ConnectionID: "conn-0",
			Epoch:        1,
			State:        ConnectionStateClosedV2,
			Revision:     sess.revision,
		}
	}
	snap := connSnapV2(sess, conn)
	snap.State = ConnectionStateClosedV2
	return snap
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
	for i, event := range in {
		out[i] = event
		if event.Prepare != nil {
			prepare := *event.Prepare
			prepare.StunURLs = append([]string(nil), event.Prepare.StunURLs...)
			if event.Prepare.Turn != nil {
				turn := *event.Prepare.Turn
				turn.URLs = append([]string(nil), event.Prepare.Turn.URLs...)
				prepare.Turn = &turn
			}
			out[i].Prepare = &prepare
		}
		if event.Start != nil {
			start := *event.Start
			out[i].Start = &start
		}
		if event.Signal != nil {
			signal := *event.Signal
			out[i].Signal = &signal
		}
		if event.Error != nil {
			errorPayload := *event.Error
			if event.Error.RetryAfterMs != nil {
				retryAfter := *event.Error.RetryAfterMs
				errorPayload.RetryAfterMs = &retryAfter
			}
			if event.Error.Revision != nil {
				revision := *event.Error.Revision
				errorPayload.Revision = &revision
			}
			if event.Error.Limit != nil {
				limit := *event.Error.Limit
				errorPayload.Limit = &limit
			}
			out[i].Error = &errorPayload
		}
	}
	return out
}

func codeErrV2(code ErrorCodeV2) error {
	return &ProtocolErrorV2{Code: code}
}
