package p2p

type SignalEventV2 struct {
	MessageID      string
	RegistrationID string
	IdentityV2
	Seq           uint64
	Type          SignalKindV2
	Payload       SignalPayloadV2
	SenderNodeKey string
}

type pendingSignalV2 struct {
	seq    uint64
	msgID  string
	target string
	event  EventV2
}

type directionRecordV2 struct {
	nextSeq  uint64
	received uint64
	acked    uint64
	recent   map[string]uint64
	pending  map[uint64]*pendingSignalV2
}

func (s *StoreV2) LookupNode(nodeKey string) (NodeSnapshotV2, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[nodeKey]
	if !ok {
		return NodeSnapshotV2{}, false
	}
	return NodeSnapshotV2{
		NodeKey:           n.key,
		RegistrationID:    n.registrationID,
		RegistrationEpoch: n.registrationEpoch,
	}, true
}

func (s *StoreV2) AcceptSignal(sender string, cmd SignalSendCommandV2) (string, EventV2, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ValidateUUIDV2(cmd.RequestID); err != nil {
		return "", EventV2{}, err
	}
	if err := ValidateUUIDV2(cmd.MessageID); err != nil {
		return "", EventV2{}, err
	}
	if err := validateIdentityV2(cmd.IdentityV2); err != nil {
		return "", EventV2{}, err
	}
	if err := validateSignalPayloadV2(cmd.Type, cmd.Payload); err != nil {
		return "", EventV2{}, err
	}
	if cmd.Seq == 0 {
		return "", EventV2{}, invalidRequestV2()
	}
	key := RequestKeyV2{SenderNodeKey: sender, RequestID: cmd.RequestID}
	if rec, ok := s.requests[key]; ok && rec.hasEv {
		if len(rec.events) == 0 || rec.events[0].Signal == nil {
			return s.peerOfLocked(sender, cmd.SessionID), EventV2{}, nil
		}
		ev := rec.events[0]
		dir := DirectionKeyV2{
			GenerationKeyV2: GenerationKeyV2{SessionID: ev.Identity.SessionID, ConnectionID: ev.Identity.ConnectionID, Epoch: ev.Identity.Epoch},
			SenderNodeKey:   sender,
		}
		if d := s.directions[dir]; d != nil {
			if _, ok := d.pending[ev.Signal.Seq]; ok {
				return ev.TargetNodeKey, ev, nil
			}
		}
		return ev.TargetNodeKey, EventV2{}, nil
	}
	if _, err := s.requireNode(sender); err != nil {
		return "", EventV2{}, err
	}
	sess, conn, err := s.requireMemberConn(sender, ConnectionCommandV2{IdentityV2: cmd.IdentityV2})
	if err != nil {
		return "", EventV2{}, err
	}
	if conn.state != ConnectionStateActiveV2 {
		return "", EventV2{}, codeErrV2(ErrInvalidStateV2)
	}
	target := sess.peer(sender)
	dir := DirectionKeyV2{
		GenerationKeyV2: GenerationKeyV2{SessionID: sess.id, ConnectionID: conn.id, Epoch: conn.epoch},
		SenderNodeKey:   sender,
	}
	d := s.directionLocked(dir)
	if prev, ok := d.recent[cmd.MessageID]; ok {
		if prev != cmd.Seq {
			return "", EventV2{}, invalidRequestV2()
		}
		if p, ok := d.pending[cmd.Seq]; ok {
			s.requests[key] = requestRecordV2{events: cloneEventsV2([]EventV2{p.event}), sessionID: sess.id, hasEv: true}
			return p.target, p.event, nil
		}
		s.requests[key] = requestRecordV2{sessionID: sess.id, hasEv: true}
		return target, EventV2{}, nil
	}
	if d.nextSeq == 0 {
		d.nextSeq = 1
	}
	if cmd.Seq != d.nextSeq {
		return "", EventV2{}, codeErrV2(ErrSequenceGapV2)
	}
	reg := ""
	if n := s.nodes[target]; n != nil {
		reg = n.registrationID
	}
	sig := &SignalEventV2{
		MessageID:      cmd.MessageID,
		RegistrationID: reg,
		IdentityV2:     IdentityV2{SessionID: sess.id, ConnectionID: conn.id, Epoch: conn.epoch},
		Seq:            cmd.Seq,
		Type:           cmd.Type,
		Payload:        cmd.Payload,
		SenderNodeKey:  sender,
	}
	ev := EventV2{
		TargetNodeKey:  target,
		RegistrationID: reg,
		Kind:           FrameKindSignalSendV2,
		MessageID:      cmd.MessageID,
		Identity:       sig.IdentityV2,
		Revision:       sess.revision,
		Signal:         sig,
	}
	d.recent[cmd.MessageID] = cmd.Seq
	d.pending[cmd.Seq] = &pendingSignalV2{seq: cmd.Seq, msgID: cmd.MessageID, target: target, event: ev}
	d.nextSeq = cmd.Seq + 1
	d.received = cmd.Seq
	s.messages[MessageKeyV2{Direction: dir, MessageID: cmd.MessageID}] = struct{}{}
	s.requests[key] = requestRecordV2{events: cloneEventsV2([]EventV2{ev}), sessionID: sess.id, hasEv: true}
	return target, ev, nil
}

func (s *StoreV2) AckSignal(sender string, cmd SignalAckCommandV2) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ValidateUUIDV2(cmd.RequestID); err != nil {
		return err
	}
	if err := validateIdentityV2(cmd.IdentityV2); err != nil {
		return err
	}
	if cmd.AckSeq == 0 {
		return invalidRequestV2()
	}
	key := RequestKeyV2{SenderNodeKey: sender, RequestID: cmd.RequestID}
	if rec, ok := s.requests[key]; ok && rec.hasEv {
		return nil
	}
	if _, err := s.requireNode(sender); err != nil {
		return err
	}
	sess, conn, err := s.requireMemberConn(sender, ConnectionCommandV2{IdentityV2: cmd.IdentityV2})
	if err != nil {
		return err
	}
	peer := sess.peer(sender)
	dir := DirectionKeyV2{
		GenerationKeyV2: GenerationKeyV2{SessionID: sess.id, ConnectionID: conn.id, Epoch: conn.epoch},
		SenderNodeKey:   peer,
	}
	d := s.directions[dir]
	if d == nil {
		return codeErrV2(ErrSequenceGapV2)
	}
	if cmd.AckSeq > d.received {
		return codeErrV2(ErrSequenceGapV2)
	}
	if cmd.AckSeq > d.acked {
		for seq := d.acked + 1; seq <= cmd.AckSeq; seq++ {
			delete(d.pending, seq)
		}
		d.acked = cmd.AckSeq
	}
	s.requests[key] = requestRecordV2{sessionID: sess.id, hasEv: true}
	return nil
}

func (s *StoreV2) directionLocked(dir DirectionKeyV2) *directionRecordV2 {
	d := s.directions[dir]
	if d == nil {
		d = &directionRecordV2{
			nextSeq: 1,
			recent:  make(map[string]uint64),
			pending: make(map[uint64]*pendingSignalV2),
		}
		s.directions[dir] = d
	}
	return d
}

func (s *StoreV2) directionPending(dir DirectionKeyV2) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.directions[dir]
	if d == nil {
		return 0
	}
	return len(d.pending)
}

func (s *StoreV2) directionAcked(dir DirectionKeyV2) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if d := s.directions[dir]; d != nil {
		return d.acked
	}
	return 0
}

func (s *StoreV2) hasDirection(dir DirectionKeyV2) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.directions[dir]
	return ok
}

func (s *StoreV2) pendingSignal(dir DirectionKeyV2, seq uint64) (EventV2, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.directions[dir]
	if d == nil {
		return EventV2{}, false
	}
	p, ok := d.pending[seq]
	if !ok {
		return EventV2{}, false
	}
	return p.event, true
}

func (s *StoreV2) peerOfLocked(sender, sessionID string) string {
	sess, err := s.requireSession(sessionID)
	if err != nil {
		return ""
	}
	return sess.peer(sender)
}

func (sess *sessionRecordV2) peer(node string) string {
	if node == sess.client {
		return sess.server
	}
	return sess.client
}
