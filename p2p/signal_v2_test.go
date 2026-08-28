package p2p

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func mustActiveSessionV2(t *testing.T, s *StoreV2) SessionSnapshotV2 {
	t.Helper()
	mustRegisterNodeV2(t, s, storeClientNodeV2, storeClientRegV2)
	mustRegisterNodeV2(t, s, storeServerNodeV2, storeServerRegV2)
	snap := mustAllocateSessionV2(t, s, storeClientNodeV2)
	ev, err := s.BindSession(storeClientNodeV2, sessionBindCmdV2(t, snap))
	if err != nil {
		t.Fatal(err)
	}
	rev := ev[0].Revision
	if _, err := s.MarkConnectionReady(storeClientNodeV2, connCmdV2(t, ConnectionCommandReadyV2, snap.SessionID, "conn-0", 1, rev)); err != nil {
		t.Fatal(err)
	}
	start, err := s.MarkConnectionReady(storeServerNodeV2, connCmdV2(t, ConnectionCommandReadyV2, snap.SessionID, "conn-0", 1, rev))
	if err != nil {
		t.Fatal(err)
	}
	snap.Revision = start[0].Revision
	snap.State = SessionStateActiveV2
	return snap
}

func signalSendCmdV2(t *testing.T, ident IdentityV2, seq uint64, kind SignalKindV2, payload SignalPayloadV2) SignalSendCommandV2 {
	t.Helper()
	return SignalSendCommandV2{
		RequestID:  mustUUIDV2(t),
		MessageID:  mustUUIDV2(t),
		IdentityV2: ident,
		Seq:        seq,
		Type:       kind,
		Payload:    payload,
	}
}

func signalIdentV2(snap SessionSnapshotV2) IdentityV2 {
	return IdentityV2{SessionID: snap.SessionID, ConnectionID: snap.ConnectionID, Epoch: snap.Epoch}
}

func dirKeyV2(snap SessionSnapshotV2, sender string) DirectionKeyV2 {
	return DirectionKeyV2{
		GenerationKeyV2: GenerationKeyV2{SessionID: snap.SessionID, ConnectionID: snap.ConnectionID, Epoch: snap.Epoch},
		SenderNodeKey:   sender,
	}
}

func TestSignalV2_AcceptDescriptionCandidateEOC(t *testing.T) {
	s, _ := newTestStoreV2(t)
	snap := mustActiveSessionV2(t, s)
	ident := signalIdentV2(snap)

	kinds := []struct {
		kind    SignalKindV2
		payload SignalPayloadV2
	}{
		{SignalKindDescriptionV2, SignalPayloadV2{SDP: "v=0"}},
		{SignalKindCandidateV2, SignalPayloadV2{Candidate: "candidate:1"}},
		{SignalKindEndOfCandidatesV2, SignalPayloadV2{}},
	}
	for i, c := range kinds {
		cmd := signalSendCmdV2(t, ident, uint64(i+1), c.kind, c.payload)
		target, ev, err := s.AcceptSignal(storeClientNodeV2, cmd)
		if err != nil {
			t.Fatalf("%s: %v", c.kind, err)
		}
		if target != storeServerNodeV2 {
			t.Fatalf("%s target %s", c.kind, target)
		}
		if ev.Kind != FrameKindSignalSendV2 || ev.MessageID != cmd.MessageID || ev.TargetNodeKey != storeServerNodeV2 {
			t.Fatalf("%s event %+v", c.kind, ev)
		}
		if ev.Signal == nil || ev.Signal.Type != c.kind || ev.Signal.Seq != cmd.Seq || ev.Signal.SenderNodeKey != storeClientNodeV2 {
			t.Fatalf("%s signal %+v", c.kind, ev.Signal)
		}
	}
}

func TestSignalV2_RejectNonMemberWrongGenerationSeqGap(t *testing.T) {
	s, _ := newTestStoreV2(t)
	snap := mustActiveSessionV2(t, s)
	ident := signalIdentV2(snap)
	mustRegisterNodeV2(t, s, storeOtherNodeV2, storeOtherRegV2)

	_, _, err := s.AcceptSignal(storeOtherNodeV2, signalSendCmdV2(t, ident, 1, SignalKindEndOfCandidatesV2, SignalPayloadV2{}))
	requireCodeV2(t, err, ErrNotSessionMemberV2)

	zero := ident
	zero.Epoch = 0
	_, _, err = s.AcceptSignal(storeClientNodeV2, signalSendCmdV2(t, zero, 1, SignalKindEndOfCandidatesV2, SignalPayloadV2{}))
	requireCodeV2(t, err, ErrInvalidRequestV2)

	old := ident
	old.Epoch = 1
	// current epoch is 1; bump via restart then reject old
	restart, err := s.AllocateRestart(storeClientNodeV2, connCmdV2(t, ConnectionCommandRestartV2, snap.SessionID, "conn-0", 1, snap.Revision))
	if err != nil {
		t.Fatal(err)
	}
	bindEv, err := s.BindConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandBindV2, snap.SessionID, "conn-0", 2, restart.Revision))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.MarkConnectionReady(storeClientNodeV2, connCmdV2(t, ConnectionCommandReadyV2, snap.SessionID, "conn-0", 2, bindEv[0].Revision)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MarkConnectionReady(storeServerNodeV2, connCmdV2(t, ConnectionCommandReadyV2, snap.SessionID, "conn-0", 2, bindEv[0].Revision)); err != nil {
		t.Fatal(err)
	}

	_, _, err = s.AcceptSignal(storeClientNodeV2, signalSendCmdV2(t, old, 1, SignalKindEndOfCandidatesV2, SignalPayloadV2{}))
	requireCodeV2(t, err, ErrStaleEpochV2)

	future := ident
	future.Epoch = 99
	_, _, err = s.AcceptSignal(storeClientNodeV2, signalSendCmdV2(t, future, 1, SignalKindEndOfCandidatesV2, SignalPayloadV2{}))
	requireCodeV2(t, err, ErrFutureEpochV2)

	active := ident
	active.Epoch = 2
	_, _, err = s.AcceptSignal(storeClientNodeV2, signalSendCmdV2(t, active, 2, SignalKindEndOfCandidatesV2, SignalPayloadV2{}))
	requireCodeV2(t, err, ErrSequenceGapV2)
}

func TestSignalV2_IndependentDirectionsAndSessions(t *testing.T) {
	s, _ := newTestStoreV2(t)
	mustRegisterNodeV2(t, s, storeClientNodeV2, storeClientRegV2)
	mustRegisterNodeV2(t, s, storeServerNodeV2, storeServerRegV2)
	mustRegisterNodeV2(t, s, storeOtherNodeV2, storeOtherRegV2)

	a := mustActiveSessionV2From(t, s, storeClientNodeV2, storeServerNodeV2)
	bSnap, err := s.AllocateSession(storeOtherNodeV2, CreateSessionCommandV2{RequestID: mustUUIDV2(t), ServerNodeKey: storeServerNodeV2})
	if err != nil {
		t.Fatal(err)
	}
	bev, err := s.BindSession(storeOtherNodeV2, sessionBindCmdV2(t, bSnap))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.MarkConnectionReady(storeOtherNodeV2, connCmdV2(t, ConnectionCommandReadyV2, bSnap.SessionID, "conn-0", 1, bev[0].Revision)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MarkConnectionReady(storeServerNodeV2, connCmdV2(t, ConnectionCommandReadyV2, bSnap.SessionID, "conn-0", 1, bev[0].Revision)); err != nil {
		t.Fatal(err)
	}

	identA := signalIdentV2(a)
	identB := IdentityV2{SessionID: bSnap.SessionID, ConnectionID: "conn-0", Epoch: 1}

	c2s, ev, err := s.AcceptSignal(storeClientNodeV2, signalSendCmdV2(t, identA, 1, SignalKindDescriptionV2, SignalPayloadV2{SDP: "c"}))
	if err != nil {
		t.Fatal(err)
	}
	if c2s != storeServerNodeV2 || ev.Signal.Seq != 1 {
		t.Fatalf("client→server %+v target %s", ev.Signal, c2s)
	}
	s2c, ev, err := s.AcceptSignal(storeServerNodeV2, signalSendCmdV2(t, identA, 1, SignalKindDescriptionV2, SignalPayloadV2{SDP: "s"}))
	if err != nil {
		t.Fatal(err)
	}
	if s2c != storeClientNodeV2 || ev.Signal.Seq != 1 {
		t.Fatalf("server→client %+v target %s", ev.Signal, s2c)
	}
	other, ev, err := s.AcceptSignal(storeOtherNodeV2, signalSendCmdV2(t, identB, 1, SignalKindDescriptionV2, SignalPayloadV2{SDP: "o"}))
	if err != nil {
		t.Fatal(err)
	}
	if other != storeServerNodeV2 || ev.Signal.Seq != 1 {
		t.Fatalf("other session %+v target %s", ev.Signal, other)
	}
}

func TestSignalV2_DedupAndCumulativeAck(t *testing.T) {
	s, _ := newTestStoreV2(t)
	snap := mustActiveSessionV2(t, s)
	ident := signalIdentV2(snap)
	dir := dirKeyV2(snap, storeClientNodeV2)

	first := signalSendCmdV2(t, ident, 1, SignalKindCandidateV2, SignalPayloadV2{Candidate: "a"})
	_, ev1, err := s.AcceptSignal(storeClientNodeV2, first)
	if err != nil {
		t.Fatal(err)
	}
	if s.directionPending(dir) != 1 {
		t.Fatalf("pending after first send: %d", s.directionPending(dir))
	}

	_, evDup, err := s.AcceptSignal(storeClientNodeV2, first)
	if err != nil {
		t.Fatal(err)
	}
	if evDup.MessageID != ev1.MessageID || evDup.Signal.Seq != 1 {
		t.Fatalf("duplicate request should replay event %+v vs %+v", evDup, ev1)
	}
	if s.directionPending(dir) != 1 {
		t.Fatalf("duplicate must not create a second pending item: %d", s.directionPending(dir))
	}

	replay := first
	replay.RequestID = mustUUIDV2(t)
	_, evReplay, err := s.AcceptSignal(storeClientNodeV2, replay)
	if err != nil {
		t.Fatal(err)
	}
	if evReplay.MessageID != ev1.MessageID {
		t.Fatalf("same message_id must not mint a new delivery: %s vs %s", evReplay.MessageID, ev1.MessageID)
	}

	second := signalSendCmdV2(t, ident, 2, SignalKindCandidateV2, SignalPayloadV2{Candidate: "b"})
	if _, _, err := s.AcceptSignal(storeClientNodeV2, second); err != nil {
		t.Fatal(err)
	}
	if s.directionPending(dir) != 2 {
		t.Fatalf("pending after seq=2: %d", s.directionPending(dir))
	}

	if err := s.AckSignal(storeServerNodeV2, SignalAckCommandV2{RequestID: mustUUIDV2(t), IdentityV2: ident, AckSeq: 1}); err != nil {
		t.Fatal(err)
	}
	if s.directionPending(dir) != 1 {
		t.Fatalf("ack_seq=1 should leave seq=2 pending: %d", s.directionPending(dir))
	}
	if s.directionAcked(dir) != 1 {
		t.Fatalf("confirmed watermark %d, want 1", s.directionAcked(dir))
	}

	if err := s.AckSignal(storeServerNodeV2, SignalAckCommandV2{RequestID: mustUUIDV2(t), IdentityV2: ident, AckSeq: 1}); err != nil {
		t.Fatal(err)
	}

	if err := s.AckSignal(storeServerNodeV2, SignalAckCommandV2{RequestID: mustUUIDV2(t), IdentityV2: ident, AckSeq: 9}); err == nil {
		t.Fatal("ack beyond receive watermark must be sequence_gap")
	} else {
		requireCodeV2(t, err, ErrSequenceGapV2)
	}

	if err := s.AckSignal(storeServerNodeV2, SignalAckCommandV2{RequestID: mustUUIDV2(t), IdentityV2: ident, AckSeq: 2}); err != nil {
		t.Fatal(err)
	}
	if s.directionPending(dir) != 0 || s.directionAcked(dir) != 2 {
		t.Fatalf("after ack 2 pending=%d acked=%d", s.directionPending(dir), s.directionAcked(dir))
	}

	_, evAfterAck, err := s.AcceptSignal(storeClientNodeV2, replay)
	if err != nil {
		t.Fatal(err)
	}
	if evAfterAck.MessageID != "" {
		t.Fatalf("acked message_id must not be delivered again: %+v", evAfterAck)
	}

	third := signalSendCmdV2(t, ident, 3, SignalKindEndOfCandidatesV2, SignalPayloadV2{})
	if _, _, err := s.AcceptSignal(storeClientNodeV2, third); err != nil {
		t.Fatal(err)
	}
}

func TestSignalV2_AckDoesNotConsumeSeqAndCannotSpoof(t *testing.T) {
	s, _ := newTestStoreV2(t)
	snap := mustActiveSessionV2(t, s)
	ident := signalIdentV2(snap)

	if _, _, err := s.AcceptSignal(storeClientNodeV2, signalSendCmdV2(t, ident, 1, SignalKindCandidateV2, SignalPayloadV2{Candidate: "x"})); err != nil {
		t.Fatal(err)
	}
	if err := s.AckSignal(storeServerNodeV2, SignalAckCommandV2{RequestID: mustUUIDV2(t), IdentityV2: ident, AckSeq: 1}); err != nil {
		t.Fatal(err)
	}
	target, ev, err := s.AcceptSignal(storeServerNodeV2, signalSendCmdV2(t, ident, 1, SignalKindCandidateV2, SignalPayloadV2{Candidate: "y"}))
	if err != nil {
		t.Fatal(err)
	}
	if target != storeClientNodeV2 || ev.Signal.Seq != 1 || ev.Signal.SenderNodeKey != storeServerNodeV2 {
		t.Fatalf("ACK must not consume the reverse direction seq: %+v target %s", ev.Signal, target)
	}

	mustRegisterNodeV2(t, s, storeOtherNodeV2, storeOtherRegV2)
	if err := s.AckSignal(storeOtherNodeV2, SignalAckCommandV2{RequestID: mustUUIDV2(t), IdentityV2: ident, AckSeq: 1}); err == nil {
		t.Fatal("non-member ACK must fail")
	} else {
		requireCodeV2(t, err, ErrNotSessionMemberV2)
	}

	spoofed := signalSendCmdV2(t, ident, 2, SignalKindEndOfCandidatesV2, SignalPayloadV2{})
	spoofed.SenderNodeKey = storeOtherNodeV2
	target, ev, err = s.AcceptSignal(storeClientNodeV2, spoofed)
	if err != nil {
		t.Fatal(err)
	}
	if target != storeServerNodeV2 || ev.Signal.SenderNodeKey != storeClientNodeV2 {
		t.Fatalf("payload sender/target must be inferred: target=%s sender=%s", target, ev.Signal.SenderNodeKey)
	}
}

func TestSignalV2_TombstoneReleasesDirectionCache(t *testing.T) {
	s, clock := newTestStoreV2(t)
	snap := mustActiveSessionV2(t, s)
	ident := signalIdentV2(snap)
	dir := dirKeyV2(snap, storeClientNodeV2)
	cmd := signalSendCmdV2(t, ident, 1, SignalKindEndOfCandidatesV2, SignalPayloadV2{})
	if _, _, err := s.AcceptSignal(storeClientNodeV2, cmd); err != nil {
		t.Fatal(err)
	}
	if s.directionPending(dir) != 1 {
		t.Fatal("expected pending signal")
	}

	if _, err := s.BindSession(storeClientNodeV2, SessionCommandV2{
		RequestID:  mustUUIDV2(t),
		Command:    SessionCommandCloseV2,
		IdentityV2: ident,
	}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(DefaultTombstoneTTLV2 + time.Millisecond)
	_ = s.Expire(clock.Now())

	if s.directionPending(dir) != 0 || s.hasDirection(dir) {
		t.Fatalf("direction cache must be released after tombstone TTL pending=%d has=%v", s.directionPending(dir), s.hasDirection(dir))
	}
	msgKey := MessageKeyV2{Direction: dir, MessageID: cmd.MessageID}
	if !s.RememberMessage(msgKey) {
		t.Fatal("message key should be recycled after tombstone TTL")
	}
}

func TestSignalV2_RejectBeforeStart(t *testing.T) {
	s, _ := newTestStoreV2(t)
	mustRegisterNodeV2(t, s, storeClientNodeV2, storeClientRegV2)
	mustRegisterNodeV2(t, s, storeServerNodeV2, storeServerRegV2)
	snap := mustAllocateSessionV2(t, s, storeClientNodeV2)
	ident := signalIdentV2(snap)
	_, _, err := s.AcceptSignal(storeClientNodeV2, signalSendCmdV2(t, ident, 1, SignalKindDescriptionV2, SignalPayloadV2{SDP: "local"}))
	requireCodeV2(t, err, ErrInvalidStateV2)

	if _, err := s.BindSession(storeClientNodeV2, sessionBindCmdV2(t, snap)); err != nil {
		t.Fatal(err)
	}
	_, _, err = s.AcceptSignal(storeClientNodeV2, signalSendCmdV2(t, ident, 1, SignalKindDescriptionV2, SignalPayloadV2{SDP: "local"}))
	requireCodeV2(t, err, ErrInvalidStateV2)
}

func TestManagerV2_SignalAcceptedAckVsDeliveryAck(t *testing.T) {
	s, _ := startManagerV2(t, Config{})
	client := agentConn(t, s, mgrClientNodeV2)
	srv := agentConn(t, s, mgrServerNodeV2)
	clientEv := subscribeEventsV2(t, client, mgrClientNodeV2, mgrClientRegV2)
	serverEv := subscribeEventsV2(t, srv, mgrServerNodeV2, mgrServerRegV2)
	mustRegisterV2(t, client, s.ID(), mgrClientNodeV2, mgrClientRegV2)
	mustRegisterV2(t, srv, s.ID(), mgrServerNodeV2, mgrServerRegV2)
	ident, _ := mustCreateBindReadySessionV2(t, client, srv, clientEv, serverEv)

	sendSubj, err := CommandSubjectV2(mgrClientNodeV2, "SIGNAL.SEND")
	if err != nil {
		t.Fatal(err)
	}
	msgID := mustUUIDV2(t)
	reqID := mustUUIDV2(t)
	got := requestV2(t, client, sendSubj, encodeFrameV2ForTest(t, reqID, msgID, "", map[string]any{
		"session_id":    ident.SessionID,
		"connection_id": ident.ConnectionID,
		"epoch":         ident.Epoch,
		"seq":           1,
		"type":          "description",
		"payload":       map[string]any{"sdp": "v=0"},
	}))
	reply := envelopePayloadMapV2(t, got.Data)
	if reply["ok"] != true || reply["accepted"] != true {
		t.Fatalf("SIGNAL.SEND must return accepted ACK, got %s", got.Data)
	}
	if reply["ack_seq"] != nil {
		t.Fatalf("accepted ACK must not carry ack_seq: %s", got.Data)
	}
	var env EnvelopeV2
	if err := json.Unmarshal(got.Data, &env); err != nil {
		t.Fatal(err)
	}
	if env.RequestID != reqID {
		t.Fatalf("accepted reply request_id %s", env.RequestID)
	}

	ev := nextEventV2(t, serverEv, FrameKindSignalSendV2)
	if ev.Envelope.MessageID != msgID || ev.Envelope.RequestID != "" {
		t.Fatalf("EVENT must keep message_id and omit request_id: %+v", ev.Envelope)
	}
	if ev.SignalSend == nil || ev.SignalSend.Seq != 1 || ev.SignalSend.Type != SignalKindDescriptionV2 || ev.SignalSend.SenderNodeKey != mgrClientNodeV2 {
		t.Fatalf("signal event %+v", ev.SignalSend)
	}

	ackSubj, err := CommandSubjectV2(mgrServerNodeV2, "SIGNAL.ACK")
	if err != nil {
		t.Fatal(err)
	}
	ackReq := mustUUIDV2(t)
	ackGot := requestV2(t, srv, ackSubj, encodeFrameV2ForTest(t, ackReq, "", "", map[string]any{
		"session_id":    ident.SessionID,
		"connection_id": ident.ConnectionID,
		"epoch":         ident.Epoch,
		"ack_seq":       1,
	}))
	ackReply := envelopePayloadMapV2(t, ackGot.Data)
	if ackReply["ok"] != true {
		t.Fatalf("SIGNAL.ACK reply %s", ackGot.Data)
	}
	if ackReply["accepted"] != nil {
		t.Fatalf("delivery ACK is not an accepted takeover reply: %s", ackGot.Data)
	}
	assertNoEventV2(t, serverEv)
	assertNoEventV2(t, clientEv)
}

func TestManagerV2_SignalRetryUsesCurrentRegistration(t *testing.T) {
	s, _ := startManagerV2(t, Config{})
	client := agentConn(t, s, mgrClientNodeV2)
	srv := agentConn(t, s, mgrServerNodeV2)
	clientEv := subscribeEventsV2(t, client, mgrClientNodeV2, mgrClientRegV2)
	oldEv := subscribeEventsV2(t, srv, mgrServerNodeV2, mgrServerRegV2)
	mustRegisterV2(t, client, s.ID(), mgrClientNodeV2, mgrClientRegV2)
	mustRegisterV2(t, srv, s.ID(), mgrServerNodeV2, mgrServerRegV2)
	ident, _ := mustCreateBindReadySessionV2(t, client, srv, clientEv, oldEv)

	var failOnce atomic.Bool
	failOnce.Store(true)
	var publishes []string
	var pubMu sync.Mutex
	v2TestPublishHook = func(subj string, _ []byte) error {
		pubMu.Lock()
		publishes = append(publishes, subj)
		pubMu.Unlock()
		if failOnce.CompareAndSwap(true, false) {
			return nats.ErrInvalidConnection
		}
		return nil
	}
	t.Cleanup(func() { v2TestPublishHook = nil })

	newReg := "dddddddddddddddddddddddddddddddd"
	newEv := subscribeEventsV2(t, srv, mgrServerNodeV2, newReg)
	mustRegisterV2(t, srv, s.ID(), mgrServerNodeV2, newReg)

	sendSubj, err := CommandSubjectV2(mgrClientNodeV2, "SIGNAL.SEND")
	if err != nil {
		t.Fatal(err)
	}
	msgID := mustUUIDV2(t)
	got := requestV2(t, client, sendSubj, encodeFrameV2ForTest(t, mustUUIDV2(t), msgID, "", map[string]any{
		"session_id":    ident.SessionID,
		"connection_id": ident.ConnectionID,
		"epoch":         ident.Epoch,
		"seq":           1,
		"type":          "candidate",
		"payload":       map[string]any{"candidate": "x"},
	}))
	reply := envelopePayloadMapV2(t, got.Data)
	if reply["ok"] != true || reply["accepted"] != true {
		t.Fatalf("accepted after publish fail: %s", got.Data)
	}

	ev := nextEventV2(t, newEv, FrameKindSignalSendV2)
	if ev.Envelope.MessageID != msgID || ev.Envelope.RegistrationID != newReg {
		t.Fatalf("retry must use new registration: %+v", ev.Envelope)
	}
	if ev.SignalSend == nil || ev.SignalSend.Seq != 1 {
		t.Fatalf("retry must keep seq/message_id %+v", ev.SignalSend)
	}
	assertNoEventV2(t, oldEv)

	wantOld, err := EventSubjectV2(mgrServerNodeV2, mgrServerRegV2)
	if err != nil {
		t.Fatal(err)
	}
	wantNew, err := EventSubjectV2(mgrServerNodeV2, newReg)
	if err != nil {
		t.Fatal(err)
	}
	pubMu.Lock()
	seen := append([]string(nil), publishes...)
	pubMu.Unlock()
	var sawNew bool
	for _, subj := range seen {
		if subj == wantOld && containsSignalSubject(seen, wantNew) {
			// first attempt may have used old subject before re-register; retry must hit new
		}
		if subj == wantNew {
			sawNew = true
		}
	}
	if !sawNew {
		t.Fatalf("retry subjects %v want %s", seen, wantNew)
	}
	_ = wantOld
}

func TestManagerV2_SignalRetryDoesNotBlockStoreOrCallback(t *testing.T) {
	s, mgr := startManagerV2(t, Config{})
	client := agentConn(t, s, mgrClientNodeV2)
	srv := agentConn(t, s, mgrServerNodeV2)
	clientEv := subscribeEventsV2(t, client, mgrClientNodeV2, mgrClientRegV2)
	serverEv := subscribeEventsV2(t, srv, mgrServerNodeV2, mgrServerRegV2)
	mustRegisterV2(t, client, s.ID(), mgrClientNodeV2, mgrClientRegV2)
	mustRegisterV2(t, srv, s.ID(), mgrServerNodeV2, mgrServerRegV2)
	ident, _ := mustCreateBindReadySessionV2(t, client, srv, clientEv, serverEv)

	block := make(chan struct{})
	var inPublish atomic.Bool
	v2TestPublishHook = func(subj string, _ []byte) error {
		if !isSignalEventSubject(subj) {
			return nil
		}
		inPublish.Store(true)
		<-block
		return nil
	}
	t.Cleanup(func() {
		v2TestPublishHook = nil
		select {
		case <-block:
		default:
			close(block)
		}
	})

	sendSubj, err := CommandSubjectV2(mgrClientNodeV2, "SIGNAL.SEND")
	if err != nil {
		t.Fatal(err)
	}
	first := requestV2(t, client, sendSubj, encodeFrameV2ForTest(t, mustUUIDV2(t), mustUUIDV2(t), "", map[string]any{
		"session_id":    ident.SessionID,
		"connection_id": ident.ConnectionID,
		"epoch":         ident.Epoch,
		"seq":           1,
		"type":          "candidate",
		"payload":       map[string]any{"candidate": "one"},
	}))
	if envelopePayloadMapV2(t, first.Data)["accepted"] != true {
		t.Fatalf("first send %s", first.Data)
	}

	deadline := time.Now().Add(2 * time.Second)
	for !inPublish.Load() {
		if time.Now().After(deadline) {
			t.Fatal("publish hook never entered")
		}
		time.Sleep(5 * time.Millisecond)
	}

	lookupStart := time.Now()
	if _, ok := mgr.v2.store.LookupNode(mgrServerNodeV2); !ok {
		t.Fatal("LookupNode must work while publish is blocked")
	}
	if time.Since(lookupStart) > 200*time.Millisecond {
		t.Fatal("Store lookup blocked by publish/retry")
	}

	second := requestV2(t, client, sendSubj, encodeFrameV2ForTest(t, mustUUIDV2(t), mustUUIDV2(t), "", map[string]any{
		"session_id":    ident.SessionID,
		"connection_id": ident.ConnectionID,
		"epoch":         ident.Epoch,
		"seq":           2,
		"type":          "end_of_candidates",
		"payload":       map[string]any{},
	}))
	if envelopePayloadMapV2(t, second.Data)["accepted"] != true {
		t.Fatalf("second send while first publish blocked: %s", second.Data)
	}
}

func TestSignalV2_UnackedPastDeadlineFailsGeneration(t *testing.T) {
	s, clock := newTestStoreV2(t)
	snap := mustActiveSessionV2(t, s)
	ident := signalIdentV2(snap)
	dir := dirKeyV2(snap, storeClientNodeV2)
	if _, _, err := s.AcceptSignal(storeClientNodeV2, signalSendCmdV2(t, ident, 1, SignalKindCandidateV2, SignalPayloadV2{Candidate: "x"})); err != nil {
		t.Fatal(err)
	}
	if s.directionPending(dir) != 1 {
		t.Fatal("expected unacked pending")
	}

	clock.Advance(DefaultSetupTimeoutV2 + time.Millisecond)
	expired := s.Expire(clock.Now())
	if len(expired) == 0 {
		t.Fatal("unacked SIGNAL past setup deadline must fail the generation")
	}
	for _, ev := range expired {
		if ev.Kind != FrameKindErrorV2 {
			t.Fatalf("expire event %+v, want ERROR", ev)
		}
		if ev.Error == nil || ev.Error.Code != ErrSetupTimeoutV2 {
			t.Fatalf("expire must be setup_timeout: %+v", ev.Error)
		}
		if ev.Identity.ConnectionID != snap.ConnectionID || ev.Identity.Epoch != snap.Epoch {
			t.Fatalf("must fail this generation: %+v", ev.Identity)
		}
	}
	if s.directionPending(dir) != 0 {
		t.Fatalf("pending must be dropped after deadline pending=%d", s.directionPending(dir))
	}
	_, _, err := s.AcceptSignal(storeClientNodeV2, signalSendCmdV2(t, ident, 2, SignalKindEndOfCandidatesV2, SignalPayloadV2{}))
	requireCodeV2(t, err, ErrInvalidStateV2)
}

func TestSignalV2_RestartAndCloseDropOldGenerationPending(t *testing.T) {
	s, _ := newTestStoreV2(t)
	snap := mustActiveSessionV2(t, s)
	ident := signalIdentV2(snap)
	dir := dirKeyV2(snap, storeClientNodeV2)
	if _, _, err := s.AcceptSignal(storeClientNodeV2, signalSendCmdV2(t, ident, 1, SignalKindCandidateV2, SignalPayloadV2{Candidate: "old"})); err != nil {
		t.Fatal(err)
	}

	restart, err := s.AllocateRestart(storeClientNodeV2, connCmdV2(t, ConnectionCommandRestartV2, snap.SessionID, snap.ConnectionID, snap.Epoch, snap.Revision))
	if err != nil {
		t.Fatal(err)
	}
	if s.directionPending(dir) != 0 || s.hasDirection(dir) {
		t.Fatalf("RESTART must drop old-generation pending has=%v pending=%d", s.hasDirection(dir), s.directionPending(dir))
	}

	bindEv, err := s.BindConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandBindV2, snap.SessionID, snap.ConnectionID, restart.Epoch, restart.Revision))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.MarkConnectionReady(storeClientNodeV2, connCmdV2(t, ConnectionCommandReadyV2, snap.SessionID, snap.ConnectionID, restart.Epoch, bindEv[0].Revision)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MarkConnectionReady(storeServerNodeV2, connCmdV2(t, ConnectionCommandReadyV2, snap.SessionID, snap.ConnectionID, restart.Epoch, bindEv[0].Revision)); err != nil {
		t.Fatal(err)
	}
	newIdent := IdentityV2{SessionID: snap.SessionID, ConnectionID: snap.ConnectionID, Epoch: restart.Epoch}
	if _, _, err := s.AcceptSignal(storeClientNodeV2, signalSendCmdV2(t, newIdent, 1, SignalKindCandidateV2, SignalPayloadV2{Candidate: "new"})); err != nil {
		t.Fatal(err)
	}
	newDir := DirectionKeyV2{GenerationKeyV2: GenerationKeyV2{SessionID: snap.SessionID, ConnectionID: snap.ConnectionID, Epoch: restart.Epoch}, SenderNodeKey: storeClientNodeV2}
	if s.directionPending(newDir) != 1 {
		t.Fatal("new epoch must keep its own pending")
	}
	if s.directionPending(dir) != 0 {
		t.Fatal("old epoch pending must stay gone after BIND/READY")
	}

	closeSess := mustActiveSessionV2From(t, s, storeClientNodeV2, storeServerNodeV2)
	closeIdent := signalIdentV2(closeSess)
	closeDir := dirKeyV2(closeSess, storeClientNodeV2)
	if _, _, err := s.AcceptSignal(storeClientNodeV2, signalSendCmdV2(t, closeIdent, 1, SignalKindEndOfCandidatesV2, SignalPayloadV2{})); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BindSession(storeClientNodeV2, SessionCommandV2{
		RequestID:  mustUUIDV2(t),
		Command:    SessionCommandCloseV2,
		IdentityV2: closeIdent,
	}); err != nil {
		t.Fatal(err)
	}
	if s.directionPending(closeDir) != 0 || s.hasDirection(closeDir) {
		t.Fatalf("SESSION.CLOSE must drop session pending immediately has=%v pending=%d", s.hasDirection(closeDir), s.directionPending(closeDir))
	}

	connSess := mustActiveSessionV2From(t, s, storeClientNodeV2, storeServerNodeV2)
	connIdent := signalIdentV2(connSess)
	connDir := dirKeyV2(connSess, storeClientNodeV2)
	if _, _, err := s.AcceptSignal(storeClientNodeV2, signalSendCmdV2(t, connIdent, 1, SignalKindCandidateV2, SignalPayloadV2{Candidate: "c"})); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CloseConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandCloseV2, connSess.SessionID, connSess.ConnectionID, connSess.Epoch, 0)); err != nil {
		t.Fatal(err)
	}
	if s.directionPending(connDir) != 0 || s.hasDirection(connDir) {
		t.Fatalf("CLOSE connection must drop generation pending has=%v pending=%d", s.hasDirection(connDir), s.directionPending(connDir))
	}
}

func TestManagerV2_SignalRetryStopsAfterSetupDeadline(t *testing.T) {
	s, m := startManagerV2(t, Config{})
	client := agentConn(t, s, mgrClientNodeV2)
	srv := agentConn(t, s, mgrServerNodeV2)
	clientEv := subscribeEventsV2(t, client, mgrClientNodeV2, mgrClientRegV2)
	serverEv := subscribeEventsV2(t, srv, mgrServerNodeV2, mgrServerRegV2)
	mustRegisterV2(t, client, s.ID(), mgrClientNodeV2, mgrClientRegV2)
	mustRegisterV2(t, srv, s.ID(), mgrServerNodeV2, mgrServerRegV2)

	const shortMs int64 = 400
	ident, _ := mustCreateBindReadySessionTimeoutV2(t, client, srv, clientEv, serverEv, shortMs)

	var pubs atomic.Int64
	v2TestPublishHook = func(subj string, data []byte) error {
		if signalSendPayloadV2(data) {
			pubs.Add(1)
		}
		return nil
	}
	t.Cleanup(func() { v2TestPublishHook = nil })

	sendSubj, err := CommandSubjectV2(mgrClientNodeV2, "SIGNAL.SEND")
	if err != nil {
		t.Fatal(err)
	}
	got := requestV2(t, client, sendSubj, encodeFrameV2ForTest(t, mustUUIDV2(t), mustUUIDV2(t), "", map[string]any{
		"session_id":    ident.SessionID,
		"connection_id": ident.ConnectionID,
		"epoch":         ident.Epoch,
		"seq":           1,
		"type":          "candidate",
		"payload":       map[string]any{"candidate": "x"},
	}))
	if envelopePayloadMapV2(t, got.Data)["accepted"] != true {
		t.Fatalf("SEND %s", got.Data)
	}
	_ = nextEventV2(t, serverEv, FrameKindSignalSendV2)

	time.Sleep(time.Duration(shortMs+250) * time.Millisecond)
	failed := requestV2(t, client, sendSubj, encodeFrameV2ForTest(t, mustUUIDV2(t), mustUUIDV2(t), "", map[string]any{
		"session_id":    ident.SessionID,
		"connection_id": ident.ConnectionID,
		"epoch":         ident.Epoch,
		"seq":           9,
		"type":          "end_of_candidates",
		"payload":       map[string]any{},
	}))
	code := envelopePayloadMapV2(t, failed.Data)["error"]
	if code != string(ErrInvalidStateV2) && code != string(ErrSetupTimeoutV2) {
		t.Fatalf("SEND after deadline must fail connection, got %s", failed.Data)
	}
	_ = m.v2.store.Expire(m.now())
	if hasSignalPendingV2(m, ident) {
		t.Fatal("store pending must be dropped once deadline fires")
	}

	n := pubs.Load()
	time.Sleep(250 * time.Millisecond)
	if got := pubs.Load(); got != n {
		t.Fatalf("SIGNAL retries must stop after deadline: before=%d after=%d", n, got)
	}
}

func TestManagerV2_SignalRetryStopsAfterRestartAndSessionClose(t *testing.T) {
	s, _ := startManagerV2(t, Config{})
	client := agentConn(t, s, mgrClientNodeV2)
	srv := agentConn(t, s, mgrServerNodeV2)
	clientEv := subscribeEventsV2(t, client, mgrClientNodeV2, mgrClientRegV2)
	serverEv := subscribeEventsV2(t, srv, mgrServerNodeV2, mgrServerRegV2)
	mustRegisterV2(t, client, s.ID(), mgrClientNodeV2, mgrClientRegV2)
	mustRegisterV2(t, srv, s.ID(), mgrServerNodeV2, mgrServerRegV2)
	ident, rev := mustCreateBindReadySessionV2(t, client, srv, clientEv, serverEv)

	var mu sync.Mutex
	var pubs []signalRetryPubV2
	v2TestPublishHook = func(subj string, data []byte) error {
		if !signalSendPayloadV2(data) {
			return nil
		}
		p := envelopePayloadMapV2(t, data)
		mu.Lock()
		pubs = append(pubs, signalRetryPubV2{at: time.Now(), epoch: uint64(limitVal(p["epoch"]))})
		mu.Unlock()
		return nil
	}
	t.Cleanup(func() { v2TestPublishHook = nil })

	sendSubj, err := CommandSubjectV2(mgrClientNodeV2, "SIGNAL.SEND")
	if err != nil {
		t.Fatal(err)
	}
	got := requestV2(t, client, sendSubj, encodeFrameV2ForTest(t, mustUUIDV2(t), mustUUIDV2(t), "", map[string]any{
		"session_id":    ident.SessionID,
		"connection_id": ident.ConnectionID,
		"epoch":         ident.Epoch,
		"seq":           1,
		"type":          "candidate",
		"payload":       map[string]any{"candidate": "old"},
	}))
	if envelopePayloadMapV2(t, got.Data)["accepted"] != true {
		t.Fatalf("SEND %s", got.Data)
	}
	_ = nextEventV2(t, serverEv, FrameKindSignalSendV2)

	connSubj, err := CommandSubjectV2(mgrClientNodeV2, "CONNECTION.COMMAND")
	if err != nil {
		t.Fatal(err)
	}
	restartGot := requestV2(t, client, connSubj, connCmdFrameV2(t, mustUUIDV2(t), ConnectionCommandRestartV2, ident, rev))
	restartAlloc, err := DecodeFrameV2(FrameKindAllocatedV2, restartGot.Data)
	if err != nil {
		t.Fatalf("RESTART: %v body=%s", err, restartGot.Data)
	}
	restartIdent := IdentityV2{SessionID: ident.SessionID, ConnectionID: ident.ConnectionID, Epoch: restartAlloc.Allocated.Epoch}
	_ = requestV2(t, client, connSubj, connCmdFrameV2(t, mustUUIDV2(t), ConnectionCommandBindV2, restartIdent, restartAlloc.Allocated.Revision))
	_ = nextEventV2(t, clientEv, FrameKindPrepareV2)
	_ = nextEventV2(t, serverEv, FrameKindPrepareV2)
	cut := time.Now()
	time.Sleep(250 * time.Millisecond)
	mu.Lock()
	for _, p := range pubs {
		if !p.at.Before(cut) && p.epoch == ident.Epoch {
			t.Fatalf("old-generation SIGNAL retry published after RESTART BIND epoch=%d", p.epoch)
		}
	}
	mu.Unlock()

	closeIdent, _ := mustCreateBindReadySessionV2(t, client, srv, clientEv, serverEv)
	closeSend := requestV2(t, client, sendSubj, encodeFrameV2ForTest(t, mustUUIDV2(t), mustUUIDV2(t), "", map[string]any{
		"session_id":    closeIdent.SessionID,
		"connection_id": closeIdent.ConnectionID,
		"epoch":         closeIdent.Epoch,
		"seq":           1,
		"type":          "description",
		"payload":       map[string]any{"sdp": "v=0"},
	}))
	if envelopePayloadMapV2(t, closeSend.Data)["accepted"] != true {
		t.Fatalf("close-session SEND %s", closeSend.Data)
	}
	_ = nextEventV2(t, serverEv, FrameKindSignalSendV2)
	sessSubj, err := CommandSubjectV2(mgrClientNodeV2, "SESSION.COMMAND")
	if err != nil {
		t.Fatal(err)
	}
	closeReply := requestV2(t, client, sessSubj, sessionCmdFrameV2(t, mustUUIDV2(t), SessionCommandCloseV2, closeIdent, 0))
	if _, err := DecodeFrameV2(FrameKindErrorV2, closeReply.Data); err == nil {
		t.Fatalf("SESSION.CLOSE error: %s", closeReply.Data)
	}
	_ = nextPayloadV2(t, clientEv)
	_ = nextPayloadV2(t, serverEv)
	closeCut := time.Now()
	time.Sleep(250 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	for _, p := range pubs {
		if !p.at.Before(closeCut) && p.epoch == closeIdent.Epoch {
			t.Fatalf("SIGNAL retry published after SESSION.CLOSE epoch=%d", p.epoch)
		}
	}
}

type signalRetryPubV2 struct {
	at    time.Time
	epoch uint64
}

func mustCreateBindReadySessionTimeoutV2(t *testing.T, client, srv *nats.Conn, clientEv, serverEv <-chan *nats.Msg, timeoutMs int64) (IdentityV2, uint64) {
	t.Helper()
	createSubj, err := CommandSubjectV2(mgrClientNodeV2, "SESSION.CREATE")
	if err != nil {
		t.Fatal(err)
	}
	got := requestV2(t, client, createSubj, createFrameV2Timeout(t, mustUUIDV2(t), mgrServerNodeV2, timeoutMs))
	alloc, err := DecodeFrameV2(FrameKindAllocatedV2, got.Data)
	if err != nil {
		t.Fatal(err)
	}
	ident := IdentityV2{
		SessionID:    alloc.Allocated.SessionID,
		ConnectionID: alloc.Allocated.ConnectionID,
		Epoch:        alloc.Allocated.Epoch,
	}
	bindSubj, err := CommandSubjectV2(mgrClientNodeV2, "SESSION.COMMAND")
	if err != nil {
		t.Fatal(err)
	}
	_ = requestV2(t, client, bindSubj, sessionCmdFrameV2(t, mustUUIDV2(t), SessionCommandBindV2, ident, alloc.Allocated.Revision))
	cPrep := nextEventV2(t, clientEv, FrameKindPrepareV2)
	sPrep := nextEventV2(t, serverEv, FrameKindPrepareV2)
	readySubj, err := CommandSubjectV2(mgrClientNodeV2, "CONNECTION.COMMAND")
	if err != nil {
		t.Fatal(err)
	}
	srvReadySubj, err := CommandSubjectV2(mgrServerNodeV2, "CONNECTION.COMMAND")
	if err != nil {
		t.Fatal(err)
	}
	_ = requestV2(t, client, readySubj, connCmdFrameV2(t, mustUUIDV2(t), ConnectionCommandReadyV2, ident, cPrep.Prepare.Revision))
	_ = requestV2(t, srv, srvReadySubj, connCmdFrameV2(t, mustUUIDV2(t), ConnectionCommandReadyV2, ident, sPrep.Prepare.Revision))
	cStart := nextEventV2(t, clientEv, FrameKindStartV2)
	_ = nextEventV2(t, serverEv, FrameKindStartV2)
	return ident, cStart.Start.Revision
}

func signalSendPayloadV2(data []byte) bool {
	var env EnvelopeV2
	if json.Unmarshal(data, &env) != nil {
		return false
	}
	var payload map[string]any
	if json.Unmarshal(env.Payload, &payload) != nil {
		return false
	}
	_, hasType := payload["type"]
	_, hasSeq := payload["seq"]
	return hasType && hasSeq
}

func hasSignalPendingV2(m *Manager, ident IdentityV2) bool {
	dir := DirectionKeyV2{
		GenerationKeyV2: GenerationKeyV2{SessionID: ident.SessionID, ConnectionID: ident.ConnectionID, Epoch: ident.Epoch},
		SenderNodeKey:   mgrClientNodeV2,
	}
	return m.v2.store.directionPending(dir) > 0
}

func mustActiveSessionV2From(t *testing.T, s *StoreV2, client, server string) SessionSnapshotV2 {
	t.Helper()
	snap, err := s.AllocateSession(client, CreateSessionCommandV2{RequestID: mustUUIDV2(t), ServerNodeKey: server})
	if err != nil {
		t.Fatal(err)
	}
	ev, err := s.BindSession(client, sessionBindCmdV2(t, snap))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.MarkConnectionReady(client, connCmdV2(t, ConnectionCommandReadyV2, snap.SessionID, "conn-0", 1, ev[0].Revision)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MarkConnectionReady(server, connCmdV2(t, ConnectionCommandReadyV2, snap.SessionID, "conn-0", 1, ev[0].Revision)); err != nil {
		t.Fatal(err)
	}
	return snap
}

func containsSignalSubject(in []string, want string) bool {
	for _, s := range in {
		if s == want {
			return true
		}
	}
	return false
}

func isSignalEventSubject(subj string) bool {
	return len(subj) > len("$P2P.V2.EVENT.") && subj[:len("$P2P.V2.EVENT.")] == "$P2P.V2.EVENT."
}
