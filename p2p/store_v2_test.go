package p2p

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

const (
	storeClientNodeV2 = "client-a"
	storeServerNodeV2 = "server-b"
	storeOtherNodeV2  = "other-c"
	storeClientRegV2  = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	storeServerRegV2  = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	storeOtherRegV2   = "cccccccccccccccccccccccccccccccc"
	storeMaxConnsV2   = 128
)

type storeClockV2 struct {
	mu sync.Mutex
	t  time.Time
}

func newStoreClockV2() *storeClockV2 {
	return &storeClockV2{t: time.Date(2026, 8, 28, 0, 0, 0, 0, time.UTC)}
}

func (c *storeClockV2) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *storeClockV2) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newTestStoreV2(t *testing.T) (*StoreV2, *storeClockV2) {
	t.Helper()
	clock := newStoreClockV2()
	return NewStoreV2(clock.Now, storeMaxConnsV2), clock
}

func mustUUIDV2(t *testing.T) string {
	t.Helper()
	id, err := NewUUIDV2()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func requireCodeV2(t *testing.T, err error, code ErrorCodeV2) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s", code)
	}
	var perr *ProtocolErrorV2
	if !errors.As(err, &perr) || perr.Code != code {
		t.Fatalf("got %v, want %s", err, code)
	}
}

func mustRegisterNodeV2(t *testing.T, s *StoreV2, node, reg string) NodeSnapshotV2 {
	t.Helper()
	snap, err := s.RegisterNode(RegisterCommandV2{
		NodeKey:        node,
		RequestID:      mustUUIDV2(t),
		RegistrationID: reg,
	})
	if err != nil {
		t.Fatal(err)
	}
	if snap.NodeKey != node || snap.RegistrationID != reg || snap.RegistrationEpoch == 0 {
		t.Fatalf("register snapshot %+v", snap)
	}
	return snap
}

func mustAllocateSessionV2(t *testing.T, s *StoreV2, sender string) SessionSnapshotV2 {
	t.Helper()
	snap, err := s.AllocateSession(sender, CreateSessionCommandV2{
		RequestID:     mustUUIDV2(t),
		ServerNodeKey: storeServerNodeV2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if snap.SessionID == "" || snap.ConnectionID != "conn-0" || snap.Epoch != 1 {
		t.Fatalf("allocate snapshot %+v", snap)
	}
	if snap.State != SessionStateAllocatedV2 || snap.Revision != 1 {
		t.Fatalf("allocate state %+v", snap)
	}
	return snap
}

func sessionBindCmdV2(t *testing.T, snap SessionSnapshotV2) SessionCommandV2 {
	t.Helper()
	return SessionCommandV2{
		RequestID: mustUUIDV2(t),
		Command:   SessionCommandBindV2,
		IdentityV2: IdentityV2{
			SessionID:    snap.SessionID,
			ConnectionID: snap.ConnectionID,
			Epoch:        snap.Epoch,
		},
		Revision: snap.Revision,
	}
}

func connCmdV2(t *testing.T, kind ConnectionCommandKindV2, sessionID, connID string, epoch, revision uint64) ConnectionCommandV2 {
	t.Helper()
	return ConnectionCommandV2{
		RequestID: mustUUIDV2(t),
		Command:   kind,
		IdentityV2: IdentityV2{
			SessionID:    sessionID,
			ConnectionID: connID,
			Epoch:        epoch,
		},
		Revision: revision,
	}
}

func sessionRevisionV2(t *testing.T, s *StoreV2, sessionID string) uint64 {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		t.Fatalf("session %s not live", sessionID)
	}
	return sess.revision
}

func eventTargetsV2(events []EventV2) map[string][]FrameKindV2 {
	out := make(map[string][]FrameKindV2)
	for _, ev := range events {
		out[ev.TargetNodeKey] = append(out[ev.TargetNodeKey], ev.Kind)
	}
	return out
}

func requireBothKindsV2(t *testing.T, events []EventV2, kind FrameKindV2) {
	t.Helper()
	targets := eventTargetsV2(events)
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2: %+v", len(events), events)
	}
	if len(targets[storeClientNodeV2]) != 1 || targets[storeClientNodeV2][0] != kind {
		t.Fatalf("client events %v, want %s", targets[storeClientNodeV2], kind)
	}
	if len(targets[storeServerNodeV2]) != 1 || targets[storeServerNodeV2][0] != kind {
		t.Fatalf("server events %v, want %s", targets[storeServerNodeV2], kind)
	}
	for _, ev := range events {
		if ev.MessageID == "" || ev.RegistrationID == "" {
			t.Fatalf("event missing ids %+v", ev)
		}
		if ev.Identity.ConnectionID == "" || ev.Identity.Epoch == 0 {
			t.Fatalf("event missing identity %+v", ev)
		}
	}
}

func TestStoreV2_CreateOnlyAllocates(t *testing.T) {
	s, _ := newTestStoreV2(t)
	mustRegisterNodeV2(t, s, storeClientNodeV2, storeClientRegV2)
	mustRegisterNodeV2(t, s, storeServerNodeV2, storeServerRegV2)

	snap := mustAllocateSessionV2(t, s, storeClientNodeV2)
	if snap.ClientNodeKey != storeClientNodeV2 || snap.ServerNodeKey != storeServerNodeV2 {
		t.Fatalf("members %+v", snap)
	}
}

func TestStoreV2_SessionBindEmitsConn0Prepare(t *testing.T) {
	s, _ := newTestStoreV2(t)
	mustRegisterNodeV2(t, s, storeClientNodeV2, storeClientRegV2)
	mustRegisterNodeV2(t, s, storeServerNodeV2, storeServerRegV2)
	snap := mustAllocateSessionV2(t, s, storeClientNodeV2)

	events, err := s.BindSession(storeClientNodeV2, sessionBindCmdV2(t, snap))
	if err != nil {
		t.Fatal(err)
	}
	requireBothKindsV2(t, events, FrameKindPrepareV2)
	for _, ev := range events {
		if ev.Identity.ConnectionID != "conn-0" || ev.Identity.Epoch != 1 {
			t.Fatalf("prepare identity %+v", ev.Identity)
		}
		if ev.Prepare == nil || ev.Prepare.Role == "" || ev.Prepare.PeerNodeKey == "" {
			t.Fatalf("prepare payload %+v", ev)
		}
		if ev.Prepare.SetupDeadlineMs <= 0 {
			t.Fatalf("setup deadline %d", ev.Prepare.SetupDeadlineMs)
		}
	}
	roles := map[string]string{}
	for _, ev := range events {
		roles[ev.TargetNodeKey] = ev.Prepare.Role
	}
	if roles[storeClientNodeV2] != "client" || roles[storeServerNodeV2] != "server" {
		t.Fatalf("roles %v", roles)
	}
}

func TestStoreV2_BothReadyEmitsStart(t *testing.T) {
	s, _ := newTestStoreV2(t)
	mustRegisterNodeV2(t, s, storeClientNodeV2, storeClientRegV2)
	mustRegisterNodeV2(t, s, storeServerNodeV2, storeServerRegV2)
	snap := mustAllocateSessionV2(t, s, storeClientNodeV2)
	events, err := s.BindSession(storeClientNodeV2, sessionBindCmdV2(t, snap))
	if err != nil {
		t.Fatal(err)
	}
	rev := events[0].Revision

	first, err := s.MarkConnectionReady(storeClientNodeV2, connCmdV2(t, ConnectionCommandReadyV2, snap.SessionID, "conn-0", 1, rev))
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 0 {
		t.Fatalf("single ready published %+v", first)
	}
	// First READY is a no-publish mark: revision stays at the BIND/PREPARE value so the
	// peer can READY with the same revision. Revision increments only when START is emitted.
	if got := sessionRevisionV2(t, s, snap.SessionID); got != rev {
		t.Fatalf("first READY bumped revision to %d, want unchanged %d", got, rev)
	}

	second, err := s.MarkConnectionReady(storeServerNodeV2, connCmdV2(t, ConnectionCommandReadyV2, snap.SessionID, "conn-0", 1, rev))
	if err != nil {
		t.Fatal(err)
	}
	requireBothKindsV2(t, second, FrameKindStartV2)
	if second[0].Revision <= rev {
		t.Fatalf("START revision %d must increase past BIND revision %d", second[0].Revision, rev)
	}
}

func TestStoreV2_StateCommandsIdempotentByRequestID(t *testing.T) {
	s, _ := newTestStoreV2(t)
	mustRegisterNodeV2(t, s, storeClientNodeV2, storeClientRegV2)
	mustRegisterNodeV2(t, s, storeServerNodeV2, storeServerRegV2)

	create := CreateSessionCommandV2{RequestID: mustUUIDV2(t), ServerNodeKey: storeServerNodeV2}
	first, err := s.AllocateSession(storeClientNodeV2, create)
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.AllocateSession(storeClientNodeV2, create)
	if err != nil {
		t.Fatal(err)
	}
	if again != first {
		t.Fatalf("create idempotency %+v vs %+v", first, again)
	}

	bind := sessionBindCmdV2(t, first)
	ev1, err := s.BindSession(storeClientNodeV2, bind)
	if err != nil {
		t.Fatal(err)
	}
	ev2, err := s.BindSession(storeClientNodeV2, bind)
	if err != nil {
		t.Fatal(err)
	}
	if len(ev2) != len(ev1) || ev2[0].MessageID != ev1[0].MessageID {
		t.Fatalf("bind replay events %+v vs %+v", ev1, ev2)
	}

	ready := connCmdV2(t, ConnectionCommandReadyV2, first.SessionID, "conn-0", 1, ev1[0].Revision)
	if _, err := s.MarkConnectionReady(storeClientNodeV2, ready); err != nil {
		t.Fatal(err)
	}
	replay, err := s.MarkConnectionReady(storeClientNodeV2, ready)
	if err != nil {
		t.Fatal(err)
	}
	if len(replay) != 0 {
		t.Fatalf("ready replay published %+v", replay)
	}
}

func TestStoreV2_OpenRestartTwoPhase(t *testing.T) {
	s, _ := newTestStoreV2(t)
	mustRegisterNodeV2(t, s, storeClientNodeV2, storeClientRegV2)
	mustRegisterNodeV2(t, s, storeServerNodeV2, storeServerRegV2)
	sess := mustAllocateSessionV2(t, s, storeClientNodeV2)
	bindEv, err := s.BindSession(storeClientNodeV2, sessionBindCmdV2(t, sess))
	if err != nil {
		t.Fatal(err)
	}
	rev := bindEv[0].Revision
	if _, err := s.MarkConnectionReady(storeClientNodeV2, connCmdV2(t, ConnectionCommandReadyV2, sess.SessionID, "conn-0", 1, rev)); err != nil {
		t.Fatal(err)
	}
	startEv, err := s.MarkConnectionReady(storeServerNodeV2, connCmdV2(t, ConnectionCommandReadyV2, sess.SessionID, "conn-0", 1, rev))
	if err != nil {
		t.Fatal(err)
	}
	rev = startEv[0].Revision

	open, err := s.AllocateConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandOpenV2, sess.SessionID, "", 0, rev))
	if err != nil {
		t.Fatal(err)
	}
	if open.ConnectionID != "conn-1" || open.Epoch != 1 || open.State != ConnectionStateAllocatedV2 {
		t.Fatalf("open snapshot %+v", open)
	}

	restart, err := s.AllocateRestart(storeClientNodeV2, connCmdV2(t, ConnectionCommandRestartV2, sess.SessionID, "conn-0", 1, open.Revision))
	if err != nil {
		t.Fatal(err)
	}
	if restart.ConnectionID != "conn-0" || restart.Epoch != 2 {
		t.Fatalf("restart snapshot %+v", restart)
	}
	if restart.State != ConnectionStateAllocatedV2 {
		t.Fatalf("restart must stay allocated before bind %+v", restart)
	}

	bindOpen, err := s.BindConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandBindV2, sess.SessionID, open.ConnectionID, open.Epoch, restart.Revision))
	if err != nil {
		t.Fatal(err)
	}
	requireBothKindsV2(t, bindOpen, FrameKindPrepareV2)
	for _, ev := range bindOpen {
		if ev.Identity.ConnectionID != "conn-1" || ev.Identity.Epoch != 1 {
			t.Fatalf("open prepare %+v", ev.Identity)
		}
	}

	bindRestart, err := s.BindConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandBindV2, sess.SessionID, "conn-0", 2, bindOpen[0].Revision))
	if err != nil {
		t.Fatal(err)
	}
	requireBothKindsV2(t, bindRestart, FrameKindPrepareV2)
	for _, ev := range bindRestart {
		if ev.Identity.Epoch != 2 {
			t.Fatalf("restart prepare epoch %+v", ev.Identity)
		}
	}
}

func TestStoreV2_Isolation(t *testing.T) {
	s, _ := newTestStoreV2(t)
	mustRegisterNodeV2(t, s, storeClientNodeV2, storeClientRegV2)
	mustRegisterNodeV2(t, s, storeServerNodeV2, storeServerRegV2)
	mustRegisterNodeV2(t, s, storeOtherNodeV2, storeOtherRegV2)

	sharedReq := mustUUIDV2(t)
	sessA, err := s.AllocateSession(storeClientNodeV2, CreateSessionCommandV2{RequestID: sharedReq, ServerNodeKey: storeServerNodeV2})
	if err != nil {
		t.Fatal(err)
	}
	sessB, err := s.AllocateSession(storeOtherNodeV2, CreateSessionCommandV2{RequestID: sharedReq, ServerNodeKey: storeServerNodeV2})
	if err != nil {
		t.Fatal(err)
	}
	if sessA.SessionID == sessB.SessionID {
		t.Fatal("shared request id across nodes collided")
	}
	if sessA.ConnectionID != "conn-0" || sessB.ConnectionID != "conn-0" {
		t.Fatalf("both sessions should own conn-0: %+v %+v", sessA, sessB)
	}

	msg := "11111111-1111-4111-8111-111111111111"
	first := s.RememberMessage(MessageKeyV2{
		Direction: DirectionKeyV2{GenerationKeyV2: GenerationKeyV2{sessA.SessionID, "conn-0", 1}, SenderNodeKey: storeClientNodeV2},
		MessageID: msg,
	})
	otherDir := s.RememberMessage(MessageKeyV2{
		Direction: DirectionKeyV2{GenerationKeyV2: GenerationKeyV2{sessA.SessionID, "conn-0", 1}, SenderNodeKey: storeServerNodeV2},
		MessageID: msg,
	})
	if !first || !otherDir {
		t.Fatal("same message id on different DirectionKey must not dedup")
	}
	dup := s.RememberMessage(MessageKeyV2{
		Direction: DirectionKeyV2{GenerationKeyV2: GenerationKeyV2{sessA.SessionID, "conn-0", 1}, SenderNodeKey: storeClientNodeV2},
		MessageID: msg,
	})
	if dup {
		t.Fatal("same DirectionKey + message id should dedup")
	}

	bind := sessionBindCmdV2(t, sessA)
	if _, err := s.BindSession(storeOtherNodeV2, bind); err == nil {
		t.Fatal("expected not_session_member")
	} else {
		requireCodeV2(t, err, ErrNotSessionMemberV2)
	}

	if _, err := s.BindSession(storeClientNodeV2, SessionCommandV2{
		RequestID: mustUUIDV2(t),
		Command:   SessionCommandBindV2,
		IdentityV2: IdentityV2{
			SessionID:    mustUUIDV2(t),
			ConnectionID: "conn-0",
			Epoch:        1,
		},
	}); err == nil {
		t.Fatal("expected session_not_found")
	} else {
		requireCodeV2(t, err, ErrSessionNotFoundV2)
	}

	ev, err := s.BindSession(storeClientNodeV2, bind)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.MarkConnectionReady(storeClientNodeV2, connCmdV2(t, ConnectionCommandReadyV2, sessA.SessionID, "conn-0", 1, ev[0].Revision)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MarkConnectionReady(storeClientNodeV2, connCmdV2(t, ConnectionCommandReadyV2, sessA.SessionID, "conn-0", 99, ev[0].Revision)); err == nil {
		t.Fatal("expected future_epoch")
	} else {
		requireCodeV2(t, err, ErrFutureEpochV2)
	}

	readyEv, err := s.MarkConnectionReady(storeServerNodeV2, connCmdV2(t, ConnectionCommandReadyV2, sessA.SessionID, "conn-0", 1, ev[0].Revision))
	if err != nil {
		t.Fatal(err)
	}
	restart, err := s.AllocateRestart(storeClientNodeV2, connCmdV2(t, ConnectionCommandRestartV2, sessA.SessionID, "conn-0", 1, readyEv[0].Revision))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.MarkConnectionReady(storeClientNodeV2, connCmdV2(t, ConnectionCommandReadyV2, sessA.SessionID, "conn-0", 1, restart.Revision)); err == nil {
		t.Fatal("expected stale_epoch")
	} else {
		requireCodeV2(t, err, ErrStaleEpochV2)
	}

	if _, err := s.MarkConnectionReady(storeOtherNodeV2, connCmdV2(t, ConnectionCommandReadyV2, sessB.SessionID, "conn-0", 1, 1)); err == nil {
		t.Fatal("expected invalid_state before bind")
	} else {
		requireCodeV2(t, err, ErrInvalidStateV2)
	}
}

func TestStoreV2_ConnectionLimit(t *testing.T) {
	s, _ := newTestStoreV2(t)
	mustRegisterNodeV2(t, s, storeClientNodeV2, storeClientRegV2)
	mustRegisterNodeV2(t, s, storeServerNodeV2, storeServerRegV2)
	mustRegisterNodeV2(t, s, storeOtherNodeV2, storeOtherRegV2)

	sess := mustAllocateSessionV2(t, s, storeClientNodeV2)
	rev := sess.Revision
	var last ConnectionSnapshotV2
	for i := 1; i < storeMaxConnsV2; i++ {
		open, err := s.AllocateConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandOpenV2, sess.SessionID, fmt.Sprintf("conn-%d", i), 0, rev))
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		last = open
		rev = open.Revision
	}
	if last.ConnectionID != "conn-127" {
		t.Fatalf("128th connection id %s", last.ConnectionID)
	}

	if _, err := s.AllocateConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandOpenV2, sess.SessionID, "conn-128", 0, rev)); err == nil {
		t.Fatal("expected connection_limit")
	} else {
		requireCodeV2(t, err, ErrConnectionLimitV2)
	}

	other, err := s.AllocateSession(storeOtherNodeV2, CreateSessionCommandV2{RequestID: mustUUIDV2(t), ServerNodeKey: storeServerNodeV2})
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.AllocateConnection(storeOtherNodeV2, connCmdV2(t, ConnectionCommandOpenV2, other.SessionID, "", 0, other.Revision))
	if err != nil {
		t.Fatal(err)
	}
	if first.ConnectionID != "conn-1" {
		t.Fatalf("other session first extra connection %+v", first)
	}

	restart, err := s.AllocateRestart(storeClientNodeV2, connCmdV2(t, ConnectionCommandRestartV2, sess.SessionID, "conn-0", 1, rev))
	if err != nil {
		t.Fatal(err)
	}
	if restart.Epoch != 2 {
		t.Fatalf("restart epoch %+v", restart)
	}
	if _, err := s.AllocateConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandOpenV2, sess.SessionID, "conn-200", 0, restart.Revision)); err == nil {
		t.Fatal("restart must not free a slot")
	} else {
		requireCodeV2(t, err, ErrConnectionLimitV2)
	}
}

func TestStoreV2_Expire(t *testing.T) {
	s, clock := newTestStoreV2(t)
	mustRegisterNodeV2(t, s, storeClientNodeV2, storeClientRegV2)
	mustRegisterNodeV2(t, s, storeServerNodeV2, storeServerRegV2)

	setupSess := mustAllocateSessionV2(t, s, storeClientNodeV2)
	clock.Advance(DefaultSetupTimeoutV2 + time.Millisecond)
	expired := s.Expire(clock.Now())
	if len(expired) == 0 {
		t.Fatal("setup deadline should emit expire events")
	}
	if _, err := s.BindSession(storeClientNodeV2, sessionBindCmdV2(t, setupSess)); err == nil {
		t.Fatal("expired allocated session should not bind")
	} else {
		requireCodeV2(t, err, ErrSessionNotFoundV2)
	}

	active := mustAllocateSessionV2(t, s, storeClientNodeV2)
	bindEv, err := s.BindSession(storeClientNodeV2, sessionBindCmdV2(t, active))
	if err != nil {
		t.Fatal(err)
	}
	rev := bindEv[0].Revision
	if _, err := s.MarkConnectionReady(storeClientNodeV2, connCmdV2(t, ConnectionCommandReadyV2, active.SessionID, "conn-0", 1, rev)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MarkConnectionReady(storeServerNodeV2, connCmdV2(t, ConnectionCommandReadyV2, active.SessionID, "conn-0", 1, rev)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(24 * time.Hour)
	if ev := s.Expire(clock.Now()); len(ev) != 0 {
		t.Fatalf("active session has no idle timeout: %+v", ev)
	}
	if _, err := s.AllocateConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandOpenV2, active.SessionID, "conn-1", 0, 0)); err != nil {
		t.Fatalf("active session should survive idle expire: %v", err)
	}

	s.MarkNodeDisconnected(storeOtherNodeV2)
	mustRegisterNodeV2(t, s, storeOtherNodeV2, storeOtherRegV2)
	s.MarkNodeDisconnected(storeOtherNodeV2)
	clock.Advance(NodeDisconnectGraceV2 - time.Second)
	_ = s.Expire(clock.Now())
	if _, err := s.AllocateSession(storeOtherNodeV2, CreateSessionCommandV2{RequestID: mustUUIDV2(t), ServerNodeKey: storeServerNodeV2}); err != nil {
		t.Fatalf("grace not elapsed: %v", err)
	}
	s.MarkNodeDisconnected(storeOtherNodeV2)
	clock.Advance(NodeDisconnectGraceV2 + time.Millisecond)
	_ = s.Expire(clock.Now())
	if _, err := s.AllocateSession(storeOtherNodeV2, CreateSessionCommandV2{RequestID: mustUUIDV2(t), ServerNodeKey: storeServerNodeV2}); err == nil {
		t.Fatal("expected not_registered after disconnect grace")
	} else {
		requireCodeV2(t, err, ErrNotRegisteredV2)
	}

	closeSess := mustAllocateSessionV2(t, s, storeClientNodeV2)
	closeReq := SessionCommandV2{
		RequestID: mustUUIDV2(t),
		Command:   SessionCommandCloseV2,
		IdentityV2: IdentityV2{
			SessionID:    closeSess.SessionID,
			ConnectionID: "conn-0",
			Epoch:        1,
		},
	}
	if _, err := s.BindSession(storeClientNodeV2, closeReq); err != nil {
		t.Fatal(err)
	}
	replay, err := s.BindSession(storeClientNodeV2, closeReq)
	if err != nil {
		t.Fatal(err)
	}
	if replay != nil && len(replay) == 0 {
		// idempotent close may return stored events or empty current state
	}
	clock.Advance(DefaultTombstoneTTLV2 + time.Millisecond)
	_ = s.Expire(clock.Now())
	if _, err := s.BindSession(storeClientNodeV2, sessionBindCmdV2(t, closeSess)); err == nil {
		t.Fatal("tombstone should be gone")
	} else {
		requireCodeV2(t, err, ErrSessionNotFoundV2)
	}
}

func TestStoreV2_ExpireUsesInjectedDurations(t *testing.T) {
	s, clock := newTestStoreV2(t)
	mustRegisterNodeV2(t, s, storeClientNodeV2, storeClientRegV2)
	mustRegisterNodeV2(t, s, storeServerNodeV2, storeServerRegV2)
	s.SetExpiryDurations(40*time.Millisecond, 80*time.Millisecond)
	if s.disconnectGrace() != 40*time.Millisecond || s.tombstoneTTL() != 80*time.Millisecond {
		t.Fatalf("injected grace=%s tomb=%s", s.disconnectGrace(), s.tombstoneTTL())
	}

	mustRegisterNodeV2(t, s, storeOtherNodeV2, storeOtherRegV2)
	s.MarkNodeDisconnected(storeOtherNodeV2)
	clock.Advance(39 * time.Millisecond)
	_ = s.Expire(clock.Now())
	if _, err := s.AllocateSession(storeOtherNodeV2, CreateSessionCommandV2{RequestID: mustUUIDV2(t), ServerNodeKey: storeServerNodeV2}); err != nil {
		t.Fatalf("custom grace not elapsed: %v", err)
	}
	s.MarkNodeDisconnected(storeOtherNodeV2)
	clock.Advance(40*time.Millisecond + time.Millisecond)
	_ = s.Expire(clock.Now())
	if _, err := s.AllocateSession(storeOtherNodeV2, CreateSessionCommandV2{RequestID: mustUUIDV2(t), ServerNodeKey: storeServerNodeV2}); err == nil {
		t.Fatal("expected not_registered after injected grace")
	} else {
		requireCodeV2(t, err, ErrNotRegisteredV2)
	}

	closeSess := mustAllocateSessionV2(t, s, storeClientNodeV2)
	closeReq := SessionCommandV2{
		RequestID: mustUUIDV2(t),
		Command:   SessionCommandCloseV2,
		IdentityV2: IdentityV2{
			SessionID:    closeSess.SessionID,
			ConnectionID: "conn-0",
			Epoch:        1,
		},
	}
	if _, err := s.BindSession(storeClientNodeV2, closeReq); err != nil {
		t.Fatal(err)
	}
	clock.Advance(79 * time.Millisecond)
	_ = s.Expire(clock.Now())
	if _, err := s.BindSession(storeClientNodeV2, closeReq); err != nil {
		t.Fatalf("custom tombstone still live: %v", err)
	}
	clock.Advance(80*time.Millisecond + time.Millisecond)
	_ = s.Expire(clock.Now())
	if _, err := s.BindSession(storeClientNodeV2, sessionBindCmdV2(t, closeSess)); err == nil {
		t.Fatal("injected tombstone should be gone")
	} else {
		requireCodeV2(t, err, ErrSessionNotFoundV2)
	}
}

func TestStoreV2_UnregisteredAndPeerErrors(t *testing.T) {
	s, _ := newTestStoreV2(t)
	if _, err := s.AllocateSession(storeClientNodeV2, CreateSessionCommandV2{RequestID: mustUUIDV2(t), ServerNodeKey: storeServerNodeV2}); err == nil {
		t.Fatal("expected not_registered")
	} else {
		requireCodeV2(t, err, ErrNotRegisteredV2)
	}
	mustRegisterNodeV2(t, s, storeClientNodeV2, storeClientRegV2)
	if _, err := s.AllocateSession(storeClientNodeV2, CreateSessionCommandV2{RequestID: mustUUIDV2(t), ServerNodeKey: storeServerNodeV2}); err == nil {
		t.Fatal("expected peer_not_registered")
	} else {
		requireCodeV2(t, err, ErrPeerNotRegisteredV2)
	}
}

func TestStoreV2_CloseConnection(t *testing.T) {
	s, _ := newTestStoreV2(t)
	mustRegisterNodeV2(t, s, storeClientNodeV2, storeClientRegV2)
	mustRegisterNodeV2(t, s, storeServerNodeV2, storeServerRegV2)
	sess := mustAllocateSessionV2(t, s, storeClientNodeV2)
	open, err := s.AllocateConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandOpenV2, sess.SessionID, "conn-1", 0, sess.Revision))
	if err != nil {
		t.Fatal(err)
	}
	closeEv, err := s.CloseConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandCloseV2, sess.SessionID, "conn-1", open.Epoch, open.Revision))
	if err != nil {
		t.Fatal(err)
	}
	if len(closeEv) == 0 {
		t.Fatal("close should notify both peers")
	}
	if _, err := s.BindConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandBindV2, sess.SessionID, "conn-1", open.Epoch, open.Revision)); err == nil {
		t.Fatal("closed connection should not bind")
	} else {
		requireCodeV2(t, err, ErrInvalidStateV2)
	}
}

func TestStoreV2_RejectConnection(t *testing.T) {
	newSession := func(t *testing.T) (*StoreV2, SessionSnapshotV2) {
		t.Helper()
		s, _ := newTestStoreV2(t)
		mustRegisterNodeV2(t, s, storeClientNodeV2, storeClientRegV2)
		mustRegisterNodeV2(t, s, storeServerNodeV2, storeServerRegV2)
		return s, mustAllocateSessionV2(t, s, storeClientNodeV2)
	}

	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, s *StoreV2, snap SessionSnapshotV2) (IdentityV2, uint64)
	}{
		{
			name: "allocated",
			setup: func(t *testing.T, _ *StoreV2, snap SessionSnapshotV2) (IdentityV2, uint64) {
				return IdentityV2{SessionID: snap.SessionID, ConnectionID: "conn-0", Epoch: 1}, snap.Revision
			},
		},
		{
			name: "preparing",
			setup: func(t *testing.T, s *StoreV2, snap SessionSnapshotV2) (IdentityV2, uint64) {
				events, err := s.BindConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandBindV2, snap.SessionID, "conn-0", 1, snap.Revision))
				if err != nil {
					t.Fatal(err)
				}
				return IdentityV2{SessionID: snap.SessionID, ConnectionID: "conn-0", Epoch: 1}, events[0].Revision
			},
		},
		{
			name: "restarting",
			setup: func(t *testing.T, s *StoreV2, snap SessionSnapshotV2) (IdentityV2, uint64) {
				s.mu.Lock()
				s.sessions[snap.SessionID].connections["conn-0"].state = ConnectionStateRestartingV2
				s.mu.Unlock()
				return IdentityV2{SessionID: snap.SessionID, ConnectionID: "conn-0", Epoch: 1}, snap.Revision
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, snap := newSession(t)
			ident, rev := tc.setup(t, s, snap)
			cmd := connCmdV2(t, ConnectionCommandRejectV2, ident.SessionID, ident.ConnectionID, ident.Epoch, rev)
			events, err := s.RejectConnection(storeClientNodeV2, cmd)
			if err != nil {
				t.Fatal(err)
			}
			requireBothKindsV2(t, events, FrameKindCloseV2)
			if got := sessionRevisionV2(t, s, ident.SessionID); got != rev+1 {
				t.Fatalf("revision=%d want %d", got, rev+1)
			}
			s.mu.Lock()
			state := s.sessions[ident.SessionID].connections[ident.ConnectionID].state
			s.mu.Unlock()
			if state != ConnectionStateClosedV2 {
				t.Fatalf("state=%s want closed", state)
			}

			again, err := s.RejectConnection(storeClientNodeV2, cmd)
			if err != nil {
				t.Fatal(err)
			}
			if len(again) != 0 {
				t.Fatalf("duplicate request must not create additional CLOSE events: %+v", again)
			}
		})
	}

	t.Run("rejects active non-member and wrong epoch without mutation", func(t *testing.T) {
		s, snap := newSession(t)
		ident := IdentityV2{SessionID: snap.SessionID, ConnectionID: "conn-0", Epoch: 1}
		bind, err := s.BindConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandBindV2, ident.SessionID, ident.ConnectionID, ident.Epoch, snap.Revision))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.MarkConnectionReady(storeClientNodeV2, connCmdV2(t, ConnectionCommandReadyV2, ident.SessionID, ident.ConnectionID, ident.Epoch, bind[0].Revision)); err != nil {
			t.Fatal(err)
		}
		if _, err = s.MarkConnectionReady(storeServerNodeV2, connCmdV2(t, ConnectionCommandReadyV2, ident.SessionID, ident.ConnectionID, ident.Epoch, bind[0].Revision)); err != nil {
			t.Fatal(err)
		}
		rev := sessionRevisionV2(t, s, ident.SessionID)
		if _, err := s.RejectConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandRejectV2, ident.SessionID, ident.ConnectionID, ident.Epoch, rev)); err == nil {
			t.Fatal("active reject must fail")
		} else {
			requireCodeV2(t, err, ErrInvalidStateV2)
		}
		mustRegisterNodeV2(t, s, storeOtherNodeV2, storeOtherRegV2)
		if _, err := s.RejectConnection(storeOtherNodeV2, connCmdV2(t, ConnectionCommandRejectV2, ident.SessionID, ident.ConnectionID, ident.Epoch, rev)); err == nil {
			t.Fatal("non-member reject must fail")
		} else {
			requireCodeV2(t, err, ErrNotSessionMemberV2)
		}
		s.mu.Lock()
		s.sessions[ident.SessionID].connections[ident.ConnectionID].epoch = 2
		s.sessions[ident.SessionID].connections[ident.ConnectionID].state = ConnectionStatePreparingV2
		s.mu.Unlock()
		for _, tc := range []struct {
			name  string
			epoch uint64
			code  ErrorCodeV2
		}{{"stale", 1, ErrStaleEpochV2}, {"future", 3, ErrFutureEpochV2}} {
			t.Run(tc.name, func(t *testing.T) {
				_, err := s.RejectConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandRejectV2, ident.SessionID, ident.ConnectionID, tc.epoch, rev))
				requireCodeV2(t, err, tc.code)
			})
		}
		if got := sessionRevisionV2(t, s, ident.SessionID); got != rev {
			t.Fatalf("failed reject mutated revision=%d want %d", got, rev)
		}
	})

	t.Run("clears pending signals", func(t *testing.T) {
		s, snap := newSession(t)
		ident := IdentityV2{SessionID: snap.SessionID, ConnectionID: "conn-0", Epoch: 1}
		bind, err := s.BindConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandBindV2, ident.SessionID, ident.ConnectionID, ident.Epoch, snap.Revision))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.MarkConnectionReady(storeClientNodeV2, connCmdV2(t, ConnectionCommandReadyV2, ident.SessionID, ident.ConnectionID, ident.Epoch, bind[0].Revision)); err != nil {
			t.Fatal(err)
		}
		if _, err = s.MarkConnectionReady(storeServerNodeV2, connCmdV2(t, ConnectionCommandReadyV2, ident.SessionID, ident.ConnectionID, ident.Epoch, bind[0].Revision)); err != nil {
			t.Fatal(err)
		}
		cmd := signalSendCmdV2(t, ident, 1, SignalKindEndOfCandidatesV2, SignalPayloadV2{})
		if _, _, err := s.AcceptSignal(storeClientNodeV2, cmd); err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.AcceptSignal(storeServerNodeV2, signalSendCmdV2(t, ident, 1, SignalKindEndOfCandidatesV2, SignalPayloadV2{})); err != nil {
			t.Fatal(err)
		}
		dir := dirKeyV2(snap, storeClientNodeV2)
		reverseDir := dirKeyV2(snap, storeServerNodeV2)
		if s.directionPending(dir) != 1 || s.directionPending(reverseDir) != 1 {
			t.Fatalf("expected bidirectional pending signals: client=%d server=%d", s.directionPending(dir), s.directionPending(reverseDir))
		}
		s.mu.Lock()
		s.sessions[ident.SessionID].connections[ident.ConnectionID].state = ConnectionStatePreparingV2
		rev := s.sessions[ident.SessionID].revision
		s.mu.Unlock()
		if _, err := s.RejectConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandRejectV2, ident.SessionID, ident.ConnectionID, ident.Epoch, rev)); err != nil {
			t.Fatal(err)
		}
		if s.directionPending(dir) != 0 || s.directionPending(reverseDir) != 0 || s.hasDirection(dir) || s.hasDirection(reverseDir) {
			t.Fatalf("reject must drop pending generation: client pending=%d has=%v server pending=%d has=%v", s.directionPending(dir), s.hasDirection(dir), s.directionPending(reverseDir), s.hasDirection(reverseDir))
		}
	})
}

func TestStoreV2_BindConnectionRequiresRevision(t *testing.T) {
	s, _ := newTestStoreV2(t)
	mustRegisterNodeV2(t, s, storeClientNodeV2, storeClientRegV2)
	mustRegisterNodeV2(t, s, storeServerNodeV2, storeServerRegV2)
	sess := mustAllocateSessionV2(t, s, storeClientNodeV2)
	open, err := s.AllocateConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandOpenV2, sess.SessionID, "conn-1", 0, sess.Revision))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.BindConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandBindV2, sess.SessionID, "conn-1", open.Epoch, open.Revision-1)); err == nil {
		t.Fatal("expected stale_revision")
	} else {
		requireCodeV2(t, err, ErrStaleRevisionV2)
	}
	ev, err := s.BindConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandBindV2, sess.SessionID, "conn-1", open.Epoch, open.Revision))
	if err != nil {
		t.Fatal(err)
	}
	requireBothKindsV2(t, ev, FrameKindPrepareV2)
}

func TestStoreV2_ConcurrentSessions(t *testing.T) {
	s, _ := newTestStoreV2(t)
	mustRegisterNodeV2(t, s, storeClientNodeV2, storeClientRegV2)
	mustRegisterNodeV2(t, s, storeServerNodeV2, storeServerRegV2)
	mustRegisterNodeV2(t, s, storeOtherNodeV2, storeOtherRegV2)

	var wg sync.WaitGroup
	errc := make(chan error, 2)
	run := func(sender string) {
		defer wg.Done()
		snap, err := s.AllocateSession(sender, CreateSessionCommandV2{RequestID: mustUUIDV2(t), ServerNodeKey: storeServerNodeV2})
		if err != nil {
			errc <- err
			return
		}
		ev, err := s.BindSession(sender, sessionBindCmdV2(t, snap))
		if err != nil {
			errc <- err
			return
		}
		if _, err := s.MarkConnectionReady(sender, connCmdV2(t, ConnectionCommandReadyV2, snap.SessionID, "conn-0", 1, ev[0].Revision)); err != nil {
			errc <- err
			return
		}
		if _, err := s.MarkConnectionReady(storeServerNodeV2, connCmdV2(t, ConnectionCommandReadyV2, snap.SessionID, "conn-0", 1, ev[0].Revision)); err != nil {
			errc <- err
		}
	}
	wg.Add(2)
	go run(storeClientNodeV2)
	go run(storeOtherNodeV2)
	wg.Wait()
	close(errc)
	for err := range errc {
		t.Fatal(err)
	}
}

func TestStoreV2_ExpireActiveSessionConnectionSetupDeadline(t *testing.T) {
	s, clock := newTestStoreV2(t)
	mustRegisterNodeV2(t, s, storeClientNodeV2, storeClientRegV2)
	mustRegisterNodeV2(t, s, storeServerNodeV2, storeServerRegV2)
	sess := mustAllocateSessionV2(t, s, storeClientNodeV2)
	bindEv, err := s.BindSession(storeClientNodeV2, sessionBindCmdV2(t, sess))
	if err != nil {
		t.Fatal(err)
	}
	rev := bindEv[0].Revision
	if _, err := s.MarkConnectionReady(storeClientNodeV2, connCmdV2(t, ConnectionCommandReadyV2, sess.SessionID, "conn-0", 1, rev)); err != nil {
		t.Fatal(err)
	}
	startEv, err := s.MarkConnectionReady(storeServerNodeV2, connCmdV2(t, ConnectionCommandReadyV2, sess.SessionID, "conn-0", 1, rev))
	if err != nil {
		t.Fatal(err)
	}
	rev = startEv[0].Revision

	open, err := s.AllocateConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandOpenV2, sess.SessionID, "conn-1", 0, rev))
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(DefaultSetupTimeoutV2 + time.Millisecond)
	expired := s.Expire(clock.Now())
	if len(expired) == 0 {
		t.Fatal("OPEN generation past setup deadline should emit fail events")
	}
	for _, ev := range expired {
		if ev.Kind != FrameKindErrorV2 {
			t.Fatalf("expire event %+v, want ERROR", ev)
		}
		if ev.Identity.ConnectionID != "conn-1" {
			t.Fatalf("expire must fail the OPEN generation, not the session: %+v", ev.Identity)
		}
	}
	if _, err := s.BindConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandBindV2, sess.SessionID, "conn-1", open.Epoch, open.Revision)); err == nil {
		t.Fatal("expired OPEN generation should not bind")
	} else {
		requireCodeV2(t, err, ErrInvalidStateV2)
	}
	if _, err := s.MarkConnectionReady(storeClientNodeV2, connCmdV2(t, ConnectionCommandReadyV2, sess.SessionID, "conn-1", open.Epoch, open.Revision)); err == nil {
		t.Fatal("expired OPEN generation should not ready")
	} else {
		requireCodeV2(t, err, ErrInvalidStateV2)
	}
	if _, err := s.AllocateConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandOpenV2, sess.SessionID, "conn-1", 0, sessionRevisionV2(t, s, sess.SessionID))); err == nil {
		t.Fatal("timed-out connection_id should still occupy its slot")
	} else {
		requireCodeV2(t, err, ErrInvalidStateV2)
	}

	again, err := s.AllocateConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandOpenV2, sess.SessionID, "conn-2", 0, sessionRevisionV2(t, s, sess.SessionID)))
	if err != nil {
		t.Fatalf("ACTIVE session should accept a new OPEN after generation setup_timeout: %v", err)
	}
	if again.ConnectionID != "conn-2" || again.State != ConnectionStateAllocatedV2 {
		t.Fatalf("new OPEN snapshot %+v", again)
	}

	restart, err := s.AllocateRestart(storeClientNodeV2, connCmdV2(t, ConnectionCommandRestartV2, sess.SessionID, "conn-0", 1, again.Revision))
	if err != nil {
		t.Fatal(err)
	}
	rev = restart.Revision
	for i := 3; i < storeMaxConnsV2; i++ {
		openN, err := s.AllocateConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandOpenV2, sess.SessionID, fmt.Sprintf("conn-%d", i), 0, rev))
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		rev = openN.Revision
	}
	clock.Advance(DefaultSetupTimeoutV2 + time.Millisecond)
	_ = s.Expire(clock.Now())
	if _, err := s.BindConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandBindV2, sess.SessionID, "conn-0", restart.Epoch, restart.Revision)); err == nil {
		t.Fatal("expired RESTART generation should not drop conn-0")
	} else {
		requireCodeV2(t, err, ErrInvalidStateV2)
	}
	if _, err := s.AllocateConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandOpenV2, sess.SessionID, "conn-128", 0, sessionRevisionV2(t, s, sess.SessionID))); err == nil {
		t.Fatal("expected connection_limit after 128 distinct IDs including timed-out")
	} else {
		requireCodeV2(t, err, ErrConnectionLimitV2)
	}
}

func TestStoreV2_TombstoneRequestLifecycle(t *testing.T) {
	s, clock := newTestStoreV2(t)
	mustRegisterNodeV2(t, s, storeClientNodeV2, storeClientRegV2)
	mustRegisterNodeV2(t, s, storeServerNodeV2, storeServerRegV2)

	create := CreateSessionCommandV2{RequestID: mustUUIDV2(t), ServerNodeKey: storeServerNodeV2}
	snap, err := s.AllocateSession(storeClientNodeV2, create)
	if err != nil {
		t.Fatal(err)
	}
	bind := sessionBindCmdV2(t, snap)
	if _, err := s.BindSession(storeClientNodeV2, bind); err != nil {
		t.Fatal(err)
	}
	closeReq := SessionCommandV2{
		RequestID: mustUUIDV2(t),
		Command:   SessionCommandCloseV2,
		IdentityV2: IdentityV2{
			SessionID:    snap.SessionID,
			ConnectionID: "conn-0",
			Epoch:        1,
		},
	}
	if _, err := s.BindSession(storeClientNodeV2, closeReq); err != nil {
		t.Fatal(err)
	}

	replay, err := s.AllocateSession(storeClientNodeV2, create)
	if err != nil {
		t.Fatal(err)
	}
	if replay.SessionID != snap.SessionID {
		t.Fatalf("CREATE during tombstone recreated session %s, want %s", replay.SessionID, snap.SessionID)
	}
	if replay.State != SessionStateClosedV2 {
		t.Fatalf("CREATE during tombstone state %s, want closed", replay.State)
	}

	repeatClose := SessionCommandV2{
		RequestID:  mustUUIDV2(t),
		Command:    SessionCommandCloseV2,
		IdentityV2: closeReq.IdentityV2,
	}
	if _, err := s.BindSession(storeClientNodeV2, repeatClose); err != nil {
		t.Fatalf("repeat CLOSE must be idempotent: %v", err)
	}

	lateOpen, err := s.AllocateConnection(storeClientNodeV2, connCmdV2(t, ConnectionCommandOpenV2, snap.SessionID, "conn-1", 0, snap.Revision))
	if err != nil {
		t.Fatalf("late OPEN during tombstone: %v", err)
	}
	if lateOpen.State != ConnectionStateClosedV2 || lateOpen.SessionID != snap.SessionID {
		t.Fatalf("late OPEN must return closed, not recreate: %+v", lateOpen)
	}

	lateBind := sessionBindCmdV2(t, snap)
	if _, err := s.BindSession(storeClientNodeV2, lateBind); err == nil {
		t.Fatal("late BIND during tombstone must not recreate")
	} else {
		requireCodeV2(t, err, ErrInvalidStateV2)
	}
	if _, err := s.MarkConnectionReady(storeClientNodeV2, connCmdV2(t, ConnectionCommandReadyV2, snap.SessionID, "conn-0", 1, snap.Revision)); err == nil {
		t.Fatal("late READY during tombstone must not recreate")
	} else {
		requireCodeV2(t, err, ErrInvalidStateV2)
	}
	lateResume := SessionCommandV2{
		RequestID:  mustUUIDV2(t),
		Command:    SessionCommandResumeV2,
		IdentityV2: closeReq.IdentityV2,
	}
	if _, err := s.BindSession(storeClientNodeV2, lateResume); err == nil {
		t.Fatal("late RESUME during tombstone must not succeed")
	} else {
		requireCodeV2(t, err, ErrInvalidStateV2)
	}
	if peeked, ok := s.PeekSession(snap.SessionID); !ok || peeked.State != SessionStateClosedV2 {
		t.Fatalf("RESUME must not revive tomb: ok=%v snap=%+v", ok, peeked)
	}
	if _, err := s.BindSession(storeClientNodeV2, bind); err != nil {
		t.Fatal(err)
	}

	msgKey := MessageKeyV2{
		Direction: DirectionKeyV2{GenerationKeyV2: GenerationKeyV2{snap.SessionID, "conn-0", 1}, SenderNodeKey: storeClientNodeV2},
		MessageID: "11111111-1111-4111-8111-111111111111",
	}
	if !s.RememberMessage(msgKey) {
		t.Fatal("first message store")
	}

	clock.Advance(DefaultTombstoneTTLV2 + time.Millisecond)
	_ = s.Expire(clock.Now())

	after, err := s.AllocateSession(storeClientNodeV2, create)
	if err != nil {
		t.Fatal(err)
	}
	if after.SessionID == snap.SessionID {
		t.Fatal("CREATE request_id still bound to dead session after tombstone TTL")
	}
	if after.State != SessionStateAllocatedV2 {
		t.Fatalf("new CREATE after TTL state %s", after.State)
	}
	if _, err := s.BindSession(storeClientNodeV2, bind); err == nil {
		t.Fatal("BIND request tombstone should be collected after TTL")
	} else {
		requireCodeV2(t, err, ErrSessionNotFoundV2)
	}
	if !s.RememberMessage(msgKey) {
		t.Fatal("message key should be recycled after tombstone TTL")
	}
}
