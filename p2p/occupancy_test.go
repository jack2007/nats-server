package p2p

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStoreOccupancyRegisterAndPeerIsolation(t *testing.T) {
	s, _ := newTestStoreV2(t)
	first := mustRegisterNodeV2(t, s, storeClientNodeV2, storeClientRegV2)
	again := mustRegisterNodeV2(t, s, storeClientNodeV2, storeClientRegV2)
	if again.RegistrationEpoch <= first.RegistrationEpoch {
		t.Fatalf("re-register should bump epoch %+v vs %+v", again, first)
	}
	mustRegisterNodeV2(t, s, storeServerNodeV2, storeServerRegV2)
	if _, err := s.AllocateSession(storeClientNodeV2, CreateSessionCommandV2{
		RequestID:     mustUUIDV2(t),
		ServerNodeKey: storeOtherNodeV2,
	}); err == nil {
		t.Fatal("unregistered peer must fail")
	} else {
		requireCodeV2(t, err, ErrPeerNotRegisteredV2)
	}
	snap := mustAllocateSessionV2(t, s, storeClientNodeV2)
	if snap.ClientNodeKey != storeClientNodeV2 || snap.ServerNodeKey != storeServerNodeV2 {
		t.Fatalf("occupancy members %+v", snap)
	}
}

func TestStoreOccupancyDisconnectExpireReleasesNode(t *testing.T) {
	s, clock := newTestStoreV2(t)
	mustRegisterNodeV2(t, s, storeClientNodeV2, storeClientRegV2)
	mustRegisterNodeV2(t, s, storeServerNodeV2, storeServerRegV2)
	mustAllocateSessionV2(t, s, storeClientNodeV2)

	s.MarkNodeDisconnected(storeClientNodeV2)
	clock.Advance(NodeDisconnectGraceV2 - time.Second)
	_ = s.Expire(clock.Now())
	if _, err := s.AllocateSession(storeClientNodeV2, CreateSessionCommandV2{
		RequestID:     mustUUIDV2(t),
		ServerNodeKey: storeServerNodeV2,
	}); err != nil {
		t.Fatalf("grace not elapsed: %v", err)
	}
	s.MarkNodeDisconnected(storeClientNodeV2)
	clock.Advance(NodeDisconnectGraceV2 + time.Millisecond)
	_ = s.Expire(clock.Now())
	if _, err := s.AllocateSession(storeClientNodeV2, CreateSessionCommandV2{
		RequestID:     mustUUIDV2(t),
		ServerNodeKey: storeServerNodeV2,
	}); err == nil {
		t.Fatal("expected not_registered after disconnect grace")
	} else {
		requireCodeV2(t, err, ErrNotRegisteredV2)
	}
}

func TestStoreOccupancySessionCapIs128NotProcessWide(t *testing.T) {
	s, _ := newTestStoreV2(t)
	if s.maxConns != 128 {
		t.Fatalf("per-session cap=%d want 128", s.maxConns)
	}
	mustRegisterNodeV2(t, s, storeClientNodeV2, storeClientRegV2)
	mustRegisterNodeV2(t, s, storeServerNodeV2, storeServerRegV2)
	a := mustAllocateSessionV2(t, s, storeClientNodeV2)
	b := mustAllocateSessionV2(t, s, storeClientNodeV2)
	if a.SessionID == b.SessionID {
		t.Fatal("sessions must not share a process-wide slot")
	}
}

func TestOccupancyPrepareTurnFollowsStoreSession(t *testing.T) {
	dir := t.TempDir()
	secretPath := filepath.Join(dir, "secret")
	secret := []byte("occupancy-turn-secret")
	if err := os.WriteFile(secretPath, secret, 0o600); err != nil {
		t.Fatal(err)
	}
	fixed := time.Unix(1700000000, 0)
	srv, m := startManagerV2(t, Config{
		STUNURLs:      []string{"stun:turn.example.com:3478"},
		TURNURLs:      []string{"turn:turn.example.com:3478?transport=udp"},
		SecretFile:    secretPath,
		CredentialTTL: DefaultCredentialTTL,
	})
	m.now = func() time.Time { return fixed }
	m.v2.store.now = func() time.Time { return fixed }

	client := agentConn(t, srv, mgrClientNodeV2)
	peer := agentConn(t, srv, mgrServerNodeV2)
	clientEv := subscribeEventsV2(t, client, mgrClientNodeV2, mgrClientRegV2)
	peerEv := subscribeEventsV2(t, peer, mgrServerNodeV2, mgrServerRegV2)
	mustRegisterV2(t, client, srv.ID(), mgrClientNodeV2, mgrClientRegV2)
	mustRegisterV2(t, peer, srv.ID(), mgrServerNodeV2, mgrServerRegV2)

	alloc := createAllocatedOnNodeV2(t, client, mgrClientNodeV2, mgrServerNodeV2)
	ident := IdentityV2{SessionID: alloc.SessionID, ConnectionID: alloc.ConnectionID, Epoch: alloc.Epoch}
	if _, ok := m.v2.store.PeekSession(alloc.SessionID); !ok {
		t.Fatal("store must occupy the allocated session")
	}
	bindSubj, err := CommandSubjectV2(mgrClientNodeV2, "SESSION.COMMAND")
	if err != nil {
		t.Fatal(err)
	}
	_ = requestV2(t, client, bindSubj, sessionCmdFrameV2(t, mustUUIDV2(t), SessionCommandBindV2, ident, alloc.Revision))
	cPrep := nextEventV2(t, clientEv, FrameKindPrepareV2)
	sPrep := nextEventV2(t, peerEv, FrameKindPrepareV2)
	user, pass := IssueREST(secret, ident.SessionID, ident.ConnectionID, ident.Epoch, fixed.Unix(), int64(DefaultCredentialTTL/time.Second))
	for _, prep := range []*PrepareEventV2{cPrep.Prepare, sPrep.Prepare} {
		if prep.Turn == nil || prep.Turn.Username != user || prep.Turn.Password != pass {
			t.Fatalf("PREPARE TURN must follow store session occupancy: %+v", prep.Turn)
		}
	}
}
