package p2p

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/nats-io/nats-server/v2/server"
)

func newRegistrationIDV2(t *testing.T) string {
	t.Helper()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b[:])
}

func requestV2Wait(t *testing.T, nc *nats.Conn, subject string, data []byte, d time.Duration) *nats.Msg {
	t.Helper()
	got, err := nc.Request(subject, data, d)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func startClusterTriple(t *testing.T) (sA, sB, sC *server.Server) {
	t.Helper()
	sA, sB = startClusterPair(t)
	ca := sA.ClusterAddr()
	cb := sB.ClusterAddr()
	ra, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", ca.Port))
	if err != nil {
		t.Fatal(err)
	}
	rb, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", cb.Port))
	if err != nil {
		t.Fatal(err)
	}
	optsC := clusterOpts("nC", []*url.URL{ra, rb})
	sC, err = server.NewServer(optsC)
	if err != nil {
		t.Fatal(err)
	}
	sC.Start()
	if !sC.ReadyForConnections(5 * time.Second) {
		t.Fatal("C")
	}
	t.Cleanup(sC.Shutdown)
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if sA.NumRoutes() >= 2 && sB.NumRoutes() >= 2 && sC.NumRoutes() >= 2 {
			return sA, sB, sC
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("triple routes A=%d B=%d C=%d", sA.NumRoutes(), sB.NumRoutes(), sC.NumRoutes())
	return sA, sB, sC
}

func startClusterTripleManagers(t *testing.T) (sA, sB, sC *server.Server, mA, mB, mC *Manager) {
	t.Helper()
	sA, sB, sC = startClusterTriple(t)
	cfg := clusterConfig()
	var err error
	mA, err = StartManager(sA, cfg)
	if err != nil {
		t.Fatal(err)
	}
	mB, err = StartManager(sB, cfg)
	if err != nil {
		t.Fatal(err)
	}
	mC, err = StartManager(sC, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mA.Stop)
	t.Cleanup(mB.Stop)
	t.Cleanup(mC.Stop)
	waitClusterPeersN(t, 3, mA, mB, mC)
	return sA, sB, sC, mA, mB, mC
}

func waitClusterPeersN(t *testing.T, need int, managers ...*Manager) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		ready := true
		for _, m := range managers {
			if len(m.peers.alive(m.now())) < need {
				ready = false
				break
			}
		}
		if ready {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("cluster peers not ready")
}

func waitSessionOwnerV2(t *testing.T, sessionID string, managers ...*Manager) (owner string, rev uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var o string
		var r uint64
		ok := true
		for _, m := range managers {
			gotO, gotR, have := m.SessionOwnerV2(sessionID)
			if !have {
				ok = false
				break
			}
			if o == "" {
				o, r = gotO, gotR
			} else if gotO != o || gotR != r {
				ok = false
				break
			}
		}
		if ok && o != "" {
			return o, r
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("owner/revision not synced")
	return "", 0
}

func sysConnV2(t *testing.T, s *server.Server) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect("", nats.InProcessServer(s), nats.UserInfo("sys", "sys"), nats.Name("sys-test"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func registerOnServerV2(t *testing.T, s *server.Server, node, reg string) *nats.Conn {
	t.Helper()
	nc := agentConnApp(t, s, node)
	t.Cleanup(nc.Close)
	if sid := nc.ConnectedServerId(); sid != s.ID() {
		t.Fatalf("INFO server id %s want %s", sid, s.ID())
	}
	subj, err := EventSubjectV2(node, reg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nc.Subscribe(subj, func(*nats.Msg) {}); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	mustRegisterV2(t, nc, s.ID(), node, reg)
	return nc
}

func createAllocatedV2(t *testing.T, client *nats.Conn, serverNode string) *AllocatedReplyV2 {
	t.Helper()
	if serverNode == "" {
		serverNode = mgrServerNodeV2
	}
	createSubj, err := CommandSubjectV2(mgrClientNodeV2, "SESSION.CREATE")
	if err != nil {
		t.Fatal(err)
	}
	got := requestV2Wait(t, client, createSubj, createFrameV2(t, mustUUIDV2(t), serverNode), 5*time.Second)
	alloc, err := DecodeFrameV2(FrameKindAllocatedV2, got.Data)
	if err != nil {
		t.Fatalf("ALLOCATED: %v body=%s", err, got.Data)
	}
	if alloc.Allocated == nil || !alloc.Allocated.OK || alloc.Allocated.SessionID == "" {
		t.Fatalf("allocated %+v", alloc.Allocated)
	}
	return alloc.Allocated
}

func managerByID(id string, managers ...*Manager) *Manager {
	for _, m := range managers {
		if m.serverID == id {
			return m
		}
	}
	return nil
}

func TestClusterV2_QueueGroupOwnerAndRevisionSync(t *testing.T) {
	sA, sB, sC, mA, mB, mC := startClusterTripleManagers(t)
	managers := []*Manager{mA, mB, mC}

	var createHits atomic.Int32
	var cmdHits atomic.Int32
	v2TestOnCommand = func(suffix string) {
		switch suffix {
		case "SESSION.CREATE":
			createHits.Add(1)
		case "SESSION.COMMAND":
			cmdHits.Add(1)
		}
	}
	t.Cleanup(func() { v2TestOnCommand = nil })

	clientReg := newRegistrationIDV2(t)
	serverReg := newRegistrationIDV2(t)
	client := registerOnServerV2(t, sA, mgrClientNodeV2, clientReg)
	_ = registerOnServerV2(t, sB, mgrServerNodeV2, serverReg)

	fwdCh := make(chan string, 8)
	sys := sysConnV2(t, sA)
	if _, err := sys.Subscribe("$P2P.V2.MGR.>", func(msg *nats.Msg) {
		if strings.HasSuffix(msg.Subject, ".COMMAND") {
			fwdCh <- msg.Subject
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := sys.Flush(); err != nil {
		t.Fatal(err)
	}

	createHits.Store(0)
	alloc := createAllocatedV2(t, client, mgrServerNodeV2)
	if n := createHits.Load(); n != 1 {
		t.Fatalf("CREATE entries=%d want 1", n)
	}
	if alloc.OwnerServerID == "" {
		t.Fatal("missing owner_server_id")
	}
	owner, rev := waitSessionOwnerV2(t, alloc.SessionID, managers...)
	if owner != alloc.OwnerServerID || rev != alloc.Revision {
		t.Fatalf("synced owner=%s rev=%d allocated owner=%s rev=%d", owner, rev, alloc.OwnerServerID, alloc.Revision)
	}

	ownerMgr := managerByID(owner, managers...)
	if ownerMgr == nil {
		t.Fatal("owner manager missing")
	}
	ownerMgr.dropQueueGroupV2()

	ident := IdentityV2{SessionID: alloc.SessionID, ConnectionID: alloc.ConnectionID, Epoch: alloc.Epoch}
	bindSubj, err := CommandSubjectV2(mgrClientNodeV2, "SESSION.COMMAND")
	if err != nil {
		t.Fatal(err)
	}
	cmdHits.Store(0)
	got := requestV2Wait(t, client, bindSubj, sessionCmdFrameV2(t, mustUUIDV2(t), SessionCommandBindV2, ident, alloc.Revision), 5*time.Second)
	if _, err := DecodeFrameV2(FrameKindErrorV2, got.Data); err == nil {
		t.Fatalf("BIND forwarded as error: %s", got.Data)
	}
	if !bytesContainsOK(got.Data) {
		t.Fatalf("BIND: %s", got.Data)
	}
	wantFwd := v2MgrCommandSubject(owner)
	select {
	case subj := <-fwdCh:
		if subj != wantFwd {
			t.Fatalf("forward subject %s want %s", subj, wantFwd)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("missing forward to owner COMMAND")
	}
	_, rev2 := waitSessionOwnerV2(t, alloc.SessionID, managers...)
	if rev2 <= rev {
		t.Fatalf("revision did not advance: %d -> %d", rev, rev2)
	}
	_ = sC
}

func TestClusterV2_ConnectionRejectReplicaRevisionSync(t *testing.T) {
	sA, sB, _, mA, mB, mC := startClusterTripleManagers(t)
	managers := []*Manager{mA, mB, mC}
	clientReg := newRegistrationIDV2(t)
	serverReg := newRegistrationIDV2(t)
	client := registerOnServerV2(t, sA, mgrClientNodeV2, clientReg)
	srv := registerOnServerV2(t, sB, mgrServerNodeV2, serverReg)
	clientEv := subscribeEventsV2(t, client, mgrClientNodeV2, clientReg)
	serverEv := subscribeEventsV2(t, srv, mgrServerNodeV2, serverReg)

	alloc := createAllocatedV2(t, client, mgrServerNodeV2)
	connSubj, err := CommandSubjectV2(mgrClientNodeV2, "CONNECTION.COMMAND")
	if err != nil {
		t.Fatal(err)
	}
	open := requestV2Wait(t, client, connSubj, connCmdFrameV2(t, mustUUIDV2(t), ConnectionCommandOpenV2, IdentityV2{
		SessionID:    alloc.SessionID,
		ConnectionID: "conn-1",
	}, 0), 5*time.Second)
	connAlloc, err := DecodeFrameV2(FrameKindAllocatedV2, open.Data)
	if err != nil {
		t.Fatalf("OPEN conn-1: %v body=%s", err, open.Data)
	}
	_, before := waitSessionOwnerV2(t, alloc.SessionID, managers...)
	ident := IdentityV2{SessionID: connAlloc.Allocated.SessionID, ConnectionID: connAlloc.Allocated.ConnectionID, Epoch: connAlloc.Allocated.Epoch}
	reqID := mustUUIDV2(t)
	reply := requestV2Wait(t, client, connSubj, connCmdFrameV2(t, reqID, ConnectionCommandRejectV2, ident, connAlloc.Allocated.Revision), 5*time.Second)
	if !bytesContainsOK(reply.Data) {
		t.Fatalf("REJECT reply must be OK: %s", reply.Data)
	}
	cClose := nextEventV2(t, clientEv, FrameKindCloseV2)
	sClose := nextEventV2(t, serverEv, FrameKindCloseV2)
	if cClose.Close.Revision != before+1 || sClose.Close.Revision != cClose.Close.Revision {
		t.Fatalf("CLOSE revision client=%d server=%d before=%d", cClose.Close.Revision, sClose.Close.Revision, before)
	}
	owner, rev := waitSessionOwnerV2(t, alloc.SessionID, managers...)
	if rev != cClose.Close.Revision {
		t.Fatalf("synced revision=%d CLOSE revision=%d", rev, cClose.Close.Revision)
	}
	if managerByID(owner, managers...) == nil {
		t.Fatalf("owner %s missing", owner)
	}
	for _, m := range managers {
		snap, ok := m.v2.store.PeekSession(alloc.SessionID)
		if !ok || snap.Revision != rev {
			t.Fatalf("manager %s replica revision ok=%v snapshot=%+v want=%d", m.serverID, ok, snap, rev)
		}
		m.v2.store.mu.Lock()
		conn := m.v2.store.sessions[alloc.SessionID].connections[ident.ConnectionID]
		var state ConnectionStateV2
		if conn != nil {
			state = conn.state
		}
		m.v2.store.mu.Unlock()
		if state != ConnectionStateClosedV2 {
			t.Fatalf("manager %s replica connection state=%s want closed", m.serverID, state)
		}
	}

	repeat := requestV2Wait(t, client, connSubj, connCmdFrameV2(t, reqID, ConnectionCommandRejectV2, ident, alloc.Revision), 5*time.Second)
	if !bytesContainsOK(repeat.Data) {
		t.Fatalf("duplicate REJECT reply must be OK: %s", repeat.Data)
	}
	assertNoEventV2(t, clientEv)
	assertNoEventV2(t, serverEv)
}

func TestClusterV2_ConnectionRejectSyncFailureRepliesBusy(t *testing.T) {
	sA, sB, _, mA, mB, mC := startClusterTripleManagers(t)
	managers := []*Manager{mA, mB, mC}
	clientReg := newRegistrationIDV2(t)
	serverReg := newRegistrationIDV2(t)
	client := registerOnServerV2(t, sA, mgrClientNodeV2, clientReg)
	srv := registerOnServerV2(t, sB, mgrServerNodeV2, serverReg)
	clientEv := subscribeEventsV2(t, client, mgrClientNodeV2, clientReg)
	serverEv := subscribeEventsV2(t, srv, mgrServerNodeV2, serverReg)

	alloc := createAllocatedV2(t, client, mgrServerNodeV2)
	_, _ = waitSessionOwnerV2(t, alloc.SessionID, managers...)
	v2TestFailSessionSync.Store(true)
	t.Cleanup(func() { v2TestFailSessionSync.Store(false) })

	ident := IdentityV2{SessionID: alloc.SessionID, ConnectionID: alloc.ConnectionID, Epoch: alloc.Epoch}
	connSubj, err := CommandSubjectV2(mgrClientNodeV2, "CONNECTION.COMMAND")
	if err != nil {
		t.Fatal(err)
	}
	reqID := mustUUIDV2(t)
	busy := requestV2Wait(t, client, connSubj, connCmdFrameV2(t, reqID, ConnectionCommandRejectV2, ident, alloc.Revision), 5*time.Second)
	perr := mustErrorV2(t, busy.Data)
	if perr.Code != ErrBusyV2 || perr.RetryAfterMs == nil || *perr.RetryAfterMs <= 0 {
		t.Fatalf("REJECT sync timeout want busy+retry: %+v body=%s", perr, busy.Data)
	}
	_ = nextEventV2(t, clientEv, FrameKindCloseV2)
	_ = nextEventV2(t, serverEv, FrameKindCloseV2)
}

func TestClusterV2_ConnectionRejectLateJoinKeepsAllConnections(t *testing.T) {
	sA, sB, sC := startClusterTriple(t)
	cfg := clusterConfig()
	mA, err := StartManager(sA, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mA.Stop)
	mB, err := StartManager(sB, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mB.Stop)
	waitClusterPeersN(t, 2, mA, mB)

	client := registerOnServerV2(t, sA, mgrClientNodeV2, newRegistrationIDV2(t))
	_ = registerOnServerV2(t, sB, mgrServerNodeV2, newRegistrationIDV2(t))
	alloc := createAllocatedV2(t, client, mgrServerNodeV2)
	connSubj, err := CommandSubjectV2(mgrClientNodeV2, "CONNECTION.COMMAND")
	if err != nil {
		t.Fatal(err)
	}
	open := requestV2Wait(t, client, connSubj, connCmdFrameV2(t, mustUUIDV2(t), ConnectionCommandOpenV2, IdentityV2{
		SessionID:    alloc.SessionID,
		ConnectionID: "conn-1",
	}, 0), 5*time.Second)
	connAlloc, err := DecodeFrameV2(FrameKindAllocatedV2, open.Data)
	if err != nil {
		t.Fatalf("OPEN conn-1: %v body=%s", err, open.Data)
	}
	ident := IdentityV2{SessionID: connAlloc.Allocated.SessionID, ConnectionID: connAlloc.Allocated.ConnectionID, Epoch: connAlloc.Allocated.Epoch}
	reject := requestV2Wait(t, client, connSubj, connCmdFrameV2(t, mustUUIDV2(t), ConnectionCommandRejectV2, ident, connAlloc.Allocated.Revision), 5*time.Second)
	if !bytesContainsOK(reject.Data) {
		t.Fatalf("REJECT conn-1: %s", reject.Data)
	}
	_, revision := waitSessionOwnerV2(t, alloc.SessionID, mA, mB)

	mC, err := StartManager(sC, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mC.Stop)
	waitClusterPeersN(t, 3, mA, mB, mC)
	_, gotRevision := waitSessionOwnerV2(t, alloc.SessionID, mA, mB, mC)
	if gotRevision != revision {
		t.Fatalf("late join revision=%d want %d", gotRevision, revision)
	}
	conn0, ok := mC.v2.store.PeekConnection(alloc.SessionID, "conn-0")
	if !ok || conn0.State != ConnectionStateAllocatedV2 || conn0.Revision != revision {
		t.Fatalf("late join conn-0 ok=%v snapshot=%+v want allocated rev=%d", ok, conn0, revision)
	}
	conn1, ok := mC.v2.store.PeekConnection(alloc.SessionID, "conn-1")
	if !ok || conn1.State != ConnectionStateClosedV2 || conn1.Revision != revision {
		t.Fatalf("late join conn-1 ok=%v snapshot=%+v want closed rev=%d", ok, conn1, revision)
	}
}

func bytesContainsOK(data []byte) bool {
	return strings.Contains(string(data), `"ok":true`)
}

func TestClusterV2_CreateReplyBarrier(t *testing.T) {
	sA, sB, _, mA, mB, mC := startClusterTripleManagers(t)
	clientReg := newRegistrationIDV2(t)
	serverReg := newRegistrationIDV2(t)
	client := registerOnServerV2(t, sA, mgrClientNodeV2, clientReg)
	_ = registerOnServerV2(t, sB, mgrServerNodeV2, serverReg)

	for _, m := range []*Manager{mA, mB, mC} {
		m.peers.mu.Lock()
		m.peers.lastBeat["ghost-peer"] = m.now()
		m.peers.mu.Unlock()
	}

	createSubj, err := CommandSubjectV2(mgrClientNodeV2, "SESSION.CREATE")
	if err != nil {
		t.Fatal(err)
	}
	reqID := mustUUIDV2(t)
	got := requestV2Wait(t, client, createSubj, createFrameV2(t, reqID, mgrServerNodeV2), 5*time.Second)
	perr := mustErrorV2(t, got.Data)
	if perr.Code != ErrBusyV2 || perr.RetryAfterMs == nil || *perr.RetryAfterMs <= 0 {
		t.Fatalf("sync timeout want busy+retry: %+v body=%s", perr, got.Data)
	}

	for _, m := range []*Manager{mA, mB, mC} {
		m.peers.mu.Lock()
		delete(m.peers.lastBeat, "ghost-peer")
		m.peers.mu.Unlock()
	}

	retry := requestV2Wait(t, client, createSubj, createFrameV2(t, reqID, mgrServerNodeV2), 5*time.Second)
	alloc, err := DecodeFrameV2(FrameKindAllocatedV2, retry.Data)
	if err != nil {
		t.Fatalf("retry ALLOCATED: %v body=%s", err, retry.Data)
	}
	firstID := alloc.Allocated.SessionID
	retry2 := requestV2Wait(t, client, createSubj, createFrameV2(t, reqID, mgrServerNodeV2), 5*time.Second)
	alloc2, err := DecodeFrameV2(FrameKindAllocatedV2, retry2.Data)
	if err != nil {
		t.Fatal(err)
	}
	if alloc2.Allocated.SessionID != firstID {
		t.Fatalf("retry rebound session %s vs %s", alloc2.Allocated.SessionID, firstID)
	}

	hold := make(chan struct{})
	v2TestHoldJoinQueue = hold
	t.Cleanup(func() { v2TestHoldJoinQueue = nil })
	cfg := clusterConfig()
	sA2, sB2 := startClusterPair(t)
	mHold, err := StartManager(sA2, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mHold.Stop)
	_ = sB2
	if mHold.joinedQueueGroupV2() {
		t.Fatal("incomplete catch-up coordinator joined external queue")
	}
	close(hold)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if mHold.joinedQueueGroupV2() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("catch-up coordinator never joined queue")
}

func TestClusterV2_PartitionNoDualMaster(t *testing.T) {
	sA, sB, _, mA, mB, mC := startClusterTripleManagers(t)
	managers := []*Manager{mA, mB, mC}
	client := registerOnServerV2(t, sA, mgrClientNodeV2, newRegistrationIDV2(t))
	_ = registerOnServerV2(t, sB, mgrServerNodeV2, newRegistrationIDV2(t))
	alloc := createAllocatedV2(t, client, mgrServerNodeV2)
	owner, rev := waitSessionOwnerV2(t, alloc.SessionID, managers...)
	ownerMgr := managerByID(owner, managers...)
	ownerMgr.dropQueueGroupV2()
	ownerMgr.setOwnerUnreachableV2(true)

	ident := IdentityV2{SessionID: alloc.SessionID, ConnectionID: alloc.ConnectionID, Epoch: alloc.Epoch}
	bindSubj, _ := CommandSubjectV2(mgrClientNodeV2, "SESSION.COMMAND")
	got := requestV2Wait(t, client, bindSubj, sessionCmdFrameV2(t, mustUUIDV2(t), SessionCommandBindV2, ident, alloc.Revision), 5*time.Second)
	if mustErrorV2(t, got.Data).Code != ErrCoordinatorUnavailableV2 {
		t.Fatalf("partition BIND: %s", got.Data)
	}
	for _, m := range managers {
		o, r, ok := m.SessionOwnerV2(alloc.SessionID)
		if !ok || o != owner {
			t.Fatalf("dual master on %s: owner=%s ok=%v want %s", m.serverID, o, ok, owner)
		}
		if r != rev {
			t.Fatalf("revision advanced on %s: %d want %d", m.serverID, r, rev)
		}
	}
	if snap, ok := ownerMgr.v2.store.PeekSession(alloc.SessionID); !ok || snap.Revision != rev {
		t.Fatalf("owner store advanced: ok=%v snap=%+v", ok, snap)
	}
}

func TestClusterV2_OwnerExitNegotiatingAndActive(t *testing.T) {
	sA, sB, _, mA, mB, mC := startClusterTripleManagers(t)
	clientReg := newRegistrationIDV2(t)
	serverReg := newRegistrationIDV2(t)
	client := registerOnServerV2(t, sA, mgrClientNodeV2, clientReg)
	srv := registerOnServerV2(t, sB, mgrServerNodeV2, serverReg)
	clientEv := subscribeEventsV2(t, client, mgrClientNodeV2, clientReg)
	serverEv := subscribeEventsV2(t, srv, mgrServerNodeV2, serverReg)

	alloc := createAllocatedV2(t, client, mgrServerNodeV2)
	owner, _ := waitSessionOwnerV2(t, alloc.SessionID, mA, mB, mC)
	ownerMgr := managerByID(owner, mA, mB, mC)
	ownerMgr.Stop()

	cEv := waitRebuildEventV2(t, clientEv)
	sEv := waitRebuildEventV2(t, serverEv)
	cDec, err := DecodeFrameV2(FrameKindErrorV2, cEv)
	if err != nil || cDec.Error == nil || cDec.Error.Code == "" {
		t.Fatalf("owner-loss client EVENT DecodeFrameV2: %v body=%s", err, cEv)
	}
	sDec, err := DecodeFrameV2(FrameKindErrorV2, sEv)
	if err != nil || sDec.Error == nil || sDec.Error.Code == "" {
		t.Fatalf("owner-loss server EVENT DecodeFrameV2: %v body=%s", err, sEv)
	}
	select {
	case extra := <-clientEv:
		if dec, err := DecodeFrameV2(FrameKindCloseV2, extra.Data); err == nil && dec != nil {
			t.Fatalf("unexpected CLOSE after negotiating owner loss: %s", extra.Data)
		}
	case extra := <-serverEv:
		if dec, err := DecodeFrameV2(FrameKindCloseV2, extra.Data); err == nil && dec != nil {
			t.Fatalf("unexpected CLOSE after negotiating owner loss: %s", extra.Data)
		}
	case <-time.After(80 * time.Millisecond):
	}

	survivors := survivingManagers(owner, mA, mB, mC)
	if len(survivors) < 2 {
		t.Fatal("need two surviving coordinators")
	}
	waitDeadOwnerDroppedV2(t, owner, survivors...)
	client2Reg := newRegistrationIDV2(t)
	server2Reg := newRegistrationIDV2(t)
	client2 := registerOnServerV2(t, survivors[0].s, "client-z", client2Reg)
	srv2 := registerOnServerV2(t, survivors[1].s, "server-z", server2Reg)
	client2Ev := subscribeEventsV2(t, client2, "client-z", client2Reg)
	srv2Ev := subscribeEventsV2(t, srv2, "server-z", server2Reg)
	createSubj, _ := CommandSubjectV2("client-z", "SESSION.CREATE")
	got := requestV2Wait(t, client2, createSubj, createFrameV2(t, mustUUIDV2(t), "server-z"), 5*time.Second)
	alloc2, err := DecodeFrameV2(FrameKindAllocatedV2, got.Data)
	if err != nil {
		t.Fatalf("second CREATE: %v body=%s", err, got.Data)
	}
	ident := IdentityV2{SessionID: alloc2.Allocated.SessionID, ConnectionID: alloc2.Allocated.ConnectionID, Epoch: alloc2.Allocated.Epoch}
	bindSubj, _ := CommandSubjectV2("client-z", "SESSION.COMMAND")
	_ = requestV2Wait(t, client2, bindSubj, sessionCmdFrameV2(t, mustUUIDV2(t), SessionCommandBindV2, ident, alloc2.Allocated.Revision), 5*time.Second)
	cPrep := nextEventV2(t, client2Ev, FrameKindPrepareV2)
	sPrep := nextEventV2(t, srv2Ev, FrameKindPrepareV2)
	readySubj, _ := CommandSubjectV2("client-z", "CONNECTION.COMMAND")
	srvReady, _ := CommandSubjectV2("server-z", "CONNECTION.COMMAND")
	_ = requestV2Wait(t, client2, readySubj, connCmdFrameV2(t, mustUUIDV2(t), ConnectionCommandReadyV2, ident, cPrep.Prepare.Revision), 5*time.Second)
	_ = requestV2Wait(t, srv2, srvReady, connCmdFrameV2(t, mustUUIDV2(t), ConnectionCommandReadyV2, ident, sPrep.Prepare.Revision), 5*time.Second)
	_ = nextEventV2(t, client2Ev, FrameKindStartV2)
	_ = nextEventV2(t, srv2Ev, FrameKindStartV2)

	owner2, _ := waitSessionOwnerV2(t, alloc2.Allocated.SessionID, survivors...)
	activeOwner := managerByID(owner2, survivors...)
	activeOwner.Stop()
	select {
	case msg := <-client2Ev:
		if _, err := DecodeFrameV2(FrameKindCloseV2, msg.Data); err == nil {
			t.Fatalf("active QUIC must not get data-plane CLOSE: %s", msg.Data)
		}
	case msg := <-srv2Ev:
		if _, err := DecodeFrameV2(FrameKindCloseV2, msg.Data); err == nil {
			t.Fatalf("active QUIC must not get data-plane CLOSE: %s", msg.Data)
		}
	case <-time.After(200 * time.Millisecond):
	}
}

func waitRebuildEventV2(t *testing.T, ch <-chan *nats.Msg) []byte {
	t.Helper()
	select {
	case msg := <-ch:
		if strings.Contains(string(msg.Data), `"type":"invite"`) {
			t.Fatalf("unexpected invite: %s", msg.Data)
		}
		if _, err := DecodeFrameV2(FrameKindCloseV2, msg.Data); err == nil {
			t.Fatalf("unexpected CLOSE: %s", msg.Data)
		}
		return msg.Data
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting rebuild/error event")
		return nil
	}
}

func waitDeadOwnerDroppedV2(t *testing.T, dead string, managers ...*Manager) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, m := range managers {
			for _, id := range m.peers.alive(m.now()) {
				if id == dead {
					ok = false
					break
				}
			}
		}
		if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("dead owner still in peer set")
}

func survivingManagers(dead string, managers ...*Manager) []*Manager {
	var out []*Manager
	for _, m := range managers {
		if m.serverID != dead && m.v2 != nil {
			out = append(out, m)
		}
	}
	return out
}

func mustCurrentRegV2(t *testing.T, m *Manager, node string) string {
	t.Helper()
	if m == nil || m.v2 == nil {
		t.Fatal("manager gone")
	}
	m.v2.mu.Lock()
	defer m.v2.mu.Unlock()
	if b, ok := m.v2.nodes[node]; ok {
		return b.registrationID
	}
	return newRegistrationIDV2(t)
}

func TestClusterV2_ResumeAfterReconnect(t *testing.T) {
	sA, sB, _, mA, mB, mC := startClusterTripleManagers(t)
	clientReg := newRegistrationIDV2(t)
	serverReg := newRegistrationIDV2(t)
	client := registerOnServerV2(t, sA, mgrClientNodeV2, clientReg)
	_ = registerOnServerV2(t, sB, mgrServerNodeV2, serverReg)
	alloc := createAllocatedV2(t, client, mgrServerNodeV2)
	waitSessionOwnerV2(t, alloc.SessionID, mA, mB, mC)

	client.Close()
	newReg := newRegistrationIDV2(t)
	client2 := registerOnServerV2(t, sA, mgrClientNodeV2, newReg)
	ident := IdentityV2{SessionID: alloc.SessionID, ConnectionID: alloc.ConnectionID, Epoch: alloc.Epoch}
	resumeSubj, _ := CommandSubjectV2(mgrClientNodeV2, "SESSION.COMMAND")
	got := requestV2Wait(t, client2, resumeSubj, sessionCmdFrameV2(t, mustUUIDV2(t), SessionCommandResumeV2, ident, alloc.Revision), 5*time.Second)
	if !bytesContainsOK(got.Data) {
		t.Fatalf("RESUME while owner lives: %s", got.Data)
	}

	owner, _ := waitSessionOwnerV2(t, alloc.SessionID, mA, mB, mC)
	managerByID(owner, mA, mB, mC).forgetSessionOwnerV2(alloc.SessionID)
	lost := requestV2Wait(t, client2, resumeSubj, sessionCmdFrameV2(t, mustUUIDV2(t), SessionCommandResumeV2, ident, alloc.Revision), 5*time.Second)
	if mustErrorV2(t, lost.Data).Code != ErrSessionNotFoundV2 {
		t.Fatalf("lost owner RESUME: %s", lost.Data)
	}
}

func TestClusterV2_RegistrationGeneration(t *testing.T) {
	sA, sB, _, mA, mB, _ := startClusterTripleManagers(t)
	node := "reg-node"
	reg := newRegistrationIDV2(t)
	if len(reg) != 32 || strings.ToLower(reg) != reg {
		t.Fatalf("registration id must be 32 lowercase hex: %s", reg)
	}
	nc := agentConnApp(t, sA, node)
	t.Cleanup(nc.Close)
	if nc.ConnectedServerId() != sA.ID() {
		t.Fatalf("agent INFO server %s want %s", nc.ConnectedServerId(), sA.ID())
	}
	evCh := subscribeEventsV2(t, nc, node, reg)
	mustRegisterV2(t, nc, sA.ID(), node, reg)
	reqID := mustUUIDV2(t)
	subj, _ := RegisterSubjectV2(node, sA.ID())
	again := requestV2Wait(t, nc, subj, registerFrameV2(t, reqID, reg), 5*time.Second)
	dec, err := DecodeFrameV2(FrameKindRegisterReplyV2, again.Data)
	if err != nil {
		t.Fatalf("idempotent register: %v", err)
	}
	firstEpoch := dec.RegisterReply.RegistrationEpoch
	again2 := requestV2Wait(t, nc, subj, registerFrameV2(t, reqID, reg), 5*time.Second)
	dec2, _ := DecodeFrameV2(FrameKindRegisterReplyV2, again2.Data)
	if dec2.RegisterReply.RegistrationEpoch != firstEpoch || dec2.RegisterReply.RegistrationID != reg {
		t.Fatalf("idempotent epoch/id changed %+v vs %+v", dec2.RegisterReply, dec.RegisterReply)
	}

	busyNC := agentConnApp(t, sA, node)
	t.Cleanup(busyNC.Close)
	busySubj, _ := RegisterSubjectV2(node, sA.ID())
	busy := requestV2Wait(t, busyNC, busySubj, registerFrameV2(t, mustUUIDV2(t), newRegistrationIDV2(t)), 5*time.Second)
	if mustErrorV2(t, busy.Data).Code != ErrBusyV2 {
		t.Fatalf("two conns same name want busy: %s", busy.Data)
	}

	ghost := agentConnApp(t, sA, "ghost-name")
	t.Cleanup(ghost.Close)
	zeroSubj, _ := RegisterSubjectV2("missing-node", sA.ID())
	zero := requestV2Wait(t, ghost, zeroSubj, registerFrameV2(t, mustUUIDV2(t), newRegistrationIDV2(t)), 5*time.Second)
	if mustErrorV2(t, zero.Data).Code != ErrBusyV2 {
		t.Fatalf("0 connz want busy: %s", zero.Data)
	}

	busyNC.Close()
	nc.Close()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		cids, err := mA.lookupNamedAgentConns(node)
		if err == nil && len(cids) == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	moved := agentConnApp(t, sA, node)
	t.Cleanup(moved.Close)
	moveSubj, _ := EventSubjectV2(node, reg)
	if _, err := moved.Subscribe(moveSubj, func(*nats.Msg) {}); err != nil {
		t.Fatal(err)
	}
	_ = moved.Flush()
	moveReg, _ := RegisterSubjectV2(node, sA.ID())
	movedReply := requestV2Wait(t, moved, moveReg, registerFrameV2(t, mustUUIDV2(t), reg), 5*time.Second)
	if mustErrorV2(t, movedReply.Data).Code != ErrInvalidRequestV2 {
		t.Fatalf("registration id moved CID: %s", movedReply.Data)
	}

	freshReg := newRegistrationIDV2(t)
	freshEv := subscribeEventsV2(t, moved, node, freshReg)
	mustRegisterV2(t, moved, sA.ID(), node, freshReg)
	_ = registerOnServerV2(t, sB, mgrServerNodeV2, newRegistrationIDV2(t))
	createSubj, _ := CommandSubjectV2(node, "SESSION.CREATE")
	got := requestV2Wait(t, moved, createSubj, createFrameV2(t, mustUUIDV2(t), mgrServerNodeV2), 5*time.Second)
	alloc, err := DecodeFrameV2(FrameKindAllocatedV2, got.Data)
	if err != nil {
		t.Fatal(err)
	}
	ident := IdentityV2{SessionID: alloc.Allocated.SessionID, ConnectionID: alloc.Allocated.ConnectionID, Epoch: alloc.Allocated.Epoch}
	bindSubj, _ := CommandSubjectV2(node, "SESSION.COMMAND")
	_ = requestV2Wait(t, moved, bindSubj, sessionCmdFrameV2(t, mustUUIDV2(t), SessionCommandBindV2, ident, alloc.Allocated.Revision), 5*time.Second)
	ev := nextEventV2(t, freshEv, FrameKindPrepareV2)
	if ev.Envelope.RegistrationID != freshReg {
		t.Fatalf("EVENT registration %s want %s", ev.Envelope.RegistrationID, freshReg)
	}
	select {
	case late := <-evCh:
		t.Fatalf("old generation received event: %s", late.Data)
	case <-time.After(50 * time.Millisecond):
	}

	mA.injectV2Disconnect(node, 1)
	if _, rev, ok := mA.SessionOwnerV2(alloc.Allocated.SessionID); !ok || rev == 0 {
		t.Fatal("late disconnect overwrote session")
	}
	_ = mB
}

func TestClusterV2_RegisterSyncBarrier(t *testing.T) {
	hold := make(chan struct{})
	v2TestHoldRegisterSync = hold
	v2TestHoldRegisterNode = mgrClientNodeV2
	t.Cleanup(func() {
		select {
		case <-hold:
		default:
			close(hold)
		}
		v2TestHoldRegisterSync = nil
		v2TestHoldRegisterNode = ""
	})
	sA, sB, _, mA, mB, mC := startClusterTripleManagers(t)

	clientReg := newRegistrationIDV2(t)
	nc := agentConnApp(t, sA, mgrClientNodeV2)
	t.Cleanup(nc.Close)
	evSubj, _ := EventSubjectV2(mgrClientNodeV2, clientReg)
	if _, err := nc.Subscribe(evSubj, func(*nats.Msg) {}); err != nil {
		t.Fatal(err)
	}
	_ = nc.Flush()
	regSubj, _ := RegisterSubjectV2(mgrClientNodeV2, sA.ID())
	done := make(chan *nats.Msg, 1)
	go func() {
		got, err := nc.Request(regSubj, registerFrameV2(t, mustUUIDV2(t), clientReg), 8*time.Second)
		if err != nil {
			done <- &nats.Msg{Data: []byte(err.Error())}
			return
		}
		done <- got
	}()
	time.Sleep(50 * time.Millisecond)
	_ = registerOnServerV2(t, sB, mgrServerNodeV2, newRegistrationIDV2(t))
	createSubj, _ := CommandSubjectV2(mgrClientNodeV2, "SESSION.CREATE")
	busy := requestV2Wait(t, nc, createSubj, createFrameV2(t, mustUUIDV2(t), mgrServerNodeV2), 5*time.Second)
	perr := mustErrorV2(t, busy.Data)
	if perr.Code != ErrBusyV2 || perr.RetryAfterMs == nil {
		t.Fatalf("command during REGISTER sync: %+v %s", perr, busy.Data)
	}
	close(hold)
	select {
	case got := <-done:
		if _, err := DecodeFrameV2(FrameKindRegisterReplyV2, got.Data); err != nil {
			t.Fatalf("REGISTER after barrier: %v body=%s", err, got.Data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("REGISTER did not complete after sync")
	}
	_ = mA
	_ = mB
	_ = mC
}

func TestClusterV2_LostCreateReply(t *testing.T) {
	sA, sB, _, mA, mB, mC := startClusterTripleManagers(t)
	clientReg := newRegistrationIDV2(t)
	serverReg := newRegistrationIDV2(t)
	client := registerOnServerV2(t, sA, mgrClientNodeV2, clientReg)
	srv := registerOnServerV2(t, sB, mgrServerNodeV2, serverReg)
	clientEv := subscribeEventsV2(t, client, mgrClientNodeV2, clientReg)
	serverEv := subscribeEventsV2(t, srv, mgrServerNodeV2, serverReg)

	createSubj, _ := CommandSubjectV2(mgrClientNodeV2, "SESSION.CREATE")
	lostReq := mustUUIDV2(t)
	got := requestV2Wait(t, client, createSubj, createFrameV2Timeout(t, lostReq, mgrServerNodeV2, 80), 5*time.Second)
	alloc, err := DecodeFrameV2(FrameKindAllocatedV2, got.Data)
	if err != nil {
		t.Fatal(err)
	}
	oldSession := alloc.Allocated.SessionID
	owner := managerByID(alloc.Allocated.OwnerServerID, mA, mB, mC)

	freshReq := mustUUIDV2(t)
	got2 := requestV2Wait(t, client, createSubj, createFrameV2(t, freshReq, mgrServerNodeV2), 5*time.Second)
	alloc2, err := DecodeFrameV2(FrameKindAllocatedV2, got2.Data)
	if err != nil {
		t.Fatal(err)
	}
	if alloc2.Allocated.SessionID == oldSession {
		t.Fatal("new request must allocate a new session")
	}
	deadlineEv := time.Now().Add(80 * time.Millisecond)
	for time.Now().Before(deadlineEv) {
		remaining := time.Until(deadlineEv)
		if remaining <= 0 {
			break
		}
		select {
		case msg := <-clientEv:
			if _, err := DecodeFrameV2(FrameKindPrepareV2, msg.Data); err == nil {
				t.Fatalf("unbound session emitted PREPARE: %s", msg.Data)
			}
		case msg := <-serverEv:
			if _, err := DecodeFrameV2(FrameKindPrepareV2, msg.Data); err == nil {
				t.Fatalf("unbound session PREPARE to server: %s", msg.Data)
			}
		case <-time.After(remaining):
		}
	}

	snap, ok := owner.v2.store.PeekSession(oldSession)
	if ok && snap.State != SessionStateFailedV2 && snap.State != SessionStateClosedV2 {
		if ev := owner.v2.store.Expire(owner.now().Add(time.Second)); len(ev) == 0 {
			t.Fatal("unbound session must expire at deadline")
		}
	}

	same := requestV2Wait(t, client, createSubj, createFrameV2(t, freshReq, mgrServerNodeV2), 5*time.Second)
	again, err := DecodeFrameV2(FrameKindAllocatedV2, same.Data)
	if err != nil {
		t.Fatal(err)
	}
	if again.Allocated.SessionID != alloc2.Allocated.SessionID || again.Allocated.OwnerServerID != alloc2.Allocated.OwnerServerID {
		t.Fatalf("ALLOCATED request must RESUME same owner, got %+v", again.Allocated)
	}
}

func TestClusterV2_IncompleteCatchUpDoesNotJoinQueue(t *testing.T) {
	sA, sB, sC := startClusterTriple(t)
	cfg := clusterConfig()
	mA, err := StartManager(sA, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mA.Stop)
	mB, err := StartManager(sB, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mB.Stop)
	waitClusterPeersN(t, 2, mA, mB)

	v2TestCatchUpExtraNeed = 8
	t.Cleanup(func() { v2TestCatchUpExtraNeed = 0 })

	mHold, err := StartManager(sC, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mHold.Stop)
	if mHold.joinedQueueGroupV2() {
		t.Fatal("incomplete catch-up must not join $P2P.V2.COORDINATORS")
	}
	reg := newRegistrationIDV2(t)
	nc := agentConnApp(t, sC, "catchup-self")
	t.Cleanup(nc.Close)
	mustRegisterV2(t, nc, sC.ID(), "catchup-self", reg)
	if mHold.joinedQueueGroupV2() {
		t.Fatal("REGISTER-to-self must not join external queue after incomplete catch-up")
	}
}

func TestClusterV2_CatchUpRetriesUntilComplete(t *testing.T) {
	sA, sB := startClusterPair(t)
	cfg := clusterConfig()
	mA, err := StartManager(sA, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mA.Stop)

	var attempts atomic.Int32
	thirdAttempt := make(chan struct{})
	v2TestCatchUpTimeout = 10 * time.Millisecond
	v2TestCatchUpRetryDelay = time.Millisecond
	v2TestBeforeCatchUp = func(m *Manager) {
		if m.serverID != sB.ID() {
			return
		}
		n := attempts.Add(1)
		if n < 3 {
			v2TestCatchUpExtraNeed = 8
			return
		}
		v2TestCatchUpExtraNeed = 0
		select {
		case <-thirdAttempt:
		default:
			close(thirdAttempt)
		}
	}
	t.Cleanup(func() {
		v2TestBeforeCatchUp = nil
		v2TestCatchUpExtraNeed = 0
		v2TestCatchUpTimeout = 0
		v2TestCatchUpRetryDelay = 0
	})

	mB, err := StartManager(sB, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mB.Stop)
	if mB.joinedQueueGroupV2() {
		t.Fatal("coordinator joined external queue before catch-up completed")
	}
	var wakeWG sync.WaitGroup
	for i := 0; i < 32; i++ {
		wakeWG.Add(1)
		go func() {
			defer wakeWG.Done()
			mB.wakeCatchUpV2()
		}()
	}
	wakeWG.Wait()

	select {
	case <-thirdAttempt:
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("catch-up stopped retrying after %d attempts", attempts.Load())
	}
	select {
	case <-mB.v2.catchUpDone:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("coordinator did not join external queue after complete catch-up")
	}
	if got := len(mB.v2.cmdSubs); got != 5 {
		t.Fatalf("concurrent catch-up wake created %d queue subscriptions, want 5", got)
	}
}

func TestClusterV2_CatchUpWaitsForStableDiscoveredMembership(t *testing.T) {
	sA, sB, sC := startClusterTriple(t)
	releaseB := make(chan struct{})
	sawA := make(chan struct{})
	var gateEnabled atomic.Bool
	t.Cleanup(func() {
		select {
		case <-releaseB:
		default:
			close(releaseB)
		}
		v2TestSnapshotGate = nil
		v2TestCatchUpSawSender = nil
		v2TestBeforeCatchUp = nil
		v2TestCatchUpTimeout = 0
		v2TestCatchUpRetryDelay = 0
	})
	v2TestSnapshotGate = func(serverID string) <-chan struct{} {
		if gateEnabled.Load() && serverID == sB.ID() {
			return releaseB
		}
		return nil
	}
	v2TestCatchUpSawSender = func(sender string) {
		if gateEnabled.Load() && sender == sA.ID() {
			select {
			case <-sawA:
			default:
				close(sawA)
			}
		}
	}
	v2TestBeforeCatchUp = func(m *Manager) {
		if !gateEnabled.Load() || m.serverID != sC.ID() {
			return
		}
		m.peers.mu.Lock()
		m.peers.lastBeat = make(map[string]time.Time)
		m.peers.mu.Unlock()
	}
	v2TestCatchUpTimeout = 200 * time.Millisecond
	v2TestCatchUpRetryDelay = time.Millisecond
	cfg := clusterConfig()
	mA, err := StartManager(sA, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mA.Stop)
	mB, err := StartManager(sB, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mB.Stop)
	waitClusterPeersN(t, 2, mA, mB)
	for _, manager := range []*Manager{mA, mB} {
		select {
		case <-manager.v2.catchUpDone:
		case <-time.After(8 * time.Second):
			t.Fatal("existing manager did not finish catch-up")
		}
	}

	seedNode := func(m *Manager, key string) {
		m.v2.mu.Lock()
		m.v2.nodes[key] = &v2NodeBinding{
			registrationID: newRegistrationIDV2(t),
			epoch:          1,
			cid:            1,
			natsServerID:   m.serverID,
			requestID:      mustUUIDV2(t),
		}
		m.v2.mu.Unlock()
	}
	seedNode(mA, "only-a")
	seedNode(mB, "only-b")
	seedSession := func(m *Manager, client, server string) string {
		sessionID := mustUUIDV2(t)
		m.v2.mu.Lock()
		m.v2.owners[sessionID] = &v2SessionMeta{
			SessionID:  sessionID,
			Owner:      m.serverID,
			Revision:   1,
			RequestID:  mustUUIDV2(t),
			ClientNode: client,
			ServerNode: server,
			State:      SessionStateAllocatedV2,
		}
		m.v2.mu.Unlock()
		return sessionID
	}
	sessionA := seedSession(mA, "only-a", "server-a")
	sessionB := seedSession(mB, "only-b", "server-b")

	gateEnabled.Store(true)
	type startResult struct {
		m   *Manager
		err error
	}
	started := make(chan startResult, 1)
	go func() {
		m, err := StartManager(sC, cfg)
		started <- startResult{m: m, err: err}
	}()
	select {
	case <-sawA:
	case <-time.After(time.Second):
		t.Fatal("C did not receive A snapshot")
	}
	var got startResult
	select {
	case got = <-started:
		if got.err != nil {
			t.Fatal(got.err)
		}
		t.Cleanup(got.m.Stop)
	case <-time.After(time.Second):
		t.Fatal("StartManager did not return after initial catch-up attempt")
	}
	select {
	case <-got.m.v2.catchUpDone:
		t.Fatal("C completed catch-up before delayed B snapshot")
	case <-time.After(75 * time.Millisecond):
	}
	close(releaseB)
	select {
	case <-got.m.v2.catchUpDone:
	case <-time.After(2 * time.Second):
		t.Fatal("C did not complete catch-up after B snapshot")
	}
	if !got.m.joinedQueueGroupV2() {
		t.Fatal("C did not join after stable A/B discovery")
	}
	got.m.v2.mu.Lock()
	_, haveA := got.m.v2.nodes["only-a"]
	_, haveB := got.m.v2.nodes["only-b"]
	got.m.v2.mu.Unlock()
	if !haveA || !haveB {
		t.Fatalf("C joined without both snapshots: A=%v B=%v", haveA, haveB)
	}
	if _, _, ok := got.m.SessionOwnerV2(sessionA); !ok {
		t.Fatal("C snapshot missing A session")
	}
	if _, _, ok := got.m.SessionOwnerV2(sessionB); !ok {
		t.Fatal("C snapshot missing B session")
	}
}

func TestClusterV2_FirstCoordinatorJoinsAfterStableEmptyDiscovery(t *testing.T) {
	sA, _ := startClusterPair(t)
	v2TestCatchUpTimeout = 10 * time.Millisecond
	v2TestCatchUpRetryDelay = time.Millisecond
	t.Cleanup(func() {
		v2TestCatchUpTimeout = 0
		v2TestCatchUpRetryDelay = 0
	})
	m, err := StartManager(sA, clusterConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Stop)
	select {
	case <-m.v2.catchUpDone:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("first coordinator did not join after stable empty discovery")
	}
}

func TestClusterV2_NodeSnapshotMutationTracksBindingState(t *testing.T) {
	s := startEmbedded(t)
	m, err := StartManager(s, Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Stop)
	base := v2MgrMutation{
		Kind:              v2MutationRegister,
		Sender:            "peer-a",
		NodeKey:           "node-a",
		RegistrationID:    newRegistrationIDV2(t),
		RegistrationEpoch: 1,
		CID:               7,
		NatsServerID:      "peer-a",
		RequestID:         mustUUIDV2(t),
		Pending:           true,
	}
	base.MutationID = snapshotNodeMutationIDV2(base)
	base.MessageID = base.MutationID
	current := base
	current.Pending = false
	current.MutationID = snapshotNodeMutationIDV2(current)
	current.MessageID = current.MutationID
	if base.MutationID == current.MutationID {
		t.Fatal("pending and final node snapshots share mutation id")
	}
	if !m.applyMutationV2(base) || !m.applyMutationV2(current) {
		t.Fatal("node snapshot state transition rejected")
	}
	m.v2.mu.Lock()
	binding := m.v2.nodes[base.NodeKey]
	m.v2.mu.Unlock()
	if binding == nil || binding.pending {
		t.Fatalf("final node snapshot was deduplicated: %+v", binding)
	}
	replay := current
	if got := snapshotNodeMutationIDV2(replay); got != current.MutationID {
		t.Fatalf("same snapshot content changed id: %s vs %s", got, current.MutationID)
	}
	otherSender := current
	otherSender.Sender = "peer-b"
	if snapshotNodeMutationIDV2(otherSender) == current.MutationID {
		t.Fatal("different snapshot senders share mutation id")
	}
}

func TestClusterV2_StopClosesRegisterIngressBeforeOwnerExit(t *testing.T) {
	s := startEmbedded(t)
	m, err := StartManager(s, Config{})
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 1)
	hold := make(chan struct{})
	v2TestStopExitEntered = entered
	v2TestStopExitHold = hold
	t.Cleanup(func() {
		select {
		case <-hold:
		default:
			close(hold)
		}
		v2TestStopExitEntered = nil
		v2TestStopExitHold = nil
	})
	stopped := make(chan struct{})
	go func() {
		m.Stop()
		close(stopped)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("Stop did not reach owner-exit barrier")
	}
	nc := agentConnApp(t, s, "late-register")
	t.Cleanup(nc.Close)
	subj, err := RegisterSubjectV2("late-register", s.ID())
	if err != nil {
		t.Fatal(err)
	}
	reply := requestV2Wait(t, nc, subj, registerFrameV2(t, mustUUIDV2(t), newRegistrationIDV2(t)), time.Second)
	if perr := mustErrorV2(t, reply.Data); perr.Code != ErrBusyV2 {
		t.Fatalf("REGISTER during Stop was admitted: %+v body=%s", perr, reply.Data)
	}
	close(hold)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop did not finish after owner-exit release")
	}
}

func TestClusterV2_CatchUpRejectsEmptyAndDiscoversUnknownSnapshotSenders(t *testing.T) {
	alive := map[string]struct{}{"peer-a": {}, "peer-c": {}}
	if catchUpSenderExpectedV2(alive, "") {
		t.Fatal("empty snapshot sender counted toward catch-up")
	}
	for _, sender := range []string{"peer-a", "peer-c", "new-peer"} {
		if !catchUpSenderExpectedV2(alive, sender) {
			t.Fatalf("non-empty snapshot sender %q rejected from discovery", sender)
		}
	}
	if !catchUpSenderExpectedV2(nil, "new-peer") {
		t.Fatal("discovering catch-up must accept a non-empty peer when alive set is empty")
	}
}

func TestClusterV2_SnapshotMembershipCompatibility(t *testing.T) {
	var legacy v2SnapshotReply
	if err := json.Unmarshal([]byte(`{"sender":"peer-a","nodes":[],"sessions":[]}`), &legacy); err != nil {
		t.Fatal(err)
	}
	discovered := make(map[string]struct{})
	discoverSnapshotMembersV2(discovered, "self", legacy)
	if !equalStringSetV2(discovered, map[string]struct{}{"peer-a": {}}) {
		t.Fatalf("legacy snapshot discovery = %v, want sender only", discovered)
	}

	discoverSnapshotMembersV2(discovered, "self", v2SnapshotReply{
		Sender:  "peer-b",
		Members: []string{"", "self", "peer-b", "ghost"},
	})
	want := map[string]struct{}{"peer-a": {}, "peer-b": {}, "ghost": {}}
	if !equalStringSetV2(discovered, want) {
		t.Fatalf("member discovery = %v, want %v", discovered, want)
	}

	// A stale transitive member blocks the current round, but is removed from
	// the remembered candidate set when no live peer reports it again. The
	// following stable round can therefore complete.
	alive := map[string]struct{}{"peer-a": {}}
	known := map[string]struct{}{"peer-a": {}, "ghost": {}}
	required := cloneStringSetV2(alive)
	mergeStringSetV2(required, known)
	candidate := cloneStringSetV2(alive)
	seen := map[string]struct{}{"peer-a": {}}
	if catchUpRoundCompleteV2(true, required, candidate, seen, 0) {
		t.Fatal("round completed while stale ghost was still required")
	}
	known = candidate
	required = cloneStringSetV2(alive)
	mergeStringSetV2(required, known)
	candidate = cloneStringSetV2(alive)
	if !catchUpRoundCompleteV2(true, required, candidate, seen, 0) {
		t.Fatal("stable round did not complete after stale ghost converged out")
	}
}

func TestClusterV2_CatchUpCancellationIsNotSuccess(t *testing.T) {
	s := startEmbedded(t)
	m, err := StartManager(s, Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Stop)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := m.catchUpV2(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled catch-up returned %v, want context.Canceled", err)
	}
}

func TestClusterV2_PartialQueueJoinDoesNotAdmitCommands(t *testing.T) {
	s := startEmbedded(t)
	m, err := StartManager(s, Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Stop)
	client := registerOnServerV2(t, s, mgrClientNodeV2, newRegistrationIDV2(t))
	_ = registerOnServerV2(t, s, mgrServerNodeV2, newRegistrationIDV2(t))
	if err := m.dropQueueGroupV2(); err != nil {
		t.Fatal(err)
	}

	sub, err := m.nc.QueueSubscribe("$P2P.V2.CMD.*.SESSION.CREATE", v2CoordinatorQueue, m.handleV2Command)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := m.nc.Flush(); err != nil {
		t.Fatal(err)
	}
	createSubj, err := CommandSubjectV2(mgrClientNodeV2, "SESSION.CREATE")
	if err != nil {
		t.Fatal(err)
	}
	reply := requestV2Wait(t, client, createSubj, createFrameV2(t, mustUUIDV2(t), mgrServerNodeV2), time.Second)
	if perr := mustErrorV2(t, reply.Data); perr.Code != ErrBusyV2 {
		t.Fatalf("partial queue join admitted CREATE: %+v body=%s", perr, reply.Data)
	}
}

func TestClusterV2_StopCancelsCatchUpWorker(t *testing.T) {
	sA, sB := startClusterPair(t)
	cfg := clusterConfig()
	mA, err := StartManager(sA, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mA.Stop)

	v2TestCatchUpTimeout = 10 * time.Millisecond
	v2TestCatchUpRetryDelay = time.Hour
	v2TestCatchUpExtraNeed = 8
	t.Cleanup(func() {
		v2TestCatchUpTimeout = 0
		v2TestCatchUpRetryDelay = 0
		v2TestCatchUpExtraNeed = 0
	})
	mB, err := StartManager(sB, cfg)
	if err != nil {
		t.Fatal(err)
	}

	stopped := make(chan struct{})
	go func() {
		mB.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Stop did not cancel catch-up retry worker")
	}
}

func TestClusterV2_DisconnectMatchesServerAndCID(t *testing.T) {
	sA, sB, _, mA, mB, mC := startClusterTripleManagers(t)
	node := "pair-node"
	reg := newRegistrationIDV2(t)
	nc := registerOnServerV2(t, sA, node, reg)
	_ = registerOnServerV2(t, sB, mgrServerNodeV2, newRegistrationIDV2(t))
	cids, err := mA.lookupNamedAgentConns(node)
	if err != nil || len(cids) != 1 {
		t.Fatalf("cid lookup: %v %v", err, cids)
	}
	cid := cids[0]
	createSubj, _ := CommandSubjectV2(node, "SESSION.CREATE")
	got := requestV2Wait(t, nc, createSubj, createFrameV2(t, mustUUIDV2(t), mgrServerNodeV2), 5*time.Second)
	alloc, err := DecodeFrameV2(FrameKindAllocatedV2, got.Data)
	if err != nil {
		t.Fatal(err)
	}

	mA.injectV2DisconnectFrom(mB.serverID, node, cid)
	if _, err := mA.v2.store.AllocateSession(node, CreateSessionCommandV2{RequestID: mustUUIDV2(t), ServerNodeKey: mgrServerNodeV2}); err != nil {
		t.Fatalf("wrong-server disconnect wiped node: %v", err)
	}
	mA.injectV2DisconnectFrom(mA.serverID, node, cid+99)
	if _, err := mA.v2.store.AllocateSession(node, CreateSessionCommandV2{RequestID: mustUUIDV2(t), ServerNodeKey: mgrServerNodeV2}); err != nil {
		t.Fatalf("wrong-cid disconnect wiped node: %v", err)
	}
	if _, _, ok := mA.SessionOwnerV2(alloc.Allocated.SessionID); !ok {
		t.Fatal("stale disconnect pair closed session")
	}

	mA.injectV2DisconnectFrom(mA.serverID, node, cid)
	freshReg := newRegistrationIDV2(t)
	nc.Close()
	moved := agentConnApp(t, sA, node)
	t.Cleanup(moved.Close)
	evSubj, _ := EventSubjectV2(node, freshReg)
	if _, err := moved.Subscribe(evSubj, func(*nats.Msg) {}); err != nil {
		t.Fatal(err)
	}
	_ = moved.Flush()
	mustRegisterV2(t, moved, sA.ID(), node, freshReg)
	mA.v2.store.SetExpiryDurations(40*time.Millisecond, 80*time.Millisecond)
	time.Sleep(80 * time.Millisecond)
	_ = mA.v2.store.Expire(mA.now())
	if _, err := mA.v2.store.AllocateSession(node, CreateSessionCommandV2{RequestID: mustUUIDV2(t), ServerNodeKey: mgrServerNodeV2}); err != nil {
		t.Fatalf("re-REGISTER before grace must keep node: %v", err)
	}
	if _, _, ok := mA.SessionOwnerV2(alloc.Allocated.SessionID); !ok {
		t.Fatal("re-REGISTER before grace closed session")
	}
	_ = mB
	_ = mC
}

func TestClusterV2_RegisterSyncWindowBusyNotNotRegistered(t *testing.T) {
	hold := make(chan struct{})
	v2TestHoldRegisterApply = hold
	v2TestHoldRegisterNode = mgrClientNodeV2
	t.Cleanup(func() {
		select {
		case <-hold:
		default:
			close(hold)
		}
		v2TestHoldRegisterApply = nil
		v2TestHoldRegisterNode = ""
	})
	sA, sB, _, mA, mB, mC := startClusterTripleManagers(t)

	clientReg := newRegistrationIDV2(t)
	nc := agentConnApp(t, sA, mgrClientNodeV2)
	t.Cleanup(nc.Close)
	evSubj, _ := EventSubjectV2(mgrClientNodeV2, clientReg)
	if _, err := nc.Subscribe(evSubj, func(*nats.Msg) {}); err != nil {
		t.Fatal(err)
	}
	_ = nc.Flush()
	_ = registerOnServerV2(t, sB, mgrServerNodeV2, newRegistrationIDV2(t))

	regSubj, _ := RegisterSubjectV2(mgrClientNodeV2, sA.ID())
	done := make(chan *nats.Msg, 1)
	go func() {
		got, err := nc.Request(regSubj, registerFrameV2(t, mustUUIDV2(t), clientReg), 8*time.Second)
		if err != nil {
			done <- &nats.Msg{Data: []byte(err.Error())}
			return
		}
		done <- got
	}()

	deadline := time.Now().Add(2 * time.Second)
	seen := false
	for time.Now().Before(deadline) {
		mB.v2.mu.Lock()
		b, ok := mB.v2.nodes[mgrClientNodeV2]
		pending := ok && b.pending
		mB.v2.mu.Unlock()
		if pending {
			seen = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !seen {
		t.Fatal("peer never saw mid-REGISTER pending window")
	}

	_ = mA
	if !mB.v2ExternalBlocked(mgrClientNodeV2) && !mC.v2ExternalBlocked(mgrClientNodeV2) {
		t.Fatal("other coordinator must treat mid-REGISTER-sync as busy")
	}
	createSubj, _ := CommandSubjectV2(mgrClientNodeV2, "SESSION.CREATE")
	busy := requestV2Wait(t, nc, createSubj, createFrameV2(t, mustUUIDV2(t), mgrServerNodeV2), 5*time.Second)
	perr := mustErrorV2(t, busy.Data)
	if perr.Code != ErrBusyV2 || perr.RetryAfterMs == nil {
		t.Fatalf("mid-REGISTER-sync CREATE want busy, got %+v %s", perr, busy.Data)
	}
	close(hold)
	select {
	case got := <-done:
		if _, err := DecodeFrameV2(FrameKindRegisterReplyV2, got.Data); err != nil {
			t.Fatalf("REGISTER after apply: %v body=%s", err, got.Data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("REGISTER did not complete after apply hold")
	}
	_ = mC
}

func TestClusterV2_InternalMutationOwnerRevisionGuard(t *testing.T) {
	sA, sB, _, mA, mB, mC := startClusterTripleManagers(t)
	client := registerOnServerV2(t, sA, mgrClientNodeV2, newRegistrationIDV2(t))
	_ = registerOnServerV2(t, sB, mgrServerNodeV2, newRegistrationIDV2(t))
	alloc := createAllocatedV2(t, client, mgrServerNodeV2)
	_, rev := waitSessionOwnerV2(t, alloc.SessionID, mA, mB, mC)

	mut := v2MgrMutation{
		MutationID: mustUUIDV2(t),
		MessageID:  mustUUIDV2(t),
		Kind:       v2MutationSession,
		SessionID:  alloc.SessionID,
		Owner:      "not-the-owner",
		Revision:   rev + 10,
		Sender:     mA.serverID,
	}
	if mB.applyMutationV2(mut) {
		t.Fatal("foreign owner mutation must not apply")
	}
	stale := mut
	stale.Owner = alloc.OwnerServerID
	stale.Revision = 0
	stale.MutationID = mustUUIDV2(t)
	if mC.applyMutationV2(stale) {
		t.Fatal("stale revision must not apply")
	}
	_, still := waitSessionOwnerV2(t, alloc.SessionID, mA, mB, mC)
	if still != rev {
		t.Fatalf("guard failed revision %d -> %d", rev, still)
	}
}

func TestClusterV2_DeliveryLedgerSnapshotDoesNotRevivePublished(t *testing.T) {
	owner, _ := newTestStoreV2(t)
	mustRegisterNodeV2(t, owner, storeClientNodeV2, storeClientRegV2)
	mustRegisterNodeV2(t, owner, storeServerNodeV2, storeServerRegV2)
	session := mustAllocateSessionV2(t, owner, storeClientNodeV2)
	cmd := sessionBindCmdV2(t, session)
	events, err := owner.BindSession(storeClientNodeV2, cmd)
	if err != nil {
		t.Fatal(err)
	}

	replica := NewStoreV2(nil, storeMaxConnsV2)
	mgr := &Manager{serverID: "replica", v2: &managerV2{
		store:    replica,
		owners:   make(map[string]*v2SessionMeta),
		requests: make(map[RequestKeyV2]string),
		applied:  make(map[string]struct{}),
	}}
	initial := v2MgrMutation{
		MutationID:      mustUUIDV2(t),
		MessageID:       mustUUIDV2(t),
		Kind:            v2MutationSession,
		SessionID:       session.SessionID,
		Owner:           "owner",
		Revision:        events[0].Revision,
		Sender:          "owner",
		ClientNodeKey:   storeClientNodeV2,
		ServerNodeKey:   storeServerNodeV2,
		State:           string(SessionStatePreparingV2),
		ConnectionID:    session.ConnectionID,
		ConnectionState: string(ConnectionStatePreparingV2),
		Connections: []v2ConnectionSnapshot{{
			ConnectionID: session.ConnectionID,
			Epoch:        session.Epoch,
			State:        string(ConnectionStatePreparingV2),
		}},
		Requests: owner.requestSnapshotsV2(session.SessionID),
	}
	if !mgr.applyMutationV2(initial) {
		t.Fatal("late-join session snapshot rejected")
	}
	if pending := replica.PendingEvents(storeClientNodeV2, cmd.RequestID); len(pending) != 2 {
		t.Fatalf("late join pending=%+v want two", pending)
	}

	if err := owner.MarkEventsPublished(storeClientNodeV2, cmd.RequestID, []string{events[0].MessageID}); err != nil {
		t.Fatal(err)
	}
	confirmed := v2MgrMutation{
		MutationID: mustUUIDV2(t),
		MessageID:  mustUUIDV2(t),
		Kind:       v2MutationDelivery,
		SessionID:  session.SessionID,
		Owner:      "owner",
		Sender:     "owner",
		Requests:   owner.requestSnapshotsV2(session.SessionID),
	}
	if !mgr.applyMutationV2(confirmed) {
		t.Fatal("delivery confirmation mutation rejected")
	}
	if pending := replica.PendingEvents(storeClientNodeV2, cmd.RequestID); len(pending) != 1 || pending[0].MessageID != events[1].MessageID {
		t.Fatalf("replica partial confirmation pending=%+v", pending)
	}

	stale := initial
	stale.MutationID = mustUUIDV2(t)
	stale.MessageID = mustUUIDV2(t)
	if !mgr.applyMutationV2(stale) {
		t.Fatal("stale peer snapshot should be harmless")
	}
	if pending := replica.PendingEvents(storeClientNodeV2, cmd.RequestID); len(pending) != 1 || pending[0].MessageID != events[1].MessageID {
		t.Fatalf("stale snapshot revived confirmed delivery: %+v", pending)
	}

	if err := owner.MarkEventsPublished(storeClientNodeV2, cmd.RequestID, []string{events[1].MessageID}); err != nil {
		t.Fatal(err)
	}
	confirmed.MutationID = mustUUIDV2(t)
	confirmed.MessageID = mustUUIDV2(t)
	confirmed.Requests = owner.requestSnapshotsV2(session.SessionID)
	if !mgr.applyMutationV2(confirmed) {
		t.Fatal("final delivery confirmation mutation rejected")
	}
	if pending := replica.PendingEvents(storeClientNodeV2, cmd.RequestID); len(pending) != 0 {
		t.Fatalf("replica retained fully confirmed delivery: %+v", pending)
	}

	stale.MutationID = mustUUIDV2(t)
	stale.MessageID = mustUUIDV2(t)
	if !mgr.applyMutationV2(stale) {
		t.Fatal("second stale peer snapshot should be harmless")
	}
	if pending := replica.PendingEvents(storeClientNodeV2, cmd.RequestID); len(pending) != 0 {
		t.Fatalf("fully confirmed delivery revived after stale snapshot: %+v", pending)
	}

	lateReplica := NewStoreV2(nil, storeMaxConnsV2)
	lateMgr := &Manager{serverID: "late-replica", v2: &managerV2{
		store:    lateReplica,
		owners:   make(map[string]*v2SessionMeta),
		requests: make(map[RequestKeyV2]string),
		applied:  make(map[string]struct{}),
	}}
	peerStale := initial
	peerStale.Sender = "stale-peer"
	peerStale.MutationID = snapshotSessionMutationIDV2(peerStale.Sender, peerStale.SessionID, peerStale.Revision, peerStale.Requests)
	peerStale.MessageID = peerStale.MutationID
	if !lateMgr.applyMutationV2(peerStale) {
		t.Fatal("late replica rejected stale peer snapshot")
	}
	peerCurrent := initial
	peerCurrent.Sender = "current-peer"
	peerCurrent.Requests = owner.requestSnapshotsV2(session.SessionID)
	peerCurrent.MutationID = snapshotSessionMutationIDV2(peerCurrent.Sender, peerCurrent.SessionID, peerCurrent.Revision, peerCurrent.Requests)
	peerCurrent.MessageID = peerCurrent.MutationID
	if !lateMgr.applyMutationV2(peerCurrent) {
		t.Fatal("late replica rejected current peer snapshot")
	}
	if pending := lateReplica.PendingEvents(storeClientNodeV2, cmd.RequestID); len(pending) != 0 {
		t.Fatalf("late join kept stale peer deliveries after current snapshot: %+v", pending)
	}
}

func TestClusterV2_DeliveryConfirmedBeforeNextPublish(t *testing.T) {
	sA, sB, _, mA, mB, mC := startClusterTripleManagers(t)
	managers := []*Manager{mA, mB, mC}
	clientReg := newRegistrationIDV2(t)
	serverReg := newRegistrationIDV2(t)
	client := registerOnServerV2(t, sA, mgrClientNodeV2, clientReg)
	srv := registerOnServerV2(t, sB, mgrServerNodeV2, serverReg)
	clientEv := subscribeEventsV2(t, client, mgrClientNodeV2, clientReg)
	serverEv := subscribeEventsV2(t, srv, mgrServerNodeV2, serverReg)
	alloc := createAllocatedV2(t, client, mgrServerNodeV2)
	owner, _ := waitSessionOwnerV2(t, alloc.SessionID, managers...)

	reqID := mustUUIDV2(t)
	ident := IdentityV2{SessionID: alloc.SessionID, ConnectionID: alloc.ConnectionID, Epoch: alloc.Epoch}
	bindSubj, err := CommandSubjectV2(mgrClientNodeV2, "SESSION.COMMAND")
	if err != nil {
		t.Fatal(err)
	}
	frame := sessionCmdFrameV2(t, reqID, SessionCommandBindV2, ident, alloc.Revision)
	secondEntered := make(chan struct{})
	releaseSecond := make(chan struct{})
	var attemptsMu sync.Mutex
	var attempts []string
	v2TestPublishHook = func(_ string, data []byte) error {
		var env EnvelopeV2
		if err := json.Unmarshal(data, &env); err != nil {
			return err
		}
		attemptsMu.Lock()
		attempts = append(attempts, env.MessageID)
		attempt := len(attempts)
		attemptsMu.Unlock()
		if attempt == 2 {
			close(secondEntered)
			<-releaseSecond
			return errors.New("injected second publish failure")
		}
		return nil
	}
	t.Cleanup(func() { v2TestPublishHook = nil })
	replyCh := make(chan *nats.Msg, 1)
	errCh := make(chan error, 1)
	go func() {
		reply, err := client.Request(bindSubj, frame, 5*time.Second)
		if err != nil {
			errCh <- err
			return
		}
		replyCh <- reply
	}()
	select {
	case <-secondEntered:
	case <-time.After(3 * time.Second):
		close(releaseSecond)
		t.Fatal("second publish did not block")
	}

	for _, manager := range managers {
		pending := manager.v2.store.PendingEvents(mgrClientNodeV2, reqID)
		if len(pending) != 1 {
			close(releaseSecond)
			t.Fatalf("manager %s pending while second publish blocked=%+v want one", manager.serverID, pending)
		}
	}
	first := nextEventV2(t, clientEv, FrameKindPrepareV2)
	close(releaseSecond)
	select {
	case err := <-errCh:
		t.Fatal(err)
	case reply := <-replyCh:
		if perr := mustErrorV2(t, reply.Data); perr.Code != ErrBusyV2 {
			t.Fatalf("blocked owner reply=%+v body=%s", perr, reply.Data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("blocked owner request did not complete")
	}

	takeover := mA
	if takeover.serverID == owner {
		takeover = mB
	}
	if takeover.serverID == owner {
		takeover = mC
	}
	if err := takeover.publishRequestEventsV2(mgrClientNodeV2, reqID); err != nil {
		t.Fatal(err)
	}
	second := nextEventV2(t, serverEv, FrameKindPrepareV2)
	attemptsMu.Lock()
	gotAttempts := append([]string(nil), attempts...)
	attemptsMu.Unlock()
	if len(gotAttempts) != 3 || gotAttempts[0] != first.Envelope.MessageID || gotAttempts[1] != gotAttempts[2] || gotAttempts[2] != second.Envelope.MessageID {
		t.Fatalf("takeover attempts=%v first=%s second=%s", gotAttempts, first.Envelope.MessageID, second.Envelope.MessageID)
	}
	assertNoEventV2(t, clientEv)
	assertNoEventV2(t, serverEv)
}

func TestClusterV2_LateJoinSnapshotsPeerBeforeQueueJoin(t *testing.T) {
	sA, sB, sC := startClusterTriple(t)
	cfg := clusterConfig()
	mA, err := StartManager(sA, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mA.Stop)
	mC, err := StartManager(sC, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mC.Stop)
	waitClusterPeersN(t, 2, mA, mC)

	client := registerOnServerV2(t, sA, mgrClientNodeV2, newRegistrationIDV2(t))
	_ = registerOnServerV2(t, sC, mgrServerNodeV2, newRegistrationIDV2(t))

	createSubj, err := CommandSubjectV2(mgrClientNodeV2, "SESSION.CREATE")
	if err != nil {
		t.Fatal(err)
	}
	reqID := mustUUIDV2(t)
	got := requestV2Wait(t, client, createSubj, createFrameV2(t, reqID, mgrServerNodeV2), 5*time.Second)
	alloc, err := DecodeFrameV2(FrameKindAllocatedV2, got.Data)
	if err != nil {
		t.Fatalf("CREATE: %v body=%s", err, got.Data)
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
	bindReq := mustUUIDV2(t)
	bind := requestV2Wait(t, client, bindSubj, sessionCmdFrameV2(t, bindReq, SessionCommandBindV2, ident, alloc.Allocated.Revision), 5*time.Second)
	if !bytesContainsOK(bind.Data) {
		t.Fatalf("BIND: %s", bind.Data)
	}

	mB, err := StartManager(sB, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mB.Stop)

	wantOwner := alloc.Allocated.OwnerServerID
	deadline := time.Now().Add(8 * time.Second)
	joined := false
	for time.Now().Before(deadline) {
		if mB.joinedQueueGroupV2() {
			joined = true
			o, _, ok := mB.SessionOwnerV2(alloc.Allocated.SessionID)
			if !ok || o != wantOwner || o == mB.serverID {
				t.Fatalf("B joined COORDINATORS without snapshotting peer: ok=%v owner=%s want %s", ok, o, wantOwner)
			}
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !joined {
		t.Fatal("B never joined after peer snapshot catch-up")
	}
	mB.v2.store.mu.Lock()
	bindRecord, haveBindRecord := mB.v2.store.requests[RequestKeyV2{SenderNodeKey: mgrClientNodeV2, RequestID: bindReq}]
	mB.v2.store.mu.Unlock()
	if !haveBindRecord || !bindRecord.hasEv || len(bindRecord.events) != 2 || len(bindRecord.pending) != 0 {
		t.Fatalf("late join BIND ledger missing or revived: found=%v record=%+v", haveBindRecord, bindRecord)
	}

	if err := mA.dropQueueGroupV2(); err != nil {
		t.Fatal(err)
	}
	if err := mC.dropQueueGroupV2(); err != nil {
		t.Fatal(err)
	}
	retry := requestV2Wait(t, client, createSubj, createFrameV2(t, reqID, mgrServerNodeV2), 5*time.Second)
	again, err := DecodeFrameV2(FrameKindAllocatedV2, retry.Data)
	if err != nil {
		t.Fatalf("retry CREATE after late join: %v body=%s", err, retry.Data)
	}
	if again.Allocated.SessionID != alloc.Allocated.SessionID {
		t.Fatalf("B minted second session %s vs %s", again.Allocated.SessionID, alloc.Allocated.SessionID)
	}
	if again.Allocated.OwnerServerID == mB.serverID {
		t.Fatal("B became second owner for A's request_id")
	}
	if o, _, ok := mB.SessionOwnerV2(alloc.Allocated.SessionID); !ok || o == mB.serverID {
		t.Fatalf("B owner map after retry: ok=%v owner=%s", ok, o)
	}
}

func TestClusterV2_CreateRetryUnsyncedMemberNoSecondOwner(t *testing.T) {
	sA, sB := startClusterPair(t)
	cfg := clusterConfig()
	mA, err := StartManager(sA, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mA.Stop)

	hold := make(chan struct{})
	v2TestHoldJoinQueue = hold
	v2TestDropSessionSync = true
	t.Cleanup(func() {
		select {
		case <-hold:
		default:
			close(hold)
		}
		v2TestHoldJoinQueue = nil
		v2TestDropSessionSync = false
		v2TestDropSessionSyncNode = ""
	})
	mB, err := StartManager(sB, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mB.Stop)
	v2TestHoldJoinQueue = nil
	v2TestDropSessionSyncNode = mB.serverID
	waitClusterPeers(t, mA, mB)
	if mB.joinedQueueGroupV2() {
		t.Fatal("B must stay off COORDINATORS until after CREATE")
	}

	client := registerOnServerV2(t, sA, mgrClientNodeV2, newRegistrationIDV2(t))
	_ = registerOnServerV2(t, sB, mgrServerNodeV2, newRegistrationIDV2(t))

	createSubj, err := CommandSubjectV2(mgrClientNodeV2, "SESSION.CREATE")
	if err != nil {
		t.Fatal(err)
	}
	reqID := mustUUIDV2(t)
	got := requestV2Wait(t, client, createSubj, createFrameV2(t, reqID, mgrServerNodeV2), 5*time.Second)
	alloc, err := DecodeFrameV2(FrameKindAllocatedV2, got.Data)
	if err != nil {
		t.Fatalf("CREATE: %v body=%s", err, got.Data)
	}
	if _, _, ok := mB.SessionOwnerV2(alloc.Allocated.SessionID); ok {
		t.Fatal("dropped SYNC must leave B without the session")
	}

	close(hold)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !mB.joinedQueueGroupV2() {
		time.Sleep(20 * time.Millisecond)
	}
	if !mB.joinedQueueGroupV2() {
		t.Fatal("B must join COORDINATORS before CREATE retry")
	}
	mA.dropQueueGroupV2()
	fwd, err := json.Marshal(v2ForwardedCmd{Subject: createSubj, Data: createFrameV2(t, reqID, mgrServerNodeV2)})
	if err != nil {
		t.Fatal(err)
	}
	sys := sysConnV2(t, sB)
	retry := requestV2Wait(t, sys, v2MgrCommandSubject(mB.serverID), fwd, 5*time.Second)
	if again, err := DecodeFrameV2(FrameKindAllocatedV2, retry.Data); err == nil {
		if again.Allocated.SessionID != alloc.Allocated.SessionID {
			t.Fatalf("unsynced member minted session %s vs %s", again.Allocated.SessionID, alloc.Allocated.SessionID)
		}
		if again.Allocated.OwnerServerID == mB.serverID {
			t.Fatal("B became second owner on CREATE retry")
		}
	} else {
		perr := mustErrorV2(t, retry.Data)
		if perr.Code != ErrBusyV2 && perr.Code != ErrCoordinatorUnavailableV2 {
			t.Fatalf("CREATE retry on unsynced member want busy/forward, got %+v %s", perr, retry.Data)
		}
	}

	mB.v2.mu.Lock()
	sid := mB.v2.requests[RequestKeyV2{SenderNodeKey: mgrClientNodeV2, RequestID: reqID}]
	meta := mB.v2.owners[sid]
	mB.v2.mu.Unlock()
	if meta != nil && meta.Owner == mB.serverID && sid != alloc.Allocated.SessionID {
		t.Fatalf("B locally allocated second session %s owner=%s", sid, meta.Owner)
	}
	if o, _, ok := mA.SessionOwnerV2(alloc.Allocated.SessionID); !ok || o != mA.serverID {
		t.Fatalf("A lost original ownership: ok=%v owner=%s", ok, o)
	}
}
