package p2p

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
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
	if !strings.Contains(string(cEv), `"error"`) || !strings.Contains(string(sEv), `"error"`) {
		t.Fatalf("negotiating owner loss must notify both sides client=%s server=%s", cEv, sEv)
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
	sA, sB, _, mA, mB, mC := startClusterTripleManagers(t)
	hold := make(chan struct{})
	v2TestHoldRegisterSync = hold
	v2TestHoldRegisterNode = mgrClientNodeV2
	t.Cleanup(func() {
		v2TestHoldRegisterSync = nil
		v2TestHoldRegisterNode = ""
	})

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
	select {
	case msg := <-clientEv:
		t.Fatalf("unbound session emitted event: %s", msg.Data)
	case msg := <-serverEv:
		t.Fatalf("unbound session PREPARE to server: %s", msg.Data)
	case <-time.After(80 * time.Millisecond):
	}

	if ev := owner.v2.store.Expire(owner.now().Add(200 * time.Millisecond)); len(ev) == 0 {
		t.Fatal("unbound session must expire at deadline")
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
