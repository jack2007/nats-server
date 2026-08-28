package p2p

import (
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/nats-io/nats-server/v2/server"
)

func clusterOpts(serverName string, routes []*url.URL) *server.Options {
	sysAcc := server.NewAccount("SYS")
	appAcc := server.NewAccount(testAgentAccount)
	return &server.Options{
		Host:       "127.0.0.1",
		Port:       -1,
		ServerName: serverName,
		NoLog:      true,
		NoSigs:     true,
		JetStream:  false,
		Cluster: server.ClusterOpts{
			Host: "127.0.0.1",
			Port: -1,
			Name: "p2p",
		},
		Routes:        routes,
		Accounts:      []*server.Account{sysAcc, appAcc},
		SystemAccount: "SYS",
		Users: []*server.User{
			{Username: "sys", Password: "sys", Account: sysAcc},
			{Username: "app", Password: "app", Account: appAcc},
		},
	}
}

func waitClusterRoutes(t *testing.T, sA, sB *server.Server) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if sA.NumRoutes() > 0 && sB.NumRoutes() > 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("cluster routes not formed A=%d B=%d", sA.NumRoutes(), sB.NumRoutes())
}

func clusterConfig() Config {
	return Config{
		STUNURLs:      []string{"stun:turn.example.com:3478"},
		SysUsername:   "sys",
		SysPassword:   "sys",
		AgentAccount:  testAgentAccount,
		AgentUsername: "app",
		AgentPassword: "app",
	}
}

func startClusterPair(t *testing.T) (sA, sB *server.Server) {
	t.Helper()
	optsA := clusterOpts("nA", nil)
	var err error
	sA, err = server.NewServer(optsA)
	if err != nil {
		t.Fatal(err)
	}
	sA.Start()
	if !sA.ReadyForConnections(5 * time.Second) {
		t.Fatal("A")
	}
	ca := sA.ClusterAddr()
	route, err := url.Parse(fmt.Sprintf("nats://127.0.0.1:%d", ca.Port))
	if err != nil {
		t.Fatal(err)
	}
	optsB := clusterOpts("nB", []*url.URL{route})
	sB, err = server.NewServer(optsB)
	if err != nil {
		t.Fatal(err)
	}
	sB.Start()
	if !sB.ReadyForConnections(5 * time.Second) {
		t.Fatal("B")
	}
	t.Cleanup(sA.Shutdown)
	t.Cleanup(sB.Shutdown)
	waitClusterRoutes(t, sA, sB)
	return sA, sB
}

func startClusterManagers(t *testing.T) (sA, sB *server.Server, mA, mB *Manager) {
	t.Helper()
	sA, sB = startClusterPair(t)
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
	t.Cleanup(mA.Stop)
	t.Cleanup(mB.Stop)
	waitClusterPeers(t, mA, mB)
	return sA, sB, mA, mB
}

func waitClusterPeers(t *testing.T, managers ...*Manager) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ready := true
		for _, m := range managers {
			if len(m.peers.alive(m.now())) < 2 {
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

func TestClusterRegisterDuplicateKey(t *testing.T) {
	sA, _, _, _ := startClusterManagers(t)
	ncA := agentConnApp(t, sA, "shared-key")
	t.Cleanup(ncA.Close)
	mustRegisterV2(t, ncA, sA.ID(), "shared-key", newRegistrationIDV2(t))

	dup := agentConnApp(t, sA, "shared-key")
	t.Cleanup(dup.Close)
	got := tryRegisterV2(t, dup, sA.ID(), "shared-key", newRegistrationIDV2(t))
	if mustErrorV2(t, got.Data).Code != ErrBusyV2 {
		t.Fatalf("duplicate name on same server want busy: %s", got.Data)
	}
}

func TestClusterRegisterAfterClose(t *testing.T) {
	sA, sB, _, _ := startClusterManagers(t)
	ncA := agentConnApp(t, sA, "shared-key")
	mustRegisterV2(t, ncA, sA.ID(), "shared-key", newRegistrationIDV2(t))
	ncA.Close()

	ncB := agentConnApp(t, sB, "shared-key")
	t.Cleanup(ncB.Close)
	deadline := time.Now().Add(5 * time.Second)
	var last []byte
	for time.Now().Before(deadline) {
		got := tryRegisterV2(t, ncB, sB.ID(), "shared-key", newRegistrationIDV2(t))
		if _, err := DecodeFrameV2(FrameKindRegisterReplyV2, got.Data); err == nil {
			return
		}
		last = got.Data
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("B register after A close failed, last=%s", last)
}

func TestClusterCreateCrossNode(t *testing.T) {
	sA, sB, _, _ := startClusterManagers(t)
	clientReg := newRegistrationIDV2(t)
	peerReg := newRegistrationIDV2(t)
	client := agentConnApp(t, sA, mgrClientNodeV2)
	peer := agentConnApp(t, sB, mgrServerNodeV2)
	t.Cleanup(client.Close)
	t.Cleanup(peer.Close)
	clientEv := subscribeEventsV2(t, client, mgrClientNodeV2, clientReg)
	peerEv := subscribeEventsV2(t, peer, mgrServerNodeV2, peerReg)
	mustRegisterV2(t, client, sA.ID(), mgrClientNodeV2, clientReg)
	mustRegisterV2(t, peer, sB.ID(), mgrServerNodeV2, peerReg)

	alloc := createAllocatedOnNodeV2(t, client, mgrClientNodeV2, mgrServerNodeV2)
	ident := IdentityV2{SessionID: alloc.SessionID, ConnectionID: alloc.ConnectionID, Epoch: alloc.Epoch}
	bindSubj, err := CommandSubjectV2(mgrClientNodeV2, "SESSION.COMMAND")
	if err != nil {
		t.Fatal(err)
	}
	_ = requestV2(t, client, bindSubj, sessionCmdFrameV2(t, mustUUIDV2(t), SessionCommandBindV2, ident, alloc.Revision))
	cPrep := nextEventV2(t, clientEv, FrameKindPrepareV2)
	sPrep := nextEventV2(t, peerEv, FrameKindPrepareV2)
	if cPrep.Prepare.Role != "client" || sPrep.Prepare.Role != "server" {
		t.Fatalf("prepare roles client=%+v server=%+v", cPrep.Prepare, sPrep.Prepare)
	}
}

func TestClusterConcurrentRegisterSameKey(t *testing.T) {
	sA, _, _, _ := startClusterManagers(t)
	a := agentConnApp(t, sA, "shared-key")
	b := agentConnApp(t, sA, "shared-key")
	t.Cleanup(a.Close)
	t.Cleanup(b.Close)

	start := make(chan struct{})
	results := make(chan ErrorCodeV2, 2)
	for _, nc := range []*nats.Conn{a, b} {
		go func(nc *nats.Conn) {
			<-start
			got := tryRegisterV2(t, nc, sA.ID(), "shared-key", newRegistrationIDV2(t))
			if _, err := DecodeFrameV2(FrameKindRegisterReplyV2, got.Data); err == nil {
				results <- ""
				return
			}
			results <- mustErrorV2(t, got.Data).Code
		}(nc)
	}
	close(start)
	ok, busy := 0, 0
	for i := 0; i < 2; i++ {
		switch <-results {
		case "":
			ok++
		case ErrBusyV2:
			busy++
		default:
			t.Fatal("expected one register reply and one busy")
		}
	}
	if ok > 1 || ok+busy != 2 {
		t.Fatalf("ok=%d busy=%d (never two successes; both busy is ok when connz=2)", ok, busy)
	}
}

func TestClusterStaleDisconnectDoesNotDropNewOccupant(t *testing.T) {
	sA, sB, mA, mB := startClusterManagers(t)
	key := "shared-key"
	reg := newRegistrationIDV2(t)
	ncB := agentConnApp(t, sB, key)
	t.Cleanup(ncB.Close)
	mustRegisterV2(t, ncB, sB.ID(), key, reg)

	cids, err := mB.lookupNamedAgentConns(key)
	if err != nil || len(cids) != 1 {
		t.Fatalf("cids=%v err=%v", cids, err)
	}
	// Wrong server / wrong CID must not mark the live occupant disconnected.
	mA.injectV2DisconnectFrom(sA.ID(), key, cids[0]+99)
	if _, ok := mB.v2.store.LookupNode(key); !ok {
		t.Fatal("stale disconnect dropped B occupancy")
	}

	peerReg := newRegistrationIDV2(t)
	peer := agentConnApp(t, sA, "peer-node")
	t.Cleanup(peer.Close)
	mustRegisterV2(t, peer, sA.ID(), "peer-node", peerReg)
	alloc := createAllocatedOnNodeV2(t, ncB, key, "peer-node")
	if alloc.SessionID == "" {
		t.Fatal("CREATE after stale disconnect")
	}
}

func TestClusterHeartbeatDropServerAllowsReregister(t *testing.T) {
	sA, sB, _, mB := startClusterManagers(t)
	key := "beat-key"
	ncB := agentConnApp(t, sB, key)
	mustRegisterV2(t, ncB, sB.ID(), key, newRegistrationIDV2(t))

	mB.Stop()
	sB.Shutdown()
	ncB.Close()

	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		if sA.NumRoutes() == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if sA.NumRoutes() > 0 {
		t.Fatalf("routes still up after B shutdown")
	}

	ncA := agentConnApp(t, sA, key)
	t.Cleanup(ncA.Close)
	var last []byte
	for time.Now().Before(deadline) {
		got := tryRegisterV2(t, ncA, sA.ID(), key, newRegistrationIDV2(t))
		if _, err := DecodeFrameV2(FrameKindRegisterReplyV2, got.Data); err == nil {
			return
		}
		last = got.Data
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("re-register after peer drop: last=%s", last)
}

func TestClusterV2_NodeDisconnectGraceAndTombstone(t *testing.T) {
	if got := NodeDisconnectGraceDuration(0, 0); got != 15*time.Second {
		t.Fatalf("zero ping grace=%s want 15s", got)
	}
	if got := NodeDisconnectGraceDuration(5*time.Second, 3); got != 20*time.Second {
		t.Fatalf("ping 5s*4 grace=%s want 20s", got)
	}
	if got := NodeDisconnectGraceDuration(2*time.Second, 2); got != 15*time.Second {
		t.Fatalf("small ping grace=%s want max(15s, 6s)=15s", got)
	}
	if got := TombstoneDuration(0); got != 60*time.Second {
		t.Fatalf("zero tombstone=%s want 60s", got)
	}
	if got := TombstoneDuration(10 * time.Second); got != 60*time.Second {
		t.Fatalf("short timeout tombstone=%s want 60s", got)
	}
	if got := TombstoneDuration(45 * time.Second); got != 90*time.Second {
		t.Fatalf("long timeout tombstone=%s want 90s", got)
	}
}
