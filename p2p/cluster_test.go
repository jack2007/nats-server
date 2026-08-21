package p2p

import (
	"bytes"
	"encoding/json"
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
	sA, sB, _, _ := startClusterManagers(t)

	ncA := agentConnApp(t, sA, "shared-key")
	if msg := requestP2P(t, ncA, "$P2P.REGISTER", "shared-key", registerBody("shared-key")); !bytes.Contains(msg.Data, []byte(`"ok":true`)) {
		t.Fatalf("A register: %s", msg.Data)
	}

	ncB := agentConnApp(t, sB, "shared-key")
	msg := requestP2P(t, ncB, "$P2P.REGISTER", "shared-key", registerBody("shared-key"))
	if !bytes.Contains(msg.Data, []byte(`node_key_in_use`)) {
		t.Fatalf("B should fail: %s", msg.Data)
	}
}

func TestClusterRegisterAfterClose(t *testing.T) {
	sA, sB, _, _ := startClusterManagers(t)

	ncA := agentConnApp(t, sA, "shared-key")
	if msg := requestP2P(t, ncA, "$P2P.REGISTER", "shared-key", registerBody("shared-key")); !bytes.Contains(msg.Data, []byte(`"ok":true`)) {
		t.Fatalf("A register: %s", msg.Data)
	}
	ncA.Close()

	ncB := agentConnApp(t, sB, "shared-key")
	deadline := time.Now().Add(5 * time.Second)
	var last []byte
	for time.Now().Before(deadline) {
		msg := requestP2P(t, ncB, "$P2P.REGISTER", "shared-key", registerBody("shared-key"))
		if bytes.Contains(msg.Data, []byte(`"ok":true`)) {
			return
		}
		last = msg.Data
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("B register after A close failed, last=%s", last)
}

func TestClusterCreateCrossNode(t *testing.T) {
	sA, sB, _, _ := startClusterManagers(t)

	client := agentConnApp(t, sA, "client-a")
	peer := agentConnApp(t, sB, "server-b")

	invCh := make(chan *nats.Msg, 2)
	if _, err := client.Subscribe("$P2P.NODE.client-a", func(msg *nats.Msg) { invCh <- msg }); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.Subscribe("$P2P.NODE.server-b", func(msg *nats.Msg) { invCh <- msg }); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	if msg := requestP2P(t, client, "$P2P.REGISTER", "client-a", registerBody("client-a")); !bytes.Contains(msg.Data, []byte(`"ok":true`)) {
		t.Fatalf("client register: %s", msg.Data)
	}
	if msg := requestP2P(t, peer, "$P2P.REGISTER", "server-b", registerBody("server-b")); !bytes.Contains(msg.Data, []byte(`"ok":true`)) {
		t.Fatalf("peer register: %s", msg.Data)
	}

	got := requestP2P(t, client, "$P2P.CREATE", "client-a", []byte(`{"peer_node_key":"server-b"}`))
	if !bytes.Contains(got.Data, []byte(`"ok":true`)) {
		t.Fatalf("create: %s", got.Data)
	}

	seen := 0
	deadline := time.After(3 * time.Second)
	for seen < 2 {
		select {
		case m := <-invCh:
			if !bytes.Contains(m.Data, []byte(`"type":"invite"`)) {
				t.Fatalf("invite: %s", m.Data)
			}
			seen++
		case <-deadline:
			t.Fatalf("invites=%d", seen)
		}
	}
}

func TestClusterConcurrentRegisterSameKey(t *testing.T) {
	sA, sB, _, _ := startClusterManagers(t)

	a := agentConnApp(t, sA, "shared-key")
	b := agentConnApp(t, sB, "shared-key")

	start := make(chan struct{})
	results := make(chan []byte, 2)
	for _, nc := range []*nats.Conn{a, b} {
		go func(nc *nats.Conn) {
			<-start
			msg := nats.NewMsg("$P2P.REGISTER")
			msg.Header.Set("Nats-P2P-Name", "shared-key")
			msg.Data = []byte(`{"node_key":"shared-key"}`)
			got, err := nc.RequestMsg(msg, 2*time.Second)
			if err != nil {
				results <- []byte(err.Error())
				return
			}
			results <- got.Data
		}(nc)
	}
	close(start)

	ok, inUse := 0, 0
	for i := 0; i < 2; i++ {
		data := <-results
		switch {
		case bytes.Contains(data, []byte(`"ok":true`)):
			ok++
		case bytes.Contains(data, []byte(`node_key_in_use`)):
			inUse++
		default:
			t.Fatalf("unexpected: %s", data)
		}
	}
	if ok != 1 || inUse != 1 {
		t.Fatalf("ok=%d in_use=%d (want 1 and 1)", ok, inUse)
	}
}

func TestClusterPrepareFailureRetryNotFalseIdempotent(t *testing.T) {
	_, sB, _, mB := startClusterManagers(t)
	account := testAgentAccount

	mB.peers.mu.Lock()
	mB.peers.lastBeat["ghost-peer"] = mB.now()
	mB.peers.mu.Unlock()

	ncB := agentConnApp(t, sB, "retry-key")
	failReq := nats.NewMsg("$P2P.REGISTER")
	failReq.Header.Set("Nats-P2P-Name", "retry-key")
	failReq.Data = registerBody("retry-key")
	got, err := ncB.RequestMsg(failReq, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(got.Data, []byte(`node_key_in_use`)) {
		t.Fatalf("first attempt should fail PREPARE: %s", got.Data)
	}
	if rec, ok := mB.table.Get(account, "retry-key"); ok && rec.ServerID == mB.serverID {
		t.Fatal("failed PREPARE should not leave a local reservation")
	}

	got, err = ncB.RequestMsg(failReq, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(got.Data, []byte(`"ok":true`)) {
		t.Fatalf("retry must not false-idempotent succeed while PREPARE still fails: %s", got.Data)
	}
	if !bytes.Contains(got.Data, []byte(`node_key_in_use`)) {
		t.Fatalf("retry should fail again: %s", got.Data)
	}
}

func TestClusterPrepareTimeoutRollsBackPeers(t *testing.T) {
	_, sB, mA, mB := startClusterManagers(t)
	account := testAgentAccount

	mB.peers.mu.Lock()
	mB.peers.lastBeat["ghost-peer"] = mB.now()
	mB.peers.mu.Unlock()

	ncB := agentConnApp(t, sB, "timeout-key")
	msg := nats.NewMsg("$P2P.REGISTER")
	msg.Header.Set("Nats-P2P-Name", "timeout-key")
	msg.Data = registerBody("timeout-key")
	got, err := ncB.RequestMsg(msg, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	msg = got
	if !bytes.Contains(msg.Data, []byte(`node_key_in_use`)) {
		t.Fatalf("want in_use on PREPARE timeout, got %s", msg.Data)
	}
	if rec, ok := mB.table.Get(account, "timeout-key"); ok && rec.ServerID == mB.serverID {
		t.Fatal("B should not retain a local PREPARE reservation after timeout rollback")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if rec, ok := mA.table.Get(account, "timeout-key"); !ok || rec.ServerID != mB.serverID {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("A should release B placeholder after timeout rollback")
}

func TestClusterStaleDisconnectDoesNotDropNewOccupant(t *testing.T) {
	sA, sB, mA, mB := startClusterManagers(t)
	account := testAgentAccount
	key := "shared-key"

	ncA := agentConnApp(t, sA, key)
	if msg := requestP2P(t, ncA, "$P2P.REGISTER", key, registerBody(key)); !bytes.Contains(msg.Data, []byte(`"ok":true`)) {
		t.Fatalf("A register: %s", msg.Data)
	}
	ncA.Close()

	ncB := agentConnApp(t, sB, key)
	deadline := time.Now().Add(10 * time.Second)
	var registered bool
	var last []byte
	for time.Now().Before(deadline) {
		msg := requestP2P(t, ncB, "$P2P.REGISTER", key, registerBody(key))
		if bytes.Contains(msg.Data, []byte(`"ok":true`)) {
			registered = true
			break
		}
		last = msg.Data
		time.Sleep(100 * time.Millisecond)
	}
	if !registered {
		t.Fatalf("B failed to register after A close, last=%s", last)
	}

	recB, ok := mA.table.Get(account, key)
	if !ok || recB.ServerID != mB.serverID {
		t.Fatalf("A table should show B owner before stale disconnect: %+v ok=%v", recB, ok)
	}

	ev, _ := json.Marshal(map[string]any{"client": map[string]string{"name": key}})
	mA.handleDisconnect(&nats.Msg{
		Subject: "$SYS.ACCOUNT." + account + ".DISCONNECT",
		Data:    ev,
	})

	if rec, ok := mA.table.Get(account, key); !ok || rec.ClaimID != recB.ClaimID {
		t.Fatalf("stale disconnect wiped A table: had=%+v now=%+v ok=%v", recB, rec, ok)
	}
	if rec, ok := mB.table.Get(account, key); !ok || rec.ClaimID != recB.ClaimID {
		t.Fatalf("stale disconnect wiped B table: had=%+v now=%+v ok=%v", recB, rec, ok)
	}

	peer := agentConnApp(t, sA, "peer-node")
	if msg := requestP2P(t, peer, "$P2P.REGISTER", "peer-node", registerBody("peer-node")); !bytes.Contains(msg.Data, []byte(`"ok":true`)) {
		t.Fatalf("peer register: %s", msg.Data)
	}
	got := requestP2P(t, ncB, "$P2P.CREATE", key, []byte(`{"peer_node_key":"peer-node"}`))
	if !bytes.Contains(got.Data, []byte(`"ok":true`)) {
		t.Fatalf("B CREATE after stale disconnect: %s", got.Data)
	}
}
