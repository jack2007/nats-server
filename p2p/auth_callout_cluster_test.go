package p2p

import (
	"bytes"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/nats-io/nats-server/v2/server"
)

func calloutClusterOpts(serverName string, routes []*url.URL) *server.Options {
	return &server.Options{
		Host:        "127.0.0.1",
		Port:        -1,
		ServerName:  serverName,
		NoLog:       true,
		NoSigs:      true,
		AuthTimeout: 2.0,
		Cluster: server.ClusterOpts{
			Host: "127.0.0.1",
			Port: -1,
			Name: "p2p-callout",
		},
		Routes: routes,
		Users: []*server.User{
			{Username: AuthInternalName, Password: "auth-internal"},
			{Username: "p2p-internal", Password: "p2p-internal"},
		},
		AuthCallout: &server.AuthCallout{
			Issuer:    IssuerPublic(),
			AuthUsers: []string{AuthInternalName, "p2p-internal"},
		},
	}
}

func startCalloutClusterPair(t *testing.T) (sA, sB *server.Server, authA, authB *AuthCalloutService, mA, mB *Manager) {
	t.Helper()
	var err error
	sA, err = server.NewServer(calloutClusterOpts("nA", nil))
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
	sB, err = server.NewServer(calloutClusterOpts("nB", []*url.URL{route}))
	if err != nil {
		t.Fatal(err)
	}
	sB.Start()
	if !sB.ReadyForConnections(5 * time.Second) {
		t.Fatal("B")
	}
	waitClusterRoutes(t, sA, sB)

	authA, err = StartAuthCallout(sA, AuthInternalName, "auth-internal")
	if err != nil {
		t.Fatal(err)
	}
	authB, err = StartAuthCallout(sB, AuthInternalName, "auth-internal")
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		STUNURLs:      []string{"stun:turn.example.com:3478"},
		AgentUsername: "p2p-internal",
		AgentPassword: "p2p-internal",
	}
	mA, err = StartManager(sA, cfg)
	if err != nil {
		t.Fatal(err)
	}
	mB, err = StartManager(sB, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		mA.Stop()
		mB.Stop()
		authA.Stop()
		authB.Stop()
		sA.Shutdown()
		sB.Shutdown()
	})
	waitClusterPeers(t, mA, mB)
	return sA, sB, authA, authB, mA, mB
}

func TestCalloutClusterCreateCrossNode(t *testing.T) {
	t.Skip("V1 CREATE subjects are no longer granted; Task 8 converts this test to V2")
	sA, sB, _, _, _, _ := startCalloutClusterPair(t)
	client := mustConnectAgent(t, sA, "client-a")
	peer := mustConnectAgent(t, sB, "server-b")
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

func TestCalloutClusterPeerMayAnswerButAllStoppedRejects(t *testing.T) {
	sA, sB, authA, authB, _, _ := startCalloutClusterPair(t)
	authA.Stop()
	time.Sleep(100 * time.Millisecond)
	// Same issuer on every node: cluster may deliver $SYS.REQ.USER.AUTH to B.
	nc, err := connectTCP(sB, AgentUser, AgentPassword, "on-b")
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()
	authB.Stop()
	time.Sleep(100 * time.Millisecond)
	if _, err := connectTCP(sA, AgentUser, AgentPassword, "on-a"); err == nil {
		t.Fatal("CONNECT must fail after every local auth stopped")
	}
	if _, err := connectTCP(sB, AgentUser, AgentPassword, "on-b-2"); err == nil {
		t.Fatal("CONNECT must fail after every local auth stopped")
	}
}
