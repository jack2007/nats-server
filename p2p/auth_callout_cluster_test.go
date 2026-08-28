package p2p

import (
	"fmt"
	"net/url"
	"testing"
	"time"

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
	sA, sB, _, _, _, _ := startCalloutClusterPair(t)
	client := mustConnectAgent(t, sA, mgrClientNodeV2)
	peer := mustConnectAgent(t, sB, mgrServerNodeV2)
	clientEv := subscribeEventsV2(t, client, mgrClientNodeV2, mgrClientRegV2)
	peerEv := subscribeEventsV2(t, peer, mgrServerNodeV2, mgrServerRegV2)
	mustRegisterV2(t, client, sA.ID(), mgrClientNodeV2, mgrClientRegV2)
	mustRegisterV2(t, peer, sB.ID(), mgrServerNodeV2, mgrServerRegV2)
	alloc := createAllocatedOnNodeV2(t, client, mgrClientNodeV2, mgrServerNodeV2)
	ident := IdentityV2{SessionID: alloc.SessionID, ConnectionID: alloc.ConnectionID, Epoch: alloc.Epoch}
	bindSubj, err := CommandSubjectV2(mgrClientNodeV2, "SESSION.COMMAND")
	if err != nil {
		t.Fatal(err)
	}
	_ = requestV2(t, client, bindSubj, sessionCmdFrameV2(t, mustUUIDV2(t), SessionCommandBindV2, ident, alloc.Revision))
	if nextEventV2(t, clientEv, FrameKindPrepareV2).Prepare.Role != "client" {
		t.Fatal("client prepare")
	}
	if nextEventV2(t, peerEv, FrameKindPrepareV2).Prepare.Role != "server" {
		t.Fatal("server prepare")
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
