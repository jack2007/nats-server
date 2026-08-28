package p2p

import (
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/nats-io/nats-server/v2/server"
)

const testAgentAccount = "APP"

func startEmbedded(t *testing.T) *server.Server {
	t.Helper()
	opts := &server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, JetStream: false}
	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("not ready")
	}
	t.Cleanup(s.Shutdown)
	return s
}

func startEmbeddedWithAccounts(t *testing.T) (*server.Server, Config) {
	t.Helper()
	sysAcc := server.NewAccount("SYS")
	appAcc := server.NewAccount(testAgentAccount)
	opts := &server.Options{
		Host:          "127.0.0.1",
		Port:          -1,
		NoLog:         true,
		NoSigs:        true,
		JetStream:     false,
		Accounts:      []*server.Account{sysAcc, appAcc},
		SystemAccount: "SYS",
		Users: []*server.User{
			{Username: "sys", Password: "sys", Account: sysAcc},
			{Username: "app", Password: "app", Account: appAcc},
		},
	}
	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("not ready")
	}
	t.Cleanup(s.Shutdown)
	return s, Config{
		STUNURLs:      []string{"stun:turn.example.com:3478"},
		SysUsername:   "sys",
		SysPassword:   "sys",
		AgentAccount:  testAgentAccount,
		AgentUsername: "app",
		AgentPassword: "app",
	}
}

func agentConn(t *testing.T, s *server.Server, nodeKey string) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect("", nats.InProcessServer(s), nats.Name(nodeKey))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func agentConnApp(t *testing.T, s *server.Server, nodeKey string) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect("", nats.InProcessServer(s), nats.UserInfo("app", "app"), nats.Name(nodeKey))
	if err != nil {
		t.Fatal(err)
	}
	return nc
}

func tryRegisterV2(t *testing.T, nc *nats.Conn, serverID, node, reg string) *nats.Msg {
	t.Helper()
	subj, err := RegisterSubjectV2(node, serverID)
	if err != nil {
		t.Fatal(err)
	}
	return requestV2(t, nc, subj, registerFrameV2(t, mustUUIDV2(t), reg))
}

func createAllocatedOnNodeV2(t *testing.T, client *nats.Conn, clientNode, serverNode string) *AllocatedReplyV2 {
	t.Helper()
	createSubj, err := CommandSubjectV2(clientNode, "SESSION.CREATE")
	if err != nil {
		t.Fatal(err)
	}
	got := requestV2(t, client, createSubj, createFrameV2(t, mustUUIDV2(t), serverNode))
	alloc, err := DecodeFrameV2(FrameKindAllocatedV2, got.Data)
	if err != nil {
		t.Fatalf("ALLOCATED: %v body=%s", err, got.Data)
	}
	if alloc.Allocated == nil || !alloc.Allocated.OK {
		t.Fatalf("allocated %+v body=%s", alloc.Allocated, got.Data)
	}
	return alloc.Allocated
}

func assertOnlyV2AndInternalManagerSubjects(t *testing.T, s *server.Server, serverID string) {
	t.Helper()
	sz, err := s.Subsz(&server.SubszOptions{Subscriptions: true, Limit: 4096})
	if err != nil {
		t.Fatal(err)
	}
	wantReg := "$P2P.V2.CMD.*.REGISTER." + serverID
	wantCreate := "$P2P.V2.CMD.*.SESSION.CREATE"
	hasReg, hasCreate := false, false
	var p2p []string
	for i := range sz.Subs {
		sub := sz.Subs[i].Subject
		if !strings.HasPrefix(sub, "$P2P.") {
			continue
		}
		p2p = append(p2p, sub)
		switch {
		case strings.HasPrefix(sub, "$P2P.V2.CMD."),
			strings.HasPrefix(sub, "$P2P.V2.MGR."),
			strings.HasPrefix(sub, "$P2P.MGR."):
		default:
			t.Fatalf("unexpected p2p subject %s (want V2 command or internal manager only) all=%v", sub, p2p)
		}
		if sub == wantReg {
			hasReg = true
		}
		if sub == wantCreate {
			hasCreate = true
		}
	}
	if !hasReg || !hasCreate {
		t.Fatalf("missing V2 command subjects reg=%v create=%v subs=%v", hasReg, hasCreate, p2p)
	}
}
