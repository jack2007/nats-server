package p2p

import (
	"encoding/json"
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

func registerBody(nodeKey string) []byte {
	b, _ := json.Marshal(map[string]string{"node_key": nodeKey})
	return b
}
