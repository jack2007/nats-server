package p2p

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func requestP2P(t *testing.T, nc *nats.Conn, subject, name string, data []byte) *nats.Msg {
	t.Helper()
	msg := nats.NewMsg(subject)
	msg.Header.Set("Nats-P2P-Name", name)
	msg.Data = data
	got, err := nc.RequestMsg(msg, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestRegisterCreateInviteSingleNode(t *testing.T) {
	s := startEmbedded(t)
	m, err := StartManager(s, Config{
		STUNURLs: []string{"stun:turn.example.com:3478"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	client := agentConn(t, s, "client-a")
	server := agentConn(t, s, "server-b")
	if _, err := client.Subscribe("$P2P.NODE.client-a", func(*nats.Msg) {}); err != nil {
		t.Fatal(err)
	}
	invCh := make(chan *nats.Msg, 2)
	if _, err := server.Subscribe("$P2P.NODE.server-b", func(msg *nats.Msg) { invCh <- msg }); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Subscribe("$P2P.NODE.client-a", func(msg *nats.Msg) { invCh <- msg }); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	if msg := requestP2P(t, client, "$P2P.REGISTER", "client-a", []byte(`{"node_key":"client-a"}`)); !bytes.Contains(msg.Data, []byte(`"ok":true`)) {
		t.Fatalf("%s", msg.Data)
	}
	// spec 1: same connection re-REGISTER succeeds
	if msg := requestP2P(t, client, "$P2P.REGISTER", "client-a", []byte(`{"node_key":"client-a"}`)); !bytes.Contains(msg.Data, []byte(`"ok":true`)) {
		t.Fatalf("idempotent register: %s", msg.Data)
	}
	if msg := requestP2P(t, server, "$P2P.REGISTER", "server-b", []byte(`{"node_key":"server-b"}`)); !bytes.Contains(msg.Data, []byte(`"ok":true`)) {
		t.Fatalf("%s", msg.Data)
	}
	// 第二条连接同一 name
	dup, err := nats.Connect("", nats.InProcessServer(s), nats.Name("client-a"))
	if err != nil {
		t.Fatal(err)
	}
	defer dup.Close()
	msg := requestP2P(t, dup, "$P2P.REGISTER", "client-a", []byte(`{"node_key":"client-a"}`))
	if !bytes.Contains(msg.Data, []byte(`node_key_in_use`)) {
		t.Fatalf("%s", msg.Data)
	}
	// name mismatch
	bad := agentConn(t, s, "name-x")
	msg = requestP2P(t, bad, "$P2P.REGISTER", "name-x", []byte(`{"node_key":"other"}`))
	if !bytes.Contains(msg.Data, []byte(`name_mismatch`)) {
		t.Fatalf("%s", msg.Data)
	}

	got := requestP2P(t, client, "$P2P.CREATE", "client-a", []byte(`{"peer_node_key":"server-b"}`))
	if !bytes.Contains(got.Data, []byte(`"ok":true`)) || bytes.Contains(got.Data, []byte(`"grant"`)) {
		t.Fatalf("%s", got.Data)
	}
	if !bytes.Contains(got.Data, []byte(`$P2P.ICE.`)) || bytes.Contains(got.Data, []byte(`"turn"`)) {
		t.Fatalf("need ice_subject and no turn: %s", got.Data)
	}
	// 两个 invite
	deadline := time.After(2 * time.Second)
	seen := 0
	for seen < 2 {
		select {
		case m := <-invCh:
			if !bytes.Contains(m.Data, []byte(`"type":"invite"`)) {
				t.Fatalf("%s", m.Data)
			}
			seen++
		case <-deadline:
			t.Fatalf("invites=%d", seen)
		}
	}
}

func TestCreatePeerNotRegistered(t *testing.T) {
	s := startEmbedded(t)
	m, err := StartManager(s, Config{STUNURLs: []string{"stun:turn.example.com:3478"}})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()
	c := agentConn(t, s, "solo")
	if msg := requestP2P(t, c, "$P2P.REGISTER", "solo", []byte(`{"node_key":"solo"}`)); !bytes.Contains(msg.Data, []byte(`"ok":true`)) {
		t.Fatalf("%s", msg.Data)
	}
	msg := requestP2P(t, c, "$P2P.CREATE", "solo", []byte(`{"peer_node_key":"missing"}`))
	if !bytes.Contains(msg.Data, []byte(`peer_not_registered`)) {
		t.Fatalf("%s", msg.Data)
	}
}

func TestCreateWithSecretIncludesTurn(t *testing.T) {
	s := startEmbedded(t)
	dir := t.TempDir()
	secretPath := filepath.Join(dir, "secret")
	secret := []byte("test-static-auth-secret")
	if err := os.WriteFile(secretPath, secret, 0o600); err != nil {
		t.Fatal(err)
	}
	fixed := time.Unix(1700000000, 0)
	m, err := StartManager(s, Config{
		STUNURLs:      []string{"stun:turn.example.com:3478"},
		TURNURLs:      []string{"turn:turn.example.com:3478?transport=udp"},
		SecretFile:    secretPath,
		CredentialTTL: DefaultCredentialTTL,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()
	m.now = func() time.Time { return fixed }

	client := agentConn(t, s, "client-a")
	peer := agentConn(t, s, "server-b")
	if _, err := client.Subscribe("$P2P.NODE.client-a", func(*nats.Msg) {}); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.Subscribe("$P2P.NODE.server-b", func(*nats.Msg) {}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	requestP2P(t, client, "$P2P.REGISTER", "client-a", []byte(`{"node_key":"client-a"}`))
	requestP2P(t, peer, "$P2P.REGISTER", "server-b", []byte(`{"node_key":"server-b"}`))

	got := requestP2P(t, client, "$P2P.CREATE", "client-a", []byte(`{"peer_node_key":"server-b"}`))
	var resp struct {
		OK           bool     `json:"ok"`
		SessionID    string   `json:"session_id"`
		ConnectionID string   `json:"connection_id"`
		Epoch        uint64   `json:"epoch"`
		Turn         TurnCred `json:"turn"`
	}
	if err := json.Unmarshal(got.Data, &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.OK || resp.Turn.Username == "" {
		t.Fatalf("%s", got.Data)
	}
	user, pass := IssueREST(secret, resp.SessionID, resp.ConnectionID, resp.Epoch, fixed.Unix(), int64(DefaultCredentialTTL/time.Second))
	if resp.Turn.Username != user || resp.Turn.Password != pass {
		t.Fatalf("turn mismatch got=%s/%s want=%s/%s body=%s", resp.Turn.Username, resp.Turn.Password, user, pass, got.Data)
	}
}
