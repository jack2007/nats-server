package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/nats-io/nats-server/v2/p2p"
)

func writeRunConf(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "nats.conf")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

func TestRun_StartsCoordinatorWithoutP2PBlock(t *testing.T) {
	port := freePort(t)
	path := writeRunConf(t, fmt.Sprintf("host: 127.0.0.1\nport: %d\n", port))
	stop := make(chan os.Signal, 1)
	errCh := make(chan error, 1)
	go func() { errCh <- run([]string{"-c", path}, stop) }()

	var nc *nats.Conn
	var err error
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		nc, err = nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", port), nats.Name("client-a"))
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if nc == nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	sid := nc.ConnectedServerId()
	subj, err := p2p.RegisterSubjectV2("client-a", sid)
	if err != nil {
		t.Fatal(err)
	}
	reqID, err := p2p.NewUUIDV2()
	if err != nil {
		t.Fatal(err)
	}
	reg := "dddddddddddddddddddddddddddddddd"
	env, err := json.Marshal(map[string]any{
		"version":         2,
		"request_id":      reqID,
		"registration_id": reg,
		"payload":         map[string]any{},
	})
	if err != nil {
		t.Fatal(err)
	}
	var dec *p2p.DecodedV2
	var got *nats.Msg
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got, err = nc.Request(subj, env, 2*time.Second)
		if err == nil {
			dec, err = p2p.DecodeFrameV2(p2p.FrameKindRegisterReplyV2, got.Data)
			if err == nil {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		body := []byte(nil)
		if got != nil {
			body = got.Data
		}
		t.Fatalf("coordinator not started: %v body=%s", err, body)
	}
	if dec.RegisterReply == nil || dec.RegisterReply.RegistrationEpoch == 0 {
		t.Fatalf("register reply %+v", dec.RegisterReply)
	}

	stop <- os.Interrupt
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not exit")
	}
}

func TestRun_StartManagerFailureAbortsBeforeSignalWait(t *testing.T) {
	port := freePort(t)
	path := writeRunConf(t, fmt.Sprintf(`
host: 127.0.0.1
port: %d
max_connections: 0
`, port))
	stop := make(chan os.Signal)
	errCh := make(chan error, 1)
	go func() { errCh <- run([]string{"-c", path}, stop) }()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected StartManager failure")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("blocked on signal wait")
	}
}
