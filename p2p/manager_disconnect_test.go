package p2p

import (
	"bytes"
	"testing"
	"time"
)

func TestReleaseAfterDisconnectAllowsReregister(t *testing.T) {
	s, mgrOpts := startEmbeddedWithAccounts(t)
	m, err := StartManager(s, mgrOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()
	c := agentConnApp(t, s, "node-z")
	if msg := requestP2P(t, c, "$P2P.REGISTER", "node-z", registerBody("node-z")); !bytes.Contains(msg.Data, []byte(`"ok":true`)) {
		t.Fatalf("register: %s", msg.Data)
	}
	c.Close()
	deadline := time.Now().Add(3 * time.Second)
	var last []byte
	for time.Now().Before(deadline) {
		c2 := agentConnApp(t, s, "node-z")
		msg := requestP2P(t, c2, "$P2P.REGISTER", "node-z", registerBody("node-z"))
		if bytes.Contains(msg.Data, []byte(`"ok":true`)) {
			return
		}
		last = msg.Data
		c2.Close()
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("reregister failed, last=%s", last)
}
