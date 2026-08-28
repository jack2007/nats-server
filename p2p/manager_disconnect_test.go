package p2p

import (
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
	reg := newRegistrationIDV2(t)
	c := agentConnApp(t, s, "node-z")
	mustRegisterV2(t, c, s.ID(), "node-z", reg)
	c.Close()
	deadline := time.Now().Add(3 * time.Second)
	var last []byte
	for time.Now().Before(deadline) {
		c2 := agentConnApp(t, s, "node-z")
		got := tryRegisterV2(t, c2, s.ID(), "node-z", newRegistrationIDV2(t))
		if _, err := DecodeFrameV2(FrameKindRegisterReplyV2, got.Data); err == nil {
			c2.Close()
			return
		}
		last = got.Data
		c2.Close()
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("reregister failed, last=%s", last)
}
