package p2p

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/nats-io/nats-server/v2/server"
)

const (
	mgrClientNodeV2 = "client-a"
	mgrServerNodeV2 = "server-b"
	mgrOtherNodeV2  = "other-c"
	mgrClientRegV2  = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	mgrServerRegV2  = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	mgrOtherRegV2   = "cccccccccccccccccccccccccccccccc"
)

func encodeFrameV2ForTest(t *testing.T, requestID, messageID, registrationID string, payload any) []byte {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(EnvelopeV2{
		Version:        ProtocolVersionV2,
		RequestID:      requestID,
		MessageID:      messageID,
		RegistrationID: registrationID,
		Payload:        raw,
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func requestV2(t *testing.T, nc *nats.Conn, subject string, data []byte) *nats.Msg {
	t.Helper()
	got, err := nc.Request(subject, data, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func registerFrameV2(t *testing.T, requestID, registrationID string) []byte {
	t.Helper()
	return encodeFrameV2ForTest(t, requestID, "", registrationID, map[string]any{})
}

func createFrameV2(t *testing.T, requestID, serverNode string) []byte {
	t.Helper()
	return encodeFrameV2ForTest(t, requestID, "", "", map[string]any{
		"server_node_key": serverNode,
	})
}

func createFrameV2Timeout(t *testing.T, requestID, serverNode string, timeoutMs int64) []byte {
	t.Helper()
	return encodeFrameV2ForTest(t, requestID, "", "", map[string]any{
		"server_node_key":  serverNode,
		"total_timeout_ms": timeoutMs,
	})
}

func sessionCmdFrameV2(t *testing.T, requestID string, cmd SessionCommandKindV2, ident IdentityV2, revision uint64) []byte {
	t.Helper()
	payload := map[string]any{
		"command":       cmd,
		"session_id":    ident.SessionID,
		"connection_id": ident.ConnectionID,
		"epoch":         ident.Epoch,
	}
	if revision != 0 {
		payload["revision"] = revision
	}
	return encodeFrameV2ForTest(t, requestID, "", "", payload)
}

func connCmdFrameV2(t *testing.T, requestID string, cmd ConnectionCommandKindV2, ident IdentityV2, revision uint64) []byte {
	t.Helper()
	payload := map[string]any{
		"command":       cmd,
		"session_id":    ident.SessionID,
		"connection_id": ident.ConnectionID,
		"epoch":         ident.Epoch,
	}
	if revision != 0 {
		payload["revision"] = revision
	}
	return encodeFrameV2ForTest(t, requestID, "", "", payload)
}

func mustRegisterV2(t *testing.T, nc *nats.Conn, serverID, node, reg string) {
	t.Helper()
	subj, err := RegisterSubjectV2(node, serverID)
	if err != nil {
		t.Fatal(err)
	}
	reqID := mustUUIDV2(t)
	got := requestV2(t, nc, subj, registerFrameV2(t, reqID, reg))
	dec, err := DecodeFrameV2(FrameKindRegisterReplyV2, got.Data)
	if err != nil {
		t.Fatalf("register decode: %v body=%s", err, got.Data)
	}
	if dec.RegisterReply == nil || dec.RegisterReply.RegistrationID != reg || dec.RegisterReply.RegistrationEpoch == 0 {
		t.Fatalf("register reply %+v", dec.RegisterReply)
	}
}

func subscribeEventsV2(t *testing.T, nc *nats.Conn, node, reg string) chan *nats.Msg {
	t.Helper()
	subj, err := EventSubjectV2(node, reg)
	if err != nil {
		t.Fatal(err)
	}
	ch := make(chan *nats.Msg, 16)
	sub, err := nc.Subscribe(subj, func(msg *nats.Msg) { ch <- msg })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	return ch
}

func nextEventV2(t *testing.T, ch <-chan *nats.Msg, kind FrameKindV2) *DecodedV2 {
	t.Helper()
	select {
	case msg := <-ch:
		dec, err := DecodeFrameV2(kind, msg.Data)
		if err != nil {
			t.Fatalf("event %s: %v body=%s", kind, err, msg.Data)
		}
		if dec.Envelope.RegistrationID == "" {
			t.Fatalf("event missing registration_id: %s", msg.Data)
		}
		if !strings.Contains(msg.Subject, dec.Envelope.RegistrationID) {
			t.Fatalf("event subject %s missing registration %s", msg.Subject, dec.Envelope.RegistrationID)
		}
		return dec
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for %s", kind)
		return nil
	}
}

func mustErrorV2(t *testing.T, data []byte) *ErrorPayloadV2 {
	t.Helper()
	dec, err := DecodeFrameV2(FrameKindErrorV2, data)
	if err != nil {
		t.Fatalf("error decode: %v body=%s", err, data)
	}
	if dec.Error == nil {
		t.Fatalf("missing error payload: %s", data)
	}
	return dec.Error
}

func startManagerV2(t *testing.T, cfg Config) (*server.Server, *Manager) {
	t.Helper()
	s := startEmbedded(t)
	if cfg.STUNURLs == nil {
		cfg.STUNURLs = []string{"stun:turn.example.com:3478"}
	}
	m, err := StartManager(s, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Stop)
	return s, m
}

func TestManagerV2_CreateBindReadySessionOrder(t *testing.T) {
	s, _ := startManagerV2(t, Config{})
	client := agentConn(t, s, mgrClientNodeV2)
	srv := agentConn(t, s, mgrServerNodeV2)
	clientEv := subscribeEventsV2(t, client, mgrClientNodeV2, mgrClientRegV2)
	serverEv := subscribeEventsV2(t, srv, mgrServerNodeV2, mgrServerRegV2)

	mustRegisterV2(t, client, s.ID(), mgrClientNodeV2, mgrClientRegV2)
	mustRegisterV2(t, srv, s.ID(), mgrServerNodeV2, mgrServerRegV2)

	createSubj, err := CommandSubjectV2(mgrClientNodeV2, "SESSION.CREATE")
	if err != nil {
		t.Fatal(err)
	}
	createReq := mustUUIDV2(t)
	got := requestV2(t, client, createSubj, createFrameV2(t, createReq, mgrServerNodeV2))
	alloc, err := DecodeFrameV2(FrameKindAllocatedV2, got.Data)
	if err != nil {
		t.Fatalf("ALLOCATED: %v body=%s", err, got.Data)
	}
	if alloc.Allocated == nil || !alloc.Allocated.OK || alloc.Allocated.RequestID != createReq {
		t.Fatalf("allocated %+v", alloc.Allocated)
	}
	if alloc.Allocated.ConnectionID != "conn-0" || alloc.Allocated.Epoch != 1 || alloc.Allocated.State != string(SessionStateAllocatedV2) {
		t.Fatalf("allocated fields %+v", alloc.Allocated)
	}
	if alloc.Allocated.Revision != 1 || alloc.Allocated.OwnerServerID != s.ID() {
		t.Fatalf("allocated revision/owner %+v", alloc.Allocated)
	}

	ident := IdentityV2{
		SessionID:    alloc.Allocated.SessionID,
		ConnectionID: alloc.Allocated.ConnectionID,
		Epoch:        alloc.Allocated.Epoch,
	}
	bindSubj, err := CommandSubjectV2(mgrClientNodeV2, "SESSION.COMMAND")
	if err != nil {
		t.Fatal(err)
	}
	_ = requestV2(t, client, bindSubj, sessionCmdFrameV2(t, mustUUIDV2(t), SessionCommandBindV2, ident, alloc.Allocated.Revision))

	cPrep := nextEventV2(t, clientEv, FrameKindPrepareV2)
	sPrep := nextEventV2(t, serverEv, FrameKindPrepareV2)
	if cPrep.Prepare.Role != "client" || cPrep.Prepare.PeerNodeKey != mgrServerNodeV2 {
		t.Fatalf("client prepare %+v", cPrep.Prepare)
	}
	if sPrep.Prepare.Role != "server" || sPrep.Prepare.PeerNodeKey != mgrClientNodeV2 {
		t.Fatalf("server prepare %+v", sPrep.Prepare)
	}
	if cPrep.Prepare.Revision != sPrep.Prepare.Revision || cPrep.Prepare.SetupDeadlineMs <= 0 {
		t.Fatalf("prepare revision/deadline client=%+v server=%+v", cPrep.Prepare, sPrep.Prepare)
	}

	readySubj, err := CommandSubjectV2(mgrClientNodeV2, "CONNECTION.COMMAND")
	if err != nil {
		t.Fatal(err)
	}
	srvReadySubj, err := CommandSubjectV2(mgrServerNodeV2, "CONNECTION.COMMAND")
	if err != nil {
		t.Fatal(err)
	}
	_ = requestV2(t, client, readySubj, connCmdFrameV2(t, mustUUIDV2(t), ConnectionCommandReadyV2, ident, cPrep.Prepare.Revision))
	_ = requestV2(t, srv, srvReadySubj, connCmdFrameV2(t, mustUUIDV2(t), ConnectionCommandReadyV2, ident, sPrep.Prepare.Revision))

	cStart := nextEventV2(t, clientEv, FrameKindStartV2)
	sStart := nextEventV2(t, serverEv, FrameKindStartV2)
	if cStart.Start.Revision != sStart.Start.Revision || cStart.Start.Revision <= cPrep.Prepare.Revision {
		t.Fatalf("start revision client=%d server=%d prepare=%d", cStart.Start.Revision, sStart.Start.Revision, cPrep.Prepare.Revision)
	}
	select {
	case extra := <-clientEv:
		t.Fatalf("unexpected extra client event: %s", extra.Data)
	case extra := <-serverEv:
		t.Fatalf("unexpected extra server event: %s", extra.Data)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestManagerV2_SpoofedSenderIgnoredForSession(t *testing.T) {
	s, _ := startManagerV2(t, Config{})
	client := agentConn(t, s, mgrClientNodeV2)
	srv := agentConn(t, s, mgrServerNodeV2)
	outsider := agentConn(t, s, mgrOtherNodeV2)
	mustRegisterV2(t, client, s.ID(), mgrClientNodeV2, mgrClientRegV2)
	mustRegisterV2(t, srv, s.ID(), mgrServerNodeV2, mgrServerRegV2)
	mustRegisterV2(t, outsider, s.ID(), mgrOtherNodeV2, mgrOtherRegV2)

	createSubj, err := CommandSubjectV2(mgrClientNodeV2, "SESSION.CREATE")
	if err != nil {
		t.Fatal(err)
	}
	got := requestV2(t, client, createSubj, createFrameV2(t, mustUUIDV2(t), mgrServerNodeV2))
	alloc, err := DecodeFrameV2(FrameKindAllocatedV2, got.Data)
	if err != nil {
		t.Fatal(err)
	}
	ident := IdentityV2{
		SessionID:    alloc.Allocated.SessionID,
		ConnectionID: alloc.Allocated.ConnectionID,
		Epoch:        alloc.Allocated.Epoch,
	}

	// Body is a valid BIND; subject token (outsider) is the real sender.
	spoof := sessionCmdFrameV2(t, mustUUIDV2(t), SessionCommandBindV2, ident, alloc.Allocated.Revision)
	outSubj, err := CommandSubjectV2(mgrOtherNodeV2, "SESSION.COMMAND")
	if err != nil {
		t.Fatal(err)
	}
	reply := requestV2(t, outsider, outSubj, spoof)
	perr := mustErrorV2(t, reply.Data)
	if perr.Code != ErrNotSessionMemberV2 {
		t.Fatalf("spoof code=%s body=%s", perr.Code, reply.Data)
	}
}

func TestManagerV2_NonMemberSessionCommandError(t *testing.T) {
	s, _ := startManagerV2(t, Config{})
	client := agentConn(t, s, mgrClientNodeV2)
	srv := agentConn(t, s, mgrServerNodeV2)
	outsider := agentConn(t, s, mgrOtherNodeV2)
	mustRegisterV2(t, client, s.ID(), mgrClientNodeV2, mgrClientRegV2)
	mustRegisterV2(t, srv, s.ID(), mgrServerNodeV2, mgrServerRegV2)
	mustRegisterV2(t, outsider, s.ID(), mgrOtherNodeV2, mgrOtherRegV2)

	createSubj, _ := CommandSubjectV2(mgrClientNodeV2, "SESSION.CREATE")
	got := requestV2(t, client, createSubj, createFrameV2(t, mustUUIDV2(t), mgrServerNodeV2))
	alloc, err := DecodeFrameV2(FrameKindAllocatedV2, got.Data)
	if err != nil {
		t.Fatal(err)
	}
	ident := IdentityV2{
		SessionID:    alloc.Allocated.SessionID,
		ConnectionID: alloc.Allocated.ConnectionID,
		Epoch:        alloc.Allocated.Epoch,
	}
	outSubj, _ := CommandSubjectV2(mgrOtherNodeV2, "SESSION.COMMAND")
	reply := requestV2(t, outsider, outSubj, sessionCmdFrameV2(t, mustUUIDV2(t), SessionCommandBindV2, ident, alloc.Allocated.Revision))
	if mustErrorV2(t, reply.Data).Code != ErrNotSessionMemberV2 {
		t.Fatalf("body=%s", reply.Data)
	}
}

func TestManagerV2_QueueGroupSingleHandlerSessionCommand(t *testing.T) {
	var hits atomic.Int32
	v2TestOnCommand = func(suffix string) {
		if suffix == "SESSION.CREATE" || suffix == "SESSION.COMMAND" {
			hits.Add(1)
		}
	}
	t.Cleanup(func() { v2TestOnCommand = nil })

	s := startEmbedded(t)
	cfg := Config{STUNURLs: []string{"stun:turn.example.com:3478"}}
	for i := 0; i < 3; i++ {
		m, err := StartManager(s, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(m.Stop)
	}

	client := agentConn(t, s, mgrClientNodeV2)
	srv := agentConn(t, s, mgrServerNodeV2)
	mustRegisterV2(t, client, s.ID(), mgrClientNodeV2, mgrClientRegV2)
	mustRegisterV2(t, srv, s.ID(), mgrServerNodeV2, mgrServerRegV2)

	createSubj, _ := CommandSubjectV2(mgrClientNodeV2, "SESSION.CREATE")
	hits.Store(0)
	got := requestV2(t, client, createSubj, createFrameV2(t, mustUUIDV2(t), mgrServerNodeV2))
	alloc, err := DecodeFrameV2(FrameKindAllocatedV2, got.Data)
	if err != nil {
		t.Fatal(err)
	}
	if n := hits.Load(); n != 1 {
		t.Fatalf("SESSION.CREATE handlers=%d want 1", n)
	}

	hits.Store(0)
	got2 := requestV2(t, client, createSubj, createFrameV2(t, mustUUIDV2(t), mgrServerNodeV2))
	if _, err := DecodeFrameV2(FrameKindAllocatedV2, got2.Data); err != nil {
		t.Fatalf("second CREATE: %v body=%s", err, got2.Data)
	}
	if n := hits.Load(); n != 1 {
		t.Fatalf("second SESSION.CREATE handlers=%d want 1", n)
	}

	hits.Store(0)
	ident := IdentityV2{
		SessionID:    alloc.Allocated.SessionID,
		ConnectionID: alloc.Allocated.ConnectionID,
		Epoch:        alloc.Allocated.Epoch,
	}
	bindSubj, _ := CommandSubjectV2(mgrClientNodeV2, "SESSION.COMMAND")
	_ = requestV2(t, client, bindSubj, sessionCmdFrameV2(t, mustUUIDV2(t), SessionCommandBindV2, ident, alloc.Allocated.Revision))
	if n := hits.Load(); n != 1 {
		t.Fatalf("SESSION.COMMAND handlers=%d want 1", n)
	}
}

func TestManagerV2_CreateTotalTimeoutMsSetupTimeout(t *testing.T) {
	s, m := startManagerV2(t, Config{})
	client := agentConn(t, s, mgrClientNodeV2)
	srv := agentConn(t, s, mgrServerNodeV2)
	mustRegisterV2(t, client, s.ID(), mgrClientNodeV2, mgrClientRegV2)
	mustRegisterV2(t, srv, s.ID(), mgrServerNodeV2, mgrServerRegV2)

	createSubj, err := CommandSubjectV2(mgrClientNodeV2, "SESSION.CREATE")
	if err != nil {
		t.Fatal(err)
	}
	const shortMs int64 = 50
	got := requestV2(t, client, createSubj, createFrameV2Timeout(t, mustUUIDV2(t), mgrServerNodeV2, shortMs))
	alloc, err := DecodeFrameV2(FrameKindAllocatedV2, got.Data)
	if err != nil {
		t.Fatalf("CREATE with total_timeout_ms: %v body=%s", err, got.Data)
	}

	expired := m.v2.store.Expire(m.now().Add(time.Duration(shortMs+1) * time.Millisecond))
	if len(expired) == 0 {
		t.Fatal("expected setup_timeout after CREATE total_timeout_ms, not the 10s default")
	}

	ident := IdentityV2{
		SessionID:    alloc.Allocated.SessionID,
		ConnectionID: alloc.Allocated.ConnectionID,
		Epoch:        alloc.Allocated.Epoch,
	}
	bindSubj, err := CommandSubjectV2(mgrClientNodeV2, "SESSION.COMMAND")
	if err != nil {
		t.Fatal(err)
	}
	reply := requestV2(t, client, bindSubj, sessionCmdFrameV2(t, mustUUIDV2(t), SessionCommandBindV2, ident, alloc.Allocated.Revision))
	if mustErrorV2(t, reply.Data).Code != ErrSessionNotFoundV2 {
		t.Fatalf("BIND after setup timeout: %s", reply.Data)
	}

	got2 := requestV2(t, client, createSubj, createFrameV2(t, mustUUIDV2(t), mgrServerNodeV2))
	if _, err := DecodeFrameV2(FrameKindAllocatedV2, got2.Data); err != nil {
		t.Fatal(err)
	}
	if ev := m.v2.store.Expire(m.now().Add(time.Duration(shortMs+1) * time.Millisecond)); len(ev) != 0 {
		t.Fatalf("omitted total_timeout_ms must keep %s default: %+v", DefaultSetupTimeoutV2, ev)
	}
}

func TestManagerV2_ParallelSessionsSerialRevision(t *testing.T) {
	s, _ := startManagerV2(t, Config{})
	c1 := agentConn(t, s, "client-x")
	c2 := agentConn(t, s, "client-y")
	srv := agentConn(t, s, mgrServerNodeV2)
	mustRegisterV2(t, c1, s.ID(), "client-x", "11111111111111111111111111111111")
	mustRegisterV2(t, c2, s.ID(), "client-y", "22222222222222222222222222222222")
	mustRegisterV2(t, srv, s.ID(), mgrServerNodeV2, mgrServerRegV2)
	ev1 := subscribeEventsV2(t, c1, "client-x", "11111111111111111111111111111111")
	ev2 := subscribeEventsV2(t, c2, "client-y", "22222222222222222222222222222222")

	createOne := func(nc *nats.Conn, node string) IdentityV2 {
		t.Helper()
		subj, _ := CommandSubjectV2(node, "SESSION.CREATE")
		got := requestV2(t, nc, subj, createFrameV2(t, mustUUIDV2(t), mgrServerNodeV2))
		alloc, err := DecodeFrameV2(FrameKindAllocatedV2, got.Data)
		if err != nil {
			t.Fatal(err)
		}
		return IdentityV2{
			SessionID:    alloc.Allocated.SessionID,
			ConnectionID: alloc.Allocated.ConnectionID,
			Epoch:        alloc.Allocated.Epoch,
		}
	}
	id1 := createOne(c1, "client-x")
	id2 := createOne(c2, "client-y")
	if id1.SessionID == id2.SessionID {
		t.Fatal("sessions must be distinct")
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		subj, _ := CommandSubjectV2("client-x", "SESSION.COMMAND")
		_ = requestV2(t, c1, subj, sessionCmdFrameV2(t, mustUUIDV2(t), SessionCommandBindV2, id1, 1))
	}()
	go func() {
		defer wg.Done()
		subj, _ := CommandSubjectV2("client-y", "SESSION.COMMAND")
		_ = requestV2(t, c2, subj, sessionCmdFrameV2(t, mustUUIDV2(t), SessionCommandBindV2, id2, 1))
	}()
	wg.Wait()
	p1 := nextEventV2(t, ev1, FrameKindPrepareV2)
	p2 := nextEventV2(t, ev2, FrameKindPrepareV2)
	if p1.Prepare.Revision == 0 || p2.Prepare.Revision == 0 {
		t.Fatal("missing prepare revision")
	}
	if p1.Prepare.IdentityV2.SessionID == p2.Prepare.IdentityV2.SessionID {
		t.Fatal("parallel sessions must stay isolated")
	}

	readySubj, _ := CommandSubjectV2("client-x", "CONNECTION.COMMAND")
	_ = requestV2(t, c1, readySubj, connCmdFrameV2(t, mustUUIDV2(t), ConnectionCommandReadyV2, id1, p1.Prepare.Revision))
	// Same session: a second BIND after PREPARE must stay serial and not recreate the session.
	bindSubj, _ := CommandSubjectV2("client-x", "SESSION.COMMAND")
	second := requestV2(t, c1, bindSubj, sessionCmdFrameV2(t, mustUUIDV2(t), SessionCommandBindV2, id1, 1))
	if perr := func() *ErrorPayloadV2 {
		dec, err := DecodeFrameV2(FrameKindErrorV2, second.Data)
		if err != nil || dec.Error == nil {
			return nil
		}
		return dec.Error
	}(); perr != nil && perr.Code != ErrInvalidStateV2 {
		t.Fatalf("second bind body=%s", second.Data)
	}
}

func TestManagerV2_RegisterBusyWhenConnzNotExact(t *testing.T) {
	s, _ := startManagerV2(t, Config{})
	a := agentConn(t, s, mgrClientNodeV2)
	dup, err := nats.Connect("", nats.InProcessServer(s), nats.Name(mgrClientNodeV2))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dup.Close)
	if err := a.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := dup.Flush(); err != nil {
		t.Fatal(err)
	}
	subj, err := RegisterSubjectV2(mgrClientNodeV2, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	got := requestV2(t, a, subj, registerFrameV2(t, mustUUIDV2(t), mgrClientRegV2))
	perr := mustErrorV2(t, got.Data)
	if perr.Code != ErrBusyV2 || perr.RetryAfterMs == nil || *perr.RetryAfterMs < 100 || *perr.RetryAfterMs > 1000 {
		t.Fatalf("busy=%+v body=%s", perr, got.Data)
	}
}

func TestManagerV2_PrepareHasStunTurnAndHidesPassword(t *testing.T) {
	dir := t.TempDir()
	secretPath := filepath.Join(dir, "secret")
	if err := os.WriteFile(secretPath, []byte("turn-rest-secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	s, _ := startManagerV2(t, Config{
		STUNURLs:      []string{"stun:turn.example.com:3478"},
		TURNURLs:      []string{"turn:turn.example.com:3478?transport=udp"},
		SecretFile:    secretPath,
		CredentialTTL: time.Hour,
	})
	client := agentConn(t, s, mgrClientNodeV2)
	srv := agentConn(t, s, mgrServerNodeV2)
	clientEv := subscribeEventsV2(t, client, mgrClientNodeV2, mgrClientRegV2)
	serverEv := subscribeEventsV2(t, srv, mgrServerNodeV2, mgrServerRegV2)
	mustRegisterV2(t, client, s.ID(), mgrClientNodeV2, mgrClientRegV2)
	mustRegisterV2(t, srv, s.ID(), mgrServerNodeV2, mgrServerRegV2)

	createSubj, _ := CommandSubjectV2(mgrClientNodeV2, "SESSION.CREATE")
	got := requestV2(t, client, createSubj, createFrameV2(t, mustUUIDV2(t), mgrServerNodeV2))
	alloc, err := DecodeFrameV2(FrameKindAllocatedV2, got.Data)
	if err != nil {
		t.Fatal(err)
	}
	ident := IdentityV2{SessionID: alloc.Allocated.SessionID, ConnectionID: alloc.Allocated.ConnectionID, Epoch: alloc.Allocated.Epoch}
	bindSubj, _ := CommandSubjectV2(mgrClientNodeV2, "SESSION.COMMAND")
	_ = requestV2(t, client, bindSubj, sessionCmdFrameV2(t, mustUUIDV2(t), SessionCommandBindV2, ident, alloc.Allocated.Revision))

	cPrep := nextEventV2(t, clientEv, FrameKindPrepareV2)
	sPrep := nextEventV2(t, serverEv, FrameKindPrepareV2)
	for _, prep := range []*PrepareEventV2{cPrep.Prepare, sPrep.Prepare} {
		if len(prep.StunURLs) == 0 || prep.Turn == nil || prep.Turn.Password == "" {
			t.Fatalf("prepare missing ice %+v", prep)
		}
		if prep.FeatureBits != FeatureBitsV2 || prep.ProtocolVersion != ProtocolVersionV2 {
			t.Fatalf("features %+v", prep)
		}
		if prep.SetupDeadlineMs != DefaultSetupTimeoutV2.Milliseconds() {
			t.Fatalf("deadline %d", prep.SetupDeadlineMs)
		}
	}
	if cPrep.Prepare.Turn.Password != sPrep.Prepare.Turn.Password || cPrep.Prepare.Turn.ExpiresAt != sPrep.Prepare.Turn.ExpiresAt {
		t.Fatalf("turn mismatch client=%+v server=%+v", cPrep.Prepare.Turn, sPrep.Prepare.Turn)
	}

	out := agentConn(t, s, mgrOtherNodeV2)
	mustRegisterV2(t, out, s.ID(), mgrOtherNodeV2, mgrOtherRegV2)
	outSubj, _ := CommandSubjectV2(mgrOtherNodeV2, "CONNECTION.COMMAND")
	errMsg := requestV2(t, out, outSubj, connCmdFrameV2(t, mustUUIDV2(t), ConnectionCommandReadyV2, ident, cPrep.Prepare.Revision))
	if strings.Contains(string(errMsg.Data), cPrep.Prepare.Turn.Password) {
		t.Fatalf("error leaked turn password: %s", errMsg.Data)
	}
}

func TestManagerV2_CommandQueueBusy(t *testing.T) {
	v2TestQueueSize = 1
	v2TestWorkers = 1
	block := make(chan struct{})
	v2TestBlock = func() { <-block }
	t.Cleanup(func() {
		v2TestQueueSize = 0
		v2TestWorkers = 0
		v2TestBlock = nil
	})
	defer close(block)

	s, _ := startManagerV2(t, Config{})
	client := agentConn(t, s, mgrClientNodeV2)
	srv := agentConn(t, s, mgrServerNodeV2)
	mustRegisterV2(t, client, s.ID(), mgrClientNodeV2, mgrClientRegV2)
	mustRegisterV2(t, srv, s.ID(), mgrServerNodeV2, mgrServerRegV2)

	createSubj, _ := CommandSubjectV2(mgrClientNodeV2, "SESSION.CREATE")
	started := make(chan struct{})
	go func() {
		close(started)
		_, _ = client.Request(createSubj, createFrameV2(t, mustUUIDV2(t), mgrServerNodeV2), 3*time.Second)
	}()
	<-started
	time.Sleep(50 * time.Millisecond)

	var busy *ErrorPayloadV2
	for i := 0; i < 8; i++ {
		got, err := client.Request(createSubj, createFrameV2(t, mustUUIDV2(t), mgrServerNodeV2), time.Second)
		if err != nil {
			continue
		}
		dec, decErr := DecodeFrameV2(FrameKindErrorV2, got.Data)
		if decErr != nil || dec.Error == nil {
			continue
		}
		if dec.Error.Code == ErrBusyV2 {
			busy = dec.Error
			break
		}
	}
	if busy == nil || busy.RetryAfterMs == nil {
		t.Fatal("expected busy/retry_after_ms under saturation")
	}
	if *busy.RetryAfterMs < RetryInitialV2.Milliseconds() || *busy.RetryAfterMs > RetryMaxV2.Milliseconds() {
		t.Fatalf("retry_after_ms=%d", *busy.RetryAfterMs)
	}
}

func TestRetryAfterV2_BackoffBounded(t *testing.T) {
	seen := map[int64]bool{}
	for attempt := 0; attempt < 8; attempt++ {
		ms := RetryAfterMsV2(attempt)
		if ms < RetryInitialV2.Milliseconds() || ms > RetryMaxV2.Milliseconds() {
			t.Fatalf("attempt %d ms=%d", attempt, ms)
		}
		seen[ms] = true
	}
	if !seen[RetryInitialV2.Milliseconds()] {
		t.Fatal("initial 100ms missing")
	}
	if RetryInitialV2 != 100*time.Millisecond || RetryMaxV2 != time.Second {
		t.Fatalf("retry params initial=%s max=%s", RetryInitialV2, RetryMaxV2)
	}
}

func TestValidateTotalTimeoutMsV2(t *testing.T) {
	if _, err := ValidateTotalTimeoutMsV2(0); err == nil {
		t.Fatal("zero timeout")
	}
	if _, err := ValidateTotalTimeoutMsV2(-1); err == nil {
		t.Fatal("negative timeout")
	}
	d, err := ValidateTotalTimeoutMsV2(DefaultSetupTimeoutV2.Milliseconds())
	if err != nil || d != DefaultSetupTimeoutV2 {
		t.Fatalf("got %s err=%v", d, err)
	}
}
