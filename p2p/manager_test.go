package p2p

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/nats-io/nats-server/v2/server"
)

func TestStartManagerOnlyV2Subscriptions(t *testing.T) {
	s := startEmbedded(t)
	m, err := StartManager(s, Config{STUNURLs: []string{"stun:turn.example.com:3478"}})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()
	if m.v2 == nil || m.v2.store == nil {
		t.Fatal("manager must init V2 store")
	}
	assertOnlyV2AndInternalManagerSubjects(t, s, s.ID())
}

func TestRegisterCreatePrepareSingleNode(t *testing.T) {
	s, _ := startManagerV2(t, Config{})
	assertOnlyV2AndInternalManagerSubjects(t, s, s.ID())

	client := agentConn(t, s, mgrClientNodeV2)
	srv := agentConn(t, s, mgrServerNodeV2)
	clientEv := subscribeEventsV2(t, client, mgrClientNodeV2, mgrClientRegV2)
	serverEv := subscribeEventsV2(t, srv, mgrServerNodeV2, mgrServerRegV2)

	mustRegisterV2(t, client, s.ID(), mgrClientNodeV2, mgrClientRegV2)
	// same connection re-REGISTER is idempotent
	mustRegisterV2(t, client, s.ID(), mgrClientNodeV2, mgrClientRegV2)
	mustRegisterV2(t, srv, s.ID(), mgrServerNodeV2, mgrServerRegV2)

	dup, err := nats.Connect("", nats.InProcessServer(s), nats.Name(mgrClientNodeV2))
	if err != nil {
		t.Fatal(err)
	}
	defer dup.Close()
	busy := tryRegisterV2(t, dup, s.ID(), mgrClientNodeV2, newRegistrationIDV2(t))
	if mustErrorV2(t, busy.Data).Code != ErrBusyV2 {
		t.Fatalf("duplicate name want busy: %s", busy.Data)
	}

	// subject token is the sender; a mismatched body node is not used
	bad := agentConn(t, s, "name-x")
	badSubj, err := RegisterSubjectV2("name-x", s.ID())
	if err != nil {
		t.Fatal(err)
	}
	gotBad := requestV2(t, bad, badSubj, registerFrameV2(t, mustUUIDV2(t), newRegistrationIDV2(t)))
	if _, err := DecodeFrameV2(FrameKindRegisterReplyV2, gotBad.Data); err != nil {
		t.Fatalf("register uses subject token, want reply: %s", gotBad.Data)
	}

	alloc := createAllocatedOnNodeV2(t, client, mgrClientNodeV2, mgrServerNodeV2)
	ident := IdentityV2{SessionID: alloc.SessionID, ConnectionID: alloc.ConnectionID, Epoch: alloc.Epoch}
	bindSubj, err := CommandSubjectV2(mgrClientNodeV2, "SESSION.COMMAND")
	if err != nil {
		t.Fatal(err)
	}
	_ = requestV2(t, client, bindSubj, sessionCmdFrameV2(t, mustUUIDV2(t), SessionCommandBindV2, ident, alloc.Revision))
	cPrep := nextEventV2(t, clientEv, FrameKindPrepareV2)
	sPrep := nextEventV2(t, serverEv, FrameKindPrepareV2)
	if cPrep.Prepare.Role != "client" || sPrep.Prepare.Role != "server" {
		t.Fatalf("prepare roles client=%+v server=%+v", cPrep.Prepare, sPrep.Prepare)
	}
	if cPrep.Prepare.Turn != nil || sPrep.Prepare.Turn != nil {
		t.Fatalf("no turn without secret: %+v %+v", cPrep.Prepare.Turn, sPrep.Prepare.Turn)
	}
}

func TestRegisterConcurrentSameNameOneWins(t *testing.T) {
	s, _ := startManagerV2(t, Config{})
	a := agentConn(t, s, "shared-key")
	b, err := nats.Connect("", nats.InProcessServer(s), nats.Name("shared-key"))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	start := make(chan struct{})
	results := make(chan ErrorCodeV2, 2)
	for _, nc := range []*nats.Conn{a, b} {
		go func(nc *nats.Conn) {
			<-start
			subj, err := RegisterSubjectV2("shared-key", s.ID())
			if err != nil {
				results <- ErrInternalErrorV2
				return
			}
			got, err := nc.Request(subj, registerFrameV2(t, mustUUIDV2(t), newRegistrationIDV2(t)), 2*time.Second)
			if err != nil {
				results <- ErrInternalErrorV2
				return
			}
			if _, err := DecodeFrameV2(FrameKindRegisterReplyV2, got.Data); err == nil {
				results <- ""
				return
			}
			results <- mustErrorV2(t, got.Data).Code
		}(nc)
	}
	close(start)
	ok, busy := 0, 0
	for i := 0; i < 2; i++ {
		switch <-results {
		case "":
			ok++
		case ErrBusyV2:
			busy++
		default:
			t.Fatal("expected one register reply and one busy")
		}
	}
	if ok > 1 || ok+busy != 2 {
		t.Fatalf("ok=%d busy=%d (never two successes; both busy is ok when connz=2)", ok, busy)
	}
}

func TestRegisterConnzNotExactIsBusy(t *testing.T) {
	s, _ := startManagerV2(t, Config{})
	c := agentConn(t, s, "solo")
	mustRegisterV2(t, c, s.ID(), "solo", newRegistrationIDV2(t))
	dup, err := nats.Connect("", nats.InProcessServer(s), nats.Name("solo"))
	if err != nil {
		t.Fatal(err)
	}
	defer dup.Close()
	got := tryRegisterV2(t, dup, s.ID(), "solo", newRegistrationIDV2(t))
	if mustErrorV2(t, got.Data).Code != ErrBusyV2 {
		t.Fatalf("want busy when connz is not exactly one, got %s", got.Data)
	}
}

func TestCreateBeforeRegister(t *testing.T) {
	s, _ := startManagerV2(t, Config{})
	c := agentConn(t, s, "solo")
	createSubj, err := CommandSubjectV2("solo", "SESSION.CREATE")
	if err != nil {
		t.Fatal(err)
	}
	got := requestV2(t, c, createSubj, createFrameV2(t, mustUUIDV2(t), "other"))
	if mustErrorV2(t, got.Data).Code != ErrNotRegisteredV2 {
		t.Fatalf("%s", got.Data)
	}
}

func TestCreatePeerNotRegistered(t *testing.T) {
	s, _ := startManagerV2(t, Config{})
	c := agentConn(t, s, "solo")
	mustRegisterV2(t, c, s.ID(), "solo", newRegistrationIDV2(t))
	createSubj, err := CommandSubjectV2("solo", "SESSION.CREATE")
	if err != nil {
		t.Fatal(err)
	}
	got := requestV2(t, c, createSubj, createFrameV2(t, mustUUIDV2(t), "missing"))
	if mustErrorV2(t, got.Data).Code != ErrPeerNotRegisteredV2 {
		t.Fatalf("%s", got.Data)
	}
}

func TestCreateDuringPendingRegisterDoesNotPrepare(t *testing.T) {
	s, cfg := startEmbeddedWithAccounts(t)
	m, err := StartManager(s, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	peer := agentConnApp(t, s, mgrServerNodeV2)
	t.Cleanup(peer.Close)
	mustRegisterV2(t, peer, s.ID(), mgrServerNodeV2, mgrServerRegV2)

	client := agentConnApp(t, s, mgrClientNodeV2)
	t.Cleanup(client.Close)
	createSubj, err := CommandSubjectV2(mgrClientNodeV2, "SESSION.CREATE")
	if err != nil {
		t.Fatal(err)
	}
	got := requestV2(t, client, createSubj, createFrameV2(t, mustUUIDV2(t), mgrServerNodeV2))
	if mustErrorV2(t, got.Data).Code != ErrNotRegisteredV2 {
		t.Fatalf("CREATE before client register should fail: %s", got.Data)
	}
}

func TestConnzAccountIsolation(t *testing.T) {
	sysAcc := server.NewAccount("SYS")
	appAcc := server.NewAccount(testAgentAccount)
	otherAcc := server.NewAccount("OTHER")
	opts := &server.Options{
		Host:          "127.0.0.1",
		Port:          -1,
		NoLog:         true,
		NoSigs:        true,
		JetStream:     false,
		Accounts:      []*server.Account{sysAcc, appAcc, otherAcc},
		SystemAccount: "SYS",
		Users: []*server.User{
			{Username: "sys", Password: "sys", Account: sysAcc},
			{Username: "app", Password: "app", Account: appAcc},
			{Username: "other", Password: "other", Account: otherAcc},
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

	cfg := Config{
		STUNURLs:      []string{"stun:turn.example.com:3478"},
		SysUsername:   "sys",
		SysPassword:   "sys",
		AgentAccount:  testAgentAccount,
		AgentUsername: "app",
		AgentPassword: "app",
	}
	m, err := StartManager(s, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	other, err := nats.Connect("", nats.InProcessServer(s), nats.UserInfo("other", "other"), nats.Name("shared-key"))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	app := agentConnApp(t, s, "shared-key")
	t.Cleanup(app.Close)

	cids, err := m.lookupNamedAgentConns("shared-key")
	if err != nil {
		t.Fatal(err)
	}
	if len(cids) != 1 {
		t.Fatalf("cids=%d want 1 (OTHER account conn must not count)", len(cids))
	}
	mustRegisterV2(t, app, s.ID(), "shared-key", newRegistrationIDV2(t))
}

func TestCreateWithSecretIncludesTurnOnPrepare(t *testing.T) {
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
	m.v2.store.now = func() time.Time { return fixed }

	client := agentConn(t, s, mgrClientNodeV2)
	peer := agentConn(t, s, mgrServerNodeV2)
	clientEv := subscribeEventsV2(t, client, mgrClientNodeV2, mgrClientRegV2)
	peerEv := subscribeEventsV2(t, peer, mgrServerNodeV2, mgrServerRegV2)
	mustRegisterV2(t, client, s.ID(), mgrClientNodeV2, mgrClientRegV2)
	mustRegisterV2(t, peer, s.ID(), mgrServerNodeV2, mgrServerRegV2)

	alloc := createAllocatedOnNodeV2(t, client, mgrClientNodeV2, mgrServerNodeV2)
	ident := IdentityV2{SessionID: alloc.SessionID, ConnectionID: alloc.ConnectionID, Epoch: alloc.Epoch}
	bindSubj, err := CommandSubjectV2(mgrClientNodeV2, "SESSION.COMMAND")
	if err != nil {
		t.Fatal(err)
	}
	_ = requestV2(t, client, bindSubj, sessionCmdFrameV2(t, mustUUIDV2(t), SessionCommandBindV2, ident, alloc.Revision))
	cPrep := nextEventV2(t, clientEv, FrameKindPrepareV2)
	sPrep := nextEventV2(t, peerEv, FrameKindPrepareV2)
	if cPrep.Prepare.Turn == nil || cPrep.Prepare.Turn.Username == "" {
		t.Fatalf("prepare missing turn: %+v", cPrep.Prepare)
	}
	user, pass := IssueREST(secret, ident.SessionID, ident.ConnectionID, ident.Epoch, fixed.Unix(), int64(DefaultCredentialTTL/time.Second))
	if cPrep.Prepare.Turn.Username != user || cPrep.Prepare.Turn.Password != pass {
		t.Fatalf("turn mismatch got=%s/%s want=%s/%s", cPrep.Prepare.Turn.Username, cPrep.Prepare.Turn.Password, user, pass)
	}
	if sPrep.Prepare.Turn.Username != user || sPrep.Prepare.Turn.Password != pass {
		t.Fatalf("peer turn mismatch %+v", sPrep.Prepare.Turn)
	}
}
