package p2p

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/nats-io/nats-server/v2/server"
)

func startManagerV2WithSys(t *testing.T) (*server.Server, *Manager, Config) {
	t.Helper()
	s, cfg := startEmbeddedWithAccounts(t)
	m, err := StartManager(s, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Stop)
	return s, m, cfg
}

func sysConnMonitorV2(t *testing.T, s *server.Server) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect("", nats.InProcessServer(s), nats.UserInfo("sys", "sys"), nats.Name("sys-statsz"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func requestStatszV2(t *testing.T, nc *nats.Conn, serverID string) []byte {
	t.Helper()
	got, err := nc.Request(statszSubjectV2(serverID), nil, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return got.Data
}

func collectStatszRepliesV2(t *testing.T, nc *nats.Conn, serverID string, wait time.Duration) [][]byte {
	t.Helper()
	inbox := nc.NewRespInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := nc.PublishRequest(statszSubjectV2(serverID), inbox, nil); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(wait)
	var replies [][]byte
	for time.Now().Before(deadline) {
		msg, err := sub.NextMsg(50 * time.Millisecond)
		if err != nil {
			continue
		}
		replies = append(replies, append([]byte(nil), msg.Data...))
	}
	return replies
}

func decodeStatszV2(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("statsz json: %v body=%s", err, raw)
	}
	return body
}

func labeledValueV2(samples any, match map[string]string) (float64, bool) {
	arr, ok := samples.([]any)
	if !ok {
		return 0, false
	}
	for _, item := range arr {
		row, ok := item.(map[string]any)
		if !ok {
			continue
		}
		good := true
		for k, want := range match {
			got, _ := row[k].(string)
			if got != want {
				good = false
				break
			}
		}
		if !good {
			continue
		}
		switch v := row["value"].(type) {
		case float64:
			return v, true
		}
	}
	return 0, false
}

func requireStatszShapeV2(t *testing.T, body map[string]any) {
	t.Helper()
	required := []string{
		"server_id",
		"p2p_sessions",
		"p2p_connections",
		"p2p_session_create_total",
		"p2p_signal_total",
		"p2p_signal_retry_total",
		"p2p_signal_deduplicated_total",
		"p2p_connection_limit_total",
		"p2p_command_queue_depth",
		"p2p_owner_sessions",
		"p2p_replica_sessions",
		"p2p_revision_sync_total",
	}
	for _, key := range required {
		if _, ok := body[key]; !ok {
			t.Fatalf("missing %s in %v", key, body)
		}
	}
}

func assertNoSecretsOrIDsV2(t *testing.T, raw []byte, banned ...string) {
	t.Helper()
	text := string(raw)
	for _, item := range banned {
		if item != "" && strings.Contains(text, item) {
			t.Fatalf("statsz leaked %q: %s", item, text)
		}
	}
	lower := strings.ToLower(text)
	for _, item := range []string{"turn_password", "\"password\"", "jwt", "nkey", "seed"} {
		if strings.Contains(lower, item) {
			t.Fatalf("statsz leaked secret field %q: %s", item, text)
		}
	}
}

func TestMonitorV2_StatszOnlyTargetInstanceAndRejectsApp(t *testing.T) {
	sA, sB := startClusterPair(t)
	cfg := clusterConfig()
	mA, err := StartManager(sA, cfg)
	if err != nil {
		t.Fatal(err)
	}
	mB, err := StartManager(sB, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mA.Stop)
	t.Cleanup(mB.Stop)
	waitClusterPeersN(t, 2, mA, mB)

	sysA := sysConnMonitorV2(t, sA)
	sysB := sysConnMonitorV2(t, sB)

	repliesA := collectStatszRepliesV2(t, sysA, sA.ID(), 300*time.Millisecond)
	if len(repliesA) != 1 {
		t.Fatalf("A STATSZ replies=%d want 1", len(repliesA))
	}
	bodyA := decodeStatszV2(t, repliesA[0])
	requireStatszShapeV2(t, bodyA)
	if bodyA["server_id"] != sA.ID() {
		t.Fatalf("server_id=%v want %s", bodyA["server_id"], sA.ID())
	}

	repliesBFromA := collectStatszRepliesV2(t, sysA, sB.ID(), 400*time.Millisecond)
	if len(repliesBFromA) != 1 {
		t.Fatalf("requesting B id must have only B reply: %d", len(repliesBFromA))
	}
	if decodeStatszV2(t, repliesBFromA[0])["server_id"] != sB.ID() {
		t.Fatalf("cross-server STATSZ must be B, got %s", repliesBFromA[0])
	}
	repliesB := collectStatszRepliesV2(t, sysB, sB.ID(), 300*time.Millisecond)
	if len(repliesB) != 1 {
		t.Fatalf("B STATSZ replies=%d want 1", len(repliesB))
	}
	if decodeStatszV2(t, repliesB[0])["server_id"] != sB.ID() {
		t.Fatalf("B server_id mismatch %s", repliesB[0])
	}

	app := agentConnApp(t, sA, "app-statsz")
	t.Cleanup(app.Close)
	_, err = app.Request(statszSubjectV2(sA.ID()), nil, 300*time.Millisecond)
	if err == nil {
		t.Fatal("app/agent account must not receive STATSZ")
	}
}

func TestMonitorV2_SnapshotAggregatesCreateSignalRetryDedupLimitAndOwners(t *testing.T) {
	s, m, _ := startManagerV2WithSys(t)
	client := agentConnApp(t, s, mgrClientNodeV2)
	srv := agentConnApp(t, s, mgrServerNodeV2)
	t.Cleanup(client.Close)
	t.Cleanup(srv.Close)
	clientEv := subscribeEventsV2(t, client, mgrClientNodeV2, mgrClientRegV2)
	serverEv := subscribeEventsV2(t, srv, mgrServerNodeV2, mgrServerRegV2)
	mustRegisterV2(t, client, s.ID(), mgrClientNodeV2, mgrClientRegV2)
	mustRegisterV2(t, srv, s.ID(), mgrServerNodeV2, mgrServerRegV2)

	ident, _ := mustCreateBindReadySessionV2(t, client, srv, clientEv, serverEv)

	sendSubj, err := CommandSubjectV2(mgrClientNodeV2, "SIGNAL.SEND")
	if err != nil {
		t.Fatal(err)
	}
	msgID := mustUUIDV2(t)
	reqID := mustUUIDV2(t)
	payload := map[string]any{
		"session_id":    ident.SessionID,
		"connection_id": ident.ConnectionID,
		"epoch":         ident.Epoch,
		"seq":           1,
		"type":          "description",
		"payload":       map[string]any{"sdp": "v=0"},
	}
	got := requestV2(t, client, sendSubj, encodeFrameV2ForTest(t, reqID, msgID, "", payload))
	reply := envelopePayloadMapV2(t, got.Data)
	if reply["ok"] != true {
		t.Fatalf("signal send %s", got.Data)
	}
	_ = nextEventV2(t, serverEv, FrameKindSignalSendV2)

	dup := requestV2(t, client, sendSubj, encodeFrameV2ForTest(t, reqID, msgID, "", payload))
	if envelopePayloadMapV2(t, dup.Data)["ok"] != true {
		t.Fatalf("dedup send %s", dup.Data)
	}

	openSubj, err := CommandSubjectV2(mgrClientNodeV2, "CONNECTION.COMMAND")
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 128; i++ {
		openID := fmt.Sprintf("conn-%d", i)
		openReq := mustUUIDV2(t)
		openGot := requestV2(t, client, openSubj, connCmdFrameV2(t, openReq, ConnectionCommandOpenV2, IdentityV2{
			SessionID:    ident.SessionID,
			ConnectionID: openID,
			Epoch:        1,
		}, 0))
		if i < 128 {
			if _, err := DecodeFrameV2(FrameKindAllocatedV2, openGot.Data); err != nil {
				t.Fatalf("open %s: %v %s", openID, err, openGot.Data)
			}
		} else {
			perr := mustErrorV2(t, openGot.Data)
			if perr.Code != ErrConnectionLimitV2 {
				t.Fatalf("129th open want connection_limit got %+v %s", perr, openGot.Data)
			}
		}
	}

	snap := m.SnapshotV2()
	raw, err := json.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	body := decodeStatszV2(t, raw)
	requireStatszShapeV2(t, body)
	if body["server_id"] != s.ID() {
		t.Fatalf("snapshot server_id=%v", body["server_id"])
	}
	okCreate, ok := labeledValueV2(body["p2p_session_create_total"], map[string]string{"result": "ok"})
	if !ok || okCreate < 1 {
		t.Fatalf("create ok=%v present=%v body=%s", okCreate, ok, raw)
	}
	if _, ok := labeledValueV2(body["p2p_sessions"], map[string]string{"state": "active", "role": "owner"}); !ok {
		t.Fatalf("missing active/owner session gauge: %s", raw)
	}
	if _, ok := labeledValueV2(body["p2p_connections"], map[string]string{"state": "active", "role": "owner"}); !ok {
		if _, ok := labeledValueV2(body["p2p_connections"], map[string]string{"state": "allocated", "role": "owner"}); !ok {
			if _, ok := labeledValueV2(body["p2p_connections"], map[string]string{"state": "preparing", "role": "owner"}); !ok {
				t.Fatalf("missing connection gauge: %s", raw)
			}
		}
	}
	sig, ok := labeledValueV2(body["p2p_signal_total"], map[string]string{
		"type": "description", "direction": "rx", "result": "ok",
	})
	if !ok || sig < 1 {
		t.Fatalf("signal total=%v present=%v %s", sig, ok, raw)
	}
	if toFloat(body["p2p_signal_retry_total"]) < 1 {
		t.Fatalf("retry total %v", body["p2p_signal_retry_total"])
	}
	if toFloat(body["p2p_signal_deduplicated_total"]) < 1 {
		t.Fatalf("dedup total %v", body["p2p_signal_deduplicated_total"])
	}
	if toFloat(body["p2p_connection_limit_total"]) < 1 {
		t.Fatalf("connection limit %v", body["p2p_connection_limit_total"])
	}
	if _, ok := body["p2p_command_queue_depth"].(float64); !ok {
		t.Fatalf("queue depth type %T", body["p2p_command_queue_depth"])
	}
	if toFloat(body["p2p_owner_sessions"]) < 1 {
		t.Fatalf("owner sessions %v", body["p2p_owner_sessions"])
	}
	if _, ok := labeledValueV2(body["p2p_revision_sync_total"], map[string]string{"result": "ok"}); !ok {
		t.Fatalf("missing revision sync: %s", raw)
	}
	assertNoSecretsOrIDsV2(t, raw, ident.SessionID, mgrClientNodeV2, mgrServerNodeV2, mgrClientRegV2, "turn-secret", "SUA")
}

func TestStatsZV2_SystemRequestMatchesSnapshotWithoutStoreWalk(t *testing.T) {
	s, m, _ := startManagerV2WithSys(t)
	client := agentConnApp(t, s, mgrClientNodeV2)
	srv := agentConnApp(t, s, mgrServerNodeV2)
	t.Cleanup(client.Close)
	t.Cleanup(srv.Close)
	mustRegisterV2(t, client, s.ID(), mgrClientNodeV2, mgrClientRegV2)
	mustRegisterV2(t, srv, s.ID(), mgrServerNodeV2, mgrServerRegV2)
	alloc := createAllocatedOnNodeV2(t, client, mgrClientNodeV2, mgrServerNodeV2)

	sys := sysConnMonitorV2(t, s)
	raw := requestStatszV2(t, sys, s.ID())
	body := decodeStatszV2(t, raw)
	requireStatszShapeV2(t, body)
	direct, err := json.Marshal(m.SnapshotV2())
	if err != nil {
		t.Fatal(err)
	}
	viaReq := decodeStatszV2(t, raw)
	viaSnap := decodeStatszV2(t, direct)
	if viaReq["server_id"] != viaSnap["server_id"] {
		t.Fatalf("request/snapshot server_id %v vs %v", viaReq["server_id"], viaSnap["server_id"])
	}
	if toFloat(viaReq["p2p_owner_sessions"]) != toFloat(viaSnap["p2p_owner_sessions"]) {
		t.Fatalf("owner sessions request=%v snap=%v", viaReq["p2p_owner_sessions"], viaSnap["p2p_owner_sessions"])
	}
	assertNoSecretsOrIDsV2(t, raw, alloc.SessionID, mgrClientNodeV2, mgrServerNodeV2)
}

func toFloat(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case uint64:
		return float64(n)
	default:
		return 0
	}
}
