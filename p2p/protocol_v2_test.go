package p2p

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const (
	testNodeKeyV2     = "client-a"
	testServerNodeV2  = "server-b"
	testServerIDV2    = "NCUOD3RJC5AEUIOLGR2CJ6B3FD3JQ7RS35TTAPC2NO75RQYAQFHNGGUY"
	testUserNKeyV2    = "UC2WHO4K2FFSONOOFYWBFXVSEYN2IUMTYTWUF2APBFNSBZPMBXM6BPVQ"
	testRegIDV2       = "0123456789abcdef0123456789abcdef"
	testRequestIDV2   = "550e8400-e29b-41d4-a716-446655440000"
	testMessageIDV2   = "7c9e6679-7425-40de-944b-e07fc1f90ae7"
	testSessionIDV2   = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	testConnectionV2  = "conn-0"
	testWireVectorsV2 = "testdata/v2_wire_vectors.json"
)

func requireInvalidRequestV2(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected invalid_request")
	}
	var perr *ProtocolErrorV2
	if !errors.As(err, &perr) || perr.Code != ErrInvalidRequestV2 {
		t.Fatalf("got %v, want invalid_request", err)
	}
}

func TestV2Subject_BuildersAndParse(t *testing.T) {
	type tc struct {
		name    string
		build   func() (string, error)
		want    string
		wantErr bool
	}
	cases := []tc{
		{
			name: "command_ok",
			build: func() (string, error) {
				return CommandSubjectV2(testNodeKeyV2, "SESSION.CREATE")
			},
			want: "$P2P.V2.CMD.client-a.SESSION.CREATE",
		},
		{
			name: "register_ok",
			build: func() (string, error) {
				return RegisterSubjectV2(testNodeKeyV2, testServerIDV2)
			},
			want: "$P2P.V2.CMD.client-a.REGISTER." + testServerIDV2,
		},
		{
			name: "event_ok",
			build: func() (string, error) {
				return EventSubjectV2(testNodeKeyV2, testRegIDV2)
			},
			want: "$P2P.V2.EVENT.client-a." + testRegIDV2,
		},
		{
			name: "command_bad_node",
			build: func() (string, error) {
				return CommandSubjectV2("bad.key", "SESSION.CREATE")
			},
			wantErr: true,
		},
		{
			name: "command_extra_token_suffix",
			build: func() (string, error) {
				return CommandSubjectV2(testNodeKeyV2, "SESSION.CREATE.EXTRA")
			},
			wantErr: true,
		},
		{
			name: "register_user_nkey",
			build: func() (string, error) {
				return RegisterSubjectV2(testNodeKeyV2, testUserNKeyV2)
			},
			wantErr: true,
		},
		{
			name: "event_bad_registration",
			build: func() (string, error) {
				return EventSubjectV2(testNodeKeyV2, "0123456789ABCDEF0123456789ABCDEF")
			},
			wantErr: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := c.build()
			if c.wantErr {
				requireInvalidRequestV2(t, err)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != c.want {
				t.Fatalf("got %q want %q", got, c.want)
			}
		})
	}

	subj, err := CommandSubjectV2(testNodeKeyV2, "SIGNAL.SEND")
	if err != nil {
		t.Fatal(err)
	}
	node, suffix, err := ParseCommandSubjectV2(subj)
	if err != nil {
		t.Fatal(err)
	}
	if node != testNodeKeyV2 || suffix != "SIGNAL.SEND" {
		t.Fatalf("parse %q %q", node, suffix)
	}

	reg, err := RegisterSubjectV2(testNodeKeyV2, testServerIDV2)
	if err != nil {
		t.Fatal(err)
	}
	node, suffix, err = ParseCommandSubjectV2(reg)
	if err != nil {
		t.Fatal(err)
	}
	if node != testNodeKeyV2 || suffix != "REGISTER."+testServerIDV2 {
		t.Fatalf("parse register %q %q", node, suffix)
	}
}

func TestV2Subject_RejectsMalformed(t *testing.T) {
	cases := []string{
		"$P2P.V2.CMD.client-a",
		"$P2P.V2.CMD",
		"$P2P.V2.CMD.client-a.SESSION",
		"$P2P.V2.CMD.client-a.SESSION.CREATE.EXTRA",
		"$P2P.V2.CMD.client-a.REGISTER." + testServerIDV2 + ".EXTRA",
		"$P2P.V2.CMD.*.SESSION.CREATE",
		"$P2P.V2.CMD.client-a.>",
		"$P2P.V2.CMD.client-a.SESSION.CREATE.>",
		"$P2P.LEGACY.CMD",
		"$P2P.V2.EVENT.client-a." + testRegIDV2,
		"$P2P.V2.CMD.client-a.SESSION.*.CREATE",
		"",
	}
	for _, subj := range cases {
		t.Run(subj, func(t *testing.T) {
			_, _, err := ParseCommandSubjectV2(subj)
			requireInvalidRequestV2(t, err)
		})
	}
}

func TestV2Protocol_ValidateNodeKey(t *testing.T) {
	ok := []string{"a", "Z", "_-_", "agent-self_1", strings.Repeat("n", 128)}
	for _, k := range ok {
		if err := ValidateNodeKeyV2(k); err != nil {
			t.Fatalf("%q: %v", k, err)
		}
	}
	bad := []string{"", "bad.key", "has*", "has>", "has ", strings.Repeat("n", 129), "中"}
	for _, k := range bad {
		requireInvalidRequestV2(t, ValidateNodeKeyV2(k))
	}
}

func TestV2Protocol_ValidateServerID(t *testing.T) {
	if err := ValidateServerIDV2(testServerIDV2); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"", "nats-server-id", testUserNKeyV2, "NNOTAVALIDSERVERNKEY", testNodeKeyV2} {
		requireInvalidRequestV2(t, ValidateServerIDV2(id))
	}
}

func TestV2Protocol_ValidateRegistrationID(t *testing.T) {
	if err := ValidateRegistrationIDV2(testRegIDV2); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{
		"",
		"0123456789abcdef0123456789abcde",
		"0123456789abcdef0123456789abcdef0",
		"0123456789ABCDEF0123456789ABCDEF",
		"0123456789abcdef0123456789abcdeg",
	} {
		requireInvalidRequestV2(t, ValidateRegistrationIDV2(id))
	}
}

func TestV2Protocol_ValidateUUIDAndConnectionID(t *testing.T) {
	if err := ValidateUUIDV2(testRequestIDV2); err != nil {
		t.Fatal(err)
	}
	if err := ValidateConnectionIDV2("conn-0"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateConnectionIDV2("conn-1"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateConnectionIDV2("conn-4294967295"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{
		"",
		"550E8400-E29B-41D4-A716-446655440000",
		"550e8400e29b41d4a716446655440000",
		"550e8400-e29b-41d4-a716-44665544000",
		"not-a-uuid",
	} {
		requireInvalidRequestV2(t, ValidateUUIDV2(id))
	}
	for _, id := range []string{"", "conn-", "conn-00", "conn-01", "conn--1", "0", "conn-a", "conn-10000000000"} {
		requireInvalidRequestV2(t, ValidateConnectionIDV2(id))
	}
}

func TestV2Protocol_NewUUID(t *testing.T) {
	id, err := NewUUIDV2()
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateUUIDV2(id); err != nil {
		t.Fatalf("%q: %v", id, err)
	}
}

func TestV2Protocol_ErrorCodes(t *testing.T) {
	want := map[ErrorCodeV2]string{
		ErrInvalidRequestV2:         "invalid_request",
		ErrNotRegisteredV2:          "not_registered",
		ErrPeerNotRegisteredV2:      "peer_not_registered",
		ErrNotSessionMemberV2:       "not_session_member",
		ErrSessionNotFoundV2:        "session_not_found",
		ErrConnectionNotFoundV2:     "connection_not_found",
		ErrConnectionLimitV2:        "connection_limit",
		ErrStaleEpochV2:             "stale_epoch",
		ErrFutureEpochV2:            "future_epoch",
		ErrSequenceGapV2:            "sequence_gap",
		ErrStaleRevisionV2:          "stale_revision",
		ErrInvalidStateV2:           "invalid_state",
		ErrSetupTimeoutV2:           "setup_timeout",
		ErrBusyV2:                   "busy",
		ErrCoordinatorUnavailableV2: "coordinator_unavailable",
		ErrInternalErrorV2:          "internal_error",
	}
	for code, text := range want {
		if string(code) != text {
			t.Fatalf("%q != %q", code, text)
		}
	}
}

func TestV2Protocol_ErrorPayloadFields(t *testing.T) {
	raw := []byte(`{"version":2,"request_id":"` + testRequestIDV2 + `","payload":{"error":"busy","retry_after_ms":250,"revision":9}}`)
	dec, err := DecodeFrameV2(FrameKindErrorV2, raw)
	if err != nil {
		t.Fatal(err)
	}
	if dec.Error == nil || dec.Error.Code != ErrBusyV2 {
		t.Fatalf("error=%v", dec.Error)
	}
	if dec.Error.RetryAfterMs == nil || *dec.Error.RetryAfterMs != 250 {
		t.Fatalf("retry_after_ms=%v", dec.Error.RetryAfterMs)
	}
	if dec.Error.Revision == nil || *dec.Error.Revision != 9 {
		t.Fatalf("revision=%v", dec.Error.Revision)
	}
	if dec.Error.SessionID != "" || dec.Error.ConnectionID != "" || dec.Error.Limit != nil {
		t.Fatalf("unexpected detail fields %+v", dec.Error)
	}
}

func TestV2Protocol_ErrorPayloadConnectionLimit(t *testing.T) {
	type tc struct {
		name    string
		payload string
		valid   bool
		code    ErrorCodeV2
		session string
		conn    string
		limit   int
	}
	cases := []tc{
		{
			name:    "manager_connection_limit",
			payload: `{"error":"connection_limit","session_id":"` + testSessionIDV2 + `","connection_id":"conn-128","limit":128}`,
			valid:   true,
			code:    ErrConnectionLimitV2,
			session: testSessionIDV2,
			conn:    "conn-128",
			limit:   128,
		},
		{
			name:    "busy_without_detail_still_ok",
			payload: `{"error":"busy","retry_after_ms":250}`,
			valid:   true,
			code:    ErrBusyV2,
		},
		{
			name:    "limit_present_not_128",
			payload: `{"error":"connection_limit","session_id":"` + testSessionIDV2 + `","connection_id":"conn-128","limit":64}`,
			valid:   false,
		},
		{
			name:    "invalid_session_id",
			payload: `{"error":"connection_limit","session_id":"not-a-uuid","connection_id":"conn-128","limit":128}`,
			valid:   false,
		},
		{
			name:    "invalid_connection_id",
			payload: `{"error":"connection_limit","session_id":"` + testSessionIDV2 + `","connection_id":"bad-id","limit":128}`,
			valid:   false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			body := `{"version":2,"request_id":"` + testRequestIDV2 + `","payload":` + c.payload + `}`
			dec, err := DecodeFrameV2(FrameKindErrorV2, []byte(body))
			if !c.valid {
				requireInvalidRequestV2(t, err)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if dec == nil || dec.Error == nil || dec.Error.Code != c.code {
				t.Fatalf("got %+v", dec)
			}
			if dec.Error.SessionID != c.session || dec.Error.ConnectionID != c.conn {
				t.Fatalf("ids session=%q conn=%q", dec.Error.SessionID, dec.Error.ConnectionID)
			}
			if c.limit == 0 {
				if dec.Error.Limit != nil {
					t.Fatalf("limit=%v", dec.Error.Limit)
				}
				return
			}
			if dec.Error.Limit == nil || *dec.Error.Limit != c.limit {
				t.Fatalf("limit=%v want %d", dec.Error.Limit, c.limit)
			}
		})
	}
}

func TestV2Protocol_Enums(t *testing.T) {
	if SessionStateAllocatedV2 != "allocated" || SessionStatePreparingV2 != "preparing" ||
		SessionStateActiveV2 != "active" || SessionStateDrainingV2 != "draining" ||
		SessionStateClosedV2 != "closed" || SessionStateFailedV2 != "failed" {
		t.Fatal("session states")
	}
	if ConnectionStatePreparingV2 != "preparing" || ConnectionStateActiveV2 != "active" ||
		ConnectionStateRestartingV2 != "restarting" || ConnectionStateClosedV2 != "closed" {
		t.Fatal("connection states")
	}
	if SessionCommandBindV2 != "BIND" || SessionCommandResumeV2 != "RESUME" || SessionCommandCloseV2 != "CLOSE" {
		t.Fatal("session commands")
	}
	if ConnectionCommandOpenV2 != "OPEN" || ConnectionCommandRestartV2 != "RESTART" ||
		ConnectionCommandBindV2 != "BIND" || ConnectionCommandReadyV2 != "READY" ||
		ConnectionCommandRejectV2 != "REJECT" || ConnectionCommandCloseV2 != "CLOSE" {
		t.Fatal("connection commands")
	}
	if SignalKindDescriptionV2 != "description" || SignalKindCandidateV2 != "candidate" ||
		SignalKindEndOfCandidatesV2 != "end_of_candidates" {
		t.Fatal("signal kinds")
	}
	if ProtocolVersionV2 != 2 {
		t.Fatalf("version=%d", ProtocolVersionV2)
	}
}

func TestV2Protocol_GoldenVectors(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("p2p", testWireVectorsV2))
	if err != nil {
		raw, err = os.ReadFile(testWireVectorsV2)
	}
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		ProtocolVersion int `json:"protocol_version"`
		Cases           []struct {
			Name  string          `json:"name"`
			Kind  FrameKindV2     `json:"kind"`
			Valid bool            `json:"valid"`
			Error string          `json:"error"`
			Body  json.RawMessage `json:"body"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	if file.ProtocolVersion != ProtocolVersionV2 {
		t.Fatalf("protocol_version=%d", file.ProtocolVersion)
	}
	if len(file.Cases) == 0 {
		t.Fatal("empty vectors")
	}
	for _, c := range file.Cases {
		t.Run(c.Name, func(t *testing.T) {
			dec, err := DecodeFrameV2(c.Kind, c.Body)
			if c.Valid {
				if err != nil {
					t.Fatal(err)
				}
				if dec == nil || dec.Envelope.Version != ProtocolVersionV2 {
					t.Fatalf("decoded=%v", dec)
				}
				return
			}
			requireInvalidRequestV2(t, err)
			if c.Error != "" && c.Error != string(ErrInvalidRequestV2) {
				t.Fatalf("vector error %q", c.Error)
			}
		})
	}
}

func TestV2Protocol_CloseProducerMatchesGoldenVectors(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("p2p", testWireVectorsV2))
	if err != nil {
		raw, err = os.ReadFile(testWireVectorsV2)
	}
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Cases []struct {
			Name string          `json:"name"`
			Body json.RawMessage `json:"body"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	vectors := make(map[string]json.RawMessage)
	for _, c := range file.Cases {
		if c.Name == "connection_close_event_ok" || c.Name == "session_close_event_ok" {
			vectors[c.Name] = c.Body
		}
	}
	if len(vectors) != 2 {
		t.Fatalf("close vectors=%d", len(vectors))
	}

	s := NewStoreV2(nil, 1)
	s.nodes[testNodeKeyV2] = &nodeRecordV2{key: testNodeKeyV2, registrationID: testRegIDV2}
	s.nodes[testServerNodeV2] = &nodeRecordV2{key: testServerNodeV2, registrationID: testRegIDV2}
	sess := &sessionRecordV2{
		id:       testSessionIDV2,
		revision: 7,
		client:   testNodeKeyV2,
		server:   testServerNodeV2,
	}
	conn := &connectionRecordV2{id: testConnectionV2, epoch: 2}
	events := s.notifyBoth(sess, conn, FrameKindCloseV2)
	if len(events) != 2 {
		t.Fatalf("close events=%d", len(events))
	}
	for _, name := range []string{"connection_close_event_ok", "session_close_event_ok"} {
		var want EnvelopeV2
		if err := json.Unmarshal(vectors[name], &want); err != nil {
			t.Fatal(err)
		}
		event := events[0]
		event.MessageID = want.MessageID
		gotRaw, err := encodeEnvelopeV2("", event.MessageID, event.RegistrationID, closeEventPayloadV2(event))
		if err != nil {
			t.Fatal(err)
		}
		var got, expected map[string]any
		if err := json.Unmarshal(gotRaw, &got); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(vectors[name], &expected); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, expected) {
			t.Fatalf("%s: got=%s want=%s", name, gotRaw, vectors[name])
		}
		dec, err := DecodeFrameV2(FrameKindCloseV2, gotRaw)
		if err != nil || dec.Close == nil || dec.Close.IdentityV2 != event.Identity ||
			dec.Close.Revision != event.Revision || dec.Close.State != SessionStateClosedV2 ||
			dec.Close.SenderNodeKey != v2CoordinatorSender {
			t.Fatalf("%s: decoded=%+v err=%v", name, dec, err)
		}
	}
}

func TestV2Protocol_SessionCloseProducesEveryConnectionGeneration(t *testing.T) {
	s := NewStoreV2(nil, 2)
	s.nodes[testNodeKeyV2] = &nodeRecordV2{key: testNodeKeyV2, registrationID: testRegIDV2}
	s.nodes[testServerNodeV2] = &nodeRecordV2{key: testServerNodeV2, registrationID: testRegIDV2}
	sess := &sessionRecordV2{
		id:       testSessionIDV2,
		revision: 7,
		client:   testNodeKeyV2,
		server:   testServerNodeV2,
		connections: map[string]*connectionRecordV2{
			"conn-0": {id: "conn-0", epoch: 2},
			"conn-1": {id: "conn-1", epoch: 3},
		},
	}
	s.sessions[sess.id] = sess
	events := s.closeSessionLocked(sess)
	if len(events) != 4 {
		t.Fatalf("close events=%d, want 4", len(events))
	}
	seen := make(map[GenerationKeyV2]int)
	for _, event := range events {
		if event.Kind != FrameKindCloseV2 {
			t.Fatalf("kind=%q", event.Kind)
		}
		seen[GenerationKeyV2{SessionID: event.Identity.SessionID, ConnectionID: event.Identity.ConnectionID, Epoch: event.Identity.Epoch}]++
	}
	for _, key := range []GenerationKeyV2{
		{SessionID: testSessionIDV2, ConnectionID: "conn-0", Epoch: 2},
		{SessionID: testSessionIDV2, ConnectionID: "conn-1", Epoch: 3},
	} {
		if seen[key] != 2 {
			t.Fatalf("generation %+v events=%d, want 2", key, seen[key])
		}
	}
}

func TestV2Protocol_CreateTotalTimeoutMs(t *testing.T) {
	type tc struct {
		name    string
		payload string
		valid   bool
		wantMs  int64
	}
	cases := []tc{
		{
			name:    "omitted_means_default",
			payload: `{"server_node_key":"` + testServerNodeV2 + `"}`,
			valid:   true,
			wantMs:  0,
		},
		{
			name:    "explicit_timeout",
			payload: `{"server_node_key":"` + testServerNodeV2 + `","total_timeout_ms":15000}`,
			valid:   true,
			wantMs:  15000,
		},
		{
			name:    "zero_rejected",
			payload: `{"server_node_key":"` + testServerNodeV2 + `","total_timeout_ms":0}`,
			valid:   false,
		},
		{
			name:    "negative_rejected",
			payload: `{"server_node_key":"` + testServerNodeV2 + `","total_timeout_ms":-1}`,
			valid:   false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			body := `{"version":2,"request_id":"` + testRequestIDV2 + `","payload":` + c.payload + `}`
			dec, err := DecodeFrameV2(FrameKindCreateV2, []byte(body))
			if c.valid {
				if err != nil {
					t.Fatal(err)
				}
				if dec == nil || dec.Create == nil || dec.Create.TotalTimeoutMs != c.wantMs {
					t.Fatalf("got %+v", dec)
				}
				return
			}
			requireInvalidRequestV2(t, err)
		})
	}
}

func TestV2Protocol_RegisterReply(t *testing.T) {
	type tc struct {
		name  string
		body  string
		want  bool
		epoch uint64
	}
	cases := []tc{
		{
			name:  "valid_echo_id_and_epoch",
			body:  `{"version":2,"request_id":"` + testRequestIDV2 + `","registration_id":"` + testRegIDV2 + `","payload":{"registration_epoch":1}}`,
			want:  true,
			epoch: 1,
		},
		{
			name: "missing_epoch",
			body: `{"version":2,"request_id":"` + testRequestIDV2 + `","registration_id":"` + testRegIDV2 + `","payload":{}}`,
		},
		{
			name: "zero_epoch",
			body: `{"version":2,"request_id":"` + testRequestIDV2 + `","registration_id":"` + testRegIDV2 + `","payload":{"registration_epoch":0}}`,
		},
		{
			name: "invalid_registration_id",
			body: `{"version":2,"request_id":"` + testRequestIDV2 + `","registration_id":"0123456789ABCDEF0123456789ABCDEF","payload":{"registration_epoch":1}}`,
		},
		{
			name: "missing_registration_id",
			body: `{"version":2,"request_id":"` + testRequestIDV2 + `","payload":{"registration_epoch":1}}`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dec, err := DecodeFrameV2(FrameKindRegisterReplyV2, []byte(c.body))
			if c.want {
				if err != nil {
					t.Fatal(err)
				}
				if dec == nil || dec.RegisterReply == nil {
					t.Fatalf("decoded=%v", dec)
				}
				if dec.RegisterReply.RegistrationID != testRegIDV2 {
					t.Fatalf("registration_id=%q", dec.RegisterReply.RegistrationID)
				}
				if dec.RegisterReply.RegistrationEpoch != c.epoch {
					t.Fatalf("registration_epoch=%d", dec.RegisterReply.RegistrationEpoch)
				}
				if dec.RegisterReply.RequestID != testRequestIDV2 {
					t.Fatalf("request_id=%q", dec.RegisterReply.RequestID)
				}
				return
			}
			requireInvalidRequestV2(t, err)
		})
	}
}

func TestV2Protocol_ConnectionOpenIdentity(t *testing.T) {
	type tc struct {
		name    string
		payload string
		valid   bool
	}
	cases := []tc{
		{
			name:    "open_omitted_id_and_epoch",
			payload: `{"command":"OPEN","session_id":"` + testSessionIDV2 + `"}`,
			valid:   true,
		},
		{
			name:    "open_empty_id_omitted_epoch",
			payload: `{"command":"OPEN","session_id":"` + testSessionIDV2 + `","connection_id":""}`,
			valid:   true,
		},
		{
			name:    "open_proposed_id_omitted_epoch",
			payload: `{"command":"OPEN","session_id":"` + testSessionIDV2 + `","connection_id":"conn-2"}`,
			valid:   true,
		},
		{
			name:    "open_proposed_id_epoch_zero",
			payload: `{"command":"OPEN","session_id":"` + testSessionIDV2 + `","connection_id":"conn-2","epoch":0}`,
			valid:   true,
		},
		{
			name:    "open_invalid_id",
			payload: `{"command":"OPEN","session_id":"` + testSessionIDV2 + `","connection_id":"conn-00"}`,
			valid:   false,
		},
		{
			name:    "restart_omitted_epoch",
			payload: `{"command":"RESTART","session_id":"` + testSessionIDV2 + `","connection_id":"conn-0"}`,
			valid:   true,
		},
		{
			name:    "restart_epoch_zero",
			payload: `{"command":"RESTART","session_id":"` + testSessionIDV2 + `","connection_id":"conn-0","epoch":0}`,
			valid:   true,
		},
		{
			name:    "restart_missing_id",
			payload: `{"command":"RESTART","session_id":"` + testSessionIDV2 + `"}`,
			valid:   false,
		},
		{
			name:    "ready_missing_id",
			payload: `{"command":"READY","session_id":"` + testSessionIDV2 + `","epoch":1}`,
			valid:   false,
		},
		{
			name:    "ready_epoch_zero",
			payload: `{"command":"READY","session_id":"` + testSessionIDV2 + `","connection_id":"conn-0","epoch":0}`,
			valid:   false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			body := `{"version":2,"request_id":"` + testRequestIDV2 + `","payload":` + c.payload + `}`
			dec, err := DecodeFrameV2(FrameKindConnectionCommandV2, []byte(body))
			if c.valid {
				if err != nil {
					t.Fatal(err)
				}
				if dec == nil || dec.Connection == nil {
					t.Fatalf("decoded=%v", dec)
				}
				return
			}
			requireInvalidRequestV2(t, err)
		})
	}
}

func TestV2Protocol_SignalSenderNodeKey(t *testing.T) {
	type tc struct {
		name string
		body string
		want bool
	}
	cases := []tc{
		{
			name: "command_omits_sender",
			body: `{"version":2,"request_id":"` + testRequestIDV2 + `","message_id":"` + testMessageIDV2 + `","payload":{"session_id":"` + testSessionIDV2 + `","connection_id":"conn-0","epoch":1,"seq":1,"type":"end_of_candidates","payload":{}}}`,
			want: true,
		},
		{
			name: "command_rejects_sender",
			body: `{"version":2,"request_id":"` + testRequestIDV2 + `","message_id":"` + testMessageIDV2 + `","payload":{"session_id":"` + testSessionIDV2 + `","connection_id":"conn-0","epoch":1,"seq":1,"type":"end_of_candidates","sender_node_key":"spoofed","payload":{}}}`,
			want: false,
		},
		{
			name: "event_requires_sender",
			body: `{"version":2,"message_id":"` + testMessageIDV2 + `","registration_id":"` + testRegIDV2 + `","payload":{"session_id":"` + testSessionIDV2 + `","connection_id":"conn-0","epoch":1,"seq":3,"type":"candidate","sender_node_key":"client-a","payload":{"candidate":"x"}}}`,
			want: true,
		},
		{
			name: "event_missing_sender",
			body: `{"version":2,"message_id":"` + testMessageIDV2 + `","registration_id":"` + testRegIDV2 + `","payload":{"session_id":"` + testSessionIDV2 + `","connection_id":"conn-0","epoch":1,"seq":3,"type":"candidate","payload":{"candidate":"x"}}}`,
			want: false,
		},
		{
			name: "event_invalid_sender",
			body: `{"version":2,"message_id":"` + testMessageIDV2 + `","registration_id":"` + testRegIDV2 + `","payload":{"session_id":"` + testSessionIDV2 + `","connection_id":"conn-0","epoch":1,"seq":3,"type":"candidate","sender_node_key":"bad.key","payload":{"candidate":"x"}}}`,
			want: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dec, err := DecodeFrameV2(FrameKindSignalSendV2, []byte(c.body))
			if c.want {
				if err != nil {
					t.Fatal(err)
				}
				if dec == nil || dec.SignalSend == nil {
					t.Fatalf("decoded=%v", dec)
				}
				return
			}
			requireInvalidRequestV2(t, err)
		})
	}
}

func TestV2Protocol_RequiredFieldsByKind(t *testing.T) {
	type tc struct {
		name string
		kind FrameKindV2
		body string
	}
	cases := []tc{
		{
			name: "state_command_missing_request_id",
			kind: FrameKindSessionCommandV2,
			body: `{"version":2,"payload":{"command":"BIND","session_id":"` + testSessionIDV2 + `","connection_id":"conn-0","epoch":1}}`,
		},
		{
			name: "signal_send_missing_request_id",
			kind: FrameKindSignalSendV2,
			body: `{"version":2,"message_id":"` + testMessageIDV2 + `","payload":{"session_id":"` + testSessionIDV2 + `","connection_id":"conn-0","epoch":1,"seq":1,"type":"end_of_candidates","payload":{}}}`,
		},
		{
			name: "signal_send_missing_message_id",
			kind: FrameKindSignalSendV2,
			body: `{"version":2,"request_id":"` + testRequestIDV2 + `","payload":{"session_id":"` + testSessionIDV2 + `","connection_id":"conn-0","epoch":1,"seq":1,"type":"end_of_candidates","payload":{}}}`,
		},
		{
			name: "signal_send_missing_seq",
			kind: FrameKindSignalSendV2,
			body: `{"version":2,"request_id":"` + testRequestIDV2 + `","message_id":"` + testMessageIDV2 + `","payload":{"session_id":"` + testSessionIDV2 + `","connection_id":"conn-0","epoch":1,"type":"end_of_candidates","payload":{}}}`,
		},
		{
			name: "signal_ack_missing_request_id",
			kind: FrameKindSignalAckV2,
			body: `{"version":2,"payload":{"session_id":"` + testSessionIDV2 + `","connection_id":"conn-0","epoch":1,"ack_seq":1}}`,
		},
		{
			name: "signal_ack_missing_ack_seq",
			kind: FrameKindSignalAckV2,
			body: `{"version":2,"request_id":"` + testRequestIDV2 + `","payload":{"session_id":"` + testSessionIDV2 + `","connection_id":"conn-0","epoch":1}}`,
		},
		{
			name: "event_missing_registration_id",
			kind: FrameKindStartV2,
			body: `{"version":2,"message_id":"` + testMessageIDV2 + `","payload":{"session_id":"` + testSessionIDV2 + `","connection_id":"conn-0","epoch":1,"revision":1,"sender_node_key":"coordinator"}}`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := DecodeFrameV2(c.kind, []byte(c.body))
			requireInvalidRequestV2(t, err)
		})
	}
}

func TestV2Protocol_ErrorEventDecodesWithoutRequestID(t *testing.T) {
	body := `{"version":2,"message_id":"` + testMessageIDV2 + `","registration_id":"` + testRegIDV2 + `","payload":{"error":"setup_timeout","session_id":"` + testSessionIDV2 + `","connection_id":"conn-0","revision":3}}`
	dec, err := DecodeFrameV2(FrameKindErrorV2, []byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if dec.Error == nil || dec.Error.Code != ErrSetupTimeoutV2 {
		t.Fatalf("event error=%+v", dec.Error)
	}
	if dec.Error.SessionID != testSessionIDV2 || dec.Error.ConnectionID != "conn-0" {
		t.Fatalf("event error identity %+v", dec.Error)
	}
	if dec.Error.Revision == nil || *dec.Error.Revision != 3 {
		t.Fatalf("event error revision=%v", dec.Error.Revision)
	}
}

func TestV2Protocol_SizeLimits(t *testing.T) {
	overFrame := bytes.Repeat([]byte("x"), MaxFrameBytesV2+1)
	_, err := DecodeFrameV2(FrameKindErrorV2, overFrame)
	requireInvalidRequestV2(t, err)

	prep, err := paddedPrepareFrameV2(MaxFrameBytesV2)
	if err != nil {
		t.Fatal(err)
	}
	if len(prep) != MaxFrameBytesV2 {
		t.Fatalf("padded frame %d", len(prep))
	}
	if _, err := DecodeFrameV2(FrameKindPrepareV2, prep); err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeFrameV2(FrameKindPrepareV2, append(prep, 'x')); err == nil {
		t.Fatal("expected oversize frame to fail")
	} else {
		requireInvalidRequestV2(t, err)
	}

	descOK := signalSendFrameV2(t, SignalKindDescriptionV2, strings.Repeat("d", MaxDescriptionBytesV2), "")
	if _, err := DecodeFrameV2(FrameKindSignalSendV2, descOK); err != nil {
		t.Fatal(err)
	}
	descOver := signalSendFrameV2(t, SignalKindDescriptionV2, strings.Repeat("d", MaxDescriptionBytesV2+1), "")
	_, err = DecodeFrameV2(FrameKindSignalSendV2, descOver)
	requireInvalidRequestV2(t, err)

	candOK := signalSendFrameV2(t, SignalKindCandidateV2, "", strings.Repeat("c", MaxCandidateBytesV2))
	if _, err := DecodeFrameV2(FrameKindSignalSendV2, candOK); err != nil {
		t.Fatal(err)
	}
	candOver := signalSendFrameV2(t, SignalKindCandidateV2, "", strings.Repeat("c", MaxCandidateBytesV2+1))
	_, err = DecodeFrameV2(FrameKindSignalSendV2, candOver)
	requireInvalidRequestV2(t, err)
}

func signalSendFrameV2(t *testing.T, kind SignalKindV2, sdp, candidate string) []byte {
	t.Helper()
	inner := map[string]any{}
	switch kind {
	case SignalKindDescriptionV2:
		inner["sdp"] = sdp
	case SignalKindCandidateV2:
		inner["candidate"] = candidate
	}
	env := map[string]any{
		"version":    ProtocolVersionV2,
		"request_id": testRequestIDV2,
		"message_id": testMessageIDV2,
		"payload": map[string]any{
			"session_id":    testSessionIDV2,
			"connection_id": testConnectionV2,
			"epoch":         1,
			"seq":           1,
			"type":          kind,
			"payload":       inner,
		},
	}
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func paddedPrepareFrameV2(target int) ([]byte, error) {
	makeFrame := func(pad int) []byte {
		env := map[string]any{
			"version":         ProtocolVersionV2,
			"message_id":      testMessageIDV2,
			"registration_id": testRegIDV2,
			"payload": map[string]any{
				"session_id":        testSessionIDV2,
				"connection_id":     testConnectionV2,
				"epoch":             1,
				"revision":          1,
				"role":              "client",
				"peer_node_key":     testServerNodeV2,
				"sender_node_key":   "coordinator",
				"stun_urls":         []string{"stun:" + strings.Repeat("x", pad)},
				"setup_deadline_ms": 15000,
				"protocol_version":  2,
				"feature_bits":      1,
			},
		}
		b, _ := json.Marshal(env)
		return b
	}
	probe := makeFrame(1)
	pad := target - len(probe) + 1
	if pad < 1 {
		return nil, errors.New("target too small")
	}
	got := makeFrame(pad)
	if len(got) != target {
		return nil, errors.New("unable to hit frame size")
	}
	return got, nil
}
