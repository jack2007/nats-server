package p2p

import (
	"encoding/json"
	"testing"
)

func TestValidNodeKey(t *testing.T) {
	if ValidNodeKey("bad.key") || ValidNodeKey("") || ValidNodeKey("has*") {
		t.Fatal("invalid keys accepted")
	}
	if !ValidNodeKey("agent-self_1") {
		t.Fatal("valid key rejected")
	}
}

func TestEncodeErrorJSON(t *testing.T) {
	var m map[string]any
	if err := json.Unmarshal(EncodeError(ErrNodeKeyInUse), &m); err != nil {
		t.Fatal(err)
	}
	if m["ok"] != false || m["error"] != "node_key_in_use" {
		t.Fatalf("%v", m)
	}
}

func TestEncodeInviteOmitsGrantAndOptionalTurn(t *testing.T) {
	raw, err := EncodeInvite("sid", "conn-0", 1, []string{"stun:turn.example.com:3478"}, nil, "$P2P.ICE.sid")
	if err != nil {
		t.Fatal(err)
	}
	var frame map[string]any
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatal(err)
	}
	if frame["type"] != "invite" {
		t.Fatalf("type=%v", frame["type"])
	}
	payload := frame["payload"].(map[string]any)
	if _, ok := payload["grant"]; ok {
		t.Fatal("grant must be absent")
	}
	if _, ok := payload["turn"]; ok {
		t.Fatal("turn must be omitted")
	}
	if payload["ice_subject"] != "$P2P.ICE.sid" {
		t.Fatalf("ice_subject=%v", payload["ice_subject"])
	}
}
