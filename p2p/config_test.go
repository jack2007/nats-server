package p2p

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestValidateConfigRejectsLoopbackTURN(t *testing.T) {
	err := ValidateConfig(Config{
		STUNURLs: []string{"stun:turn.example.com:3478"},
		TURNURLs: []string{"turn:127.0.0.1:3478?transport=udp"},
	})
	if err == nil {
		t.Fatal("expected loopback TURN to fail")
	}
}

func TestValidateConfigRejectsTURNWithoutUDP(t *testing.T) {
	err := ValidateConfig(Config{
		TURNURLs: []string{"turn:turn.example.com:3478"},
	})
	if err == nil {
		t.Fatal("expected missing transport=udp to fail")
	}
}

func TestValidateConfigAcceptsPublicSTUNandUDPTURN(t *testing.T) {
	if err := ValidateConfig(Config{
		STUNURLs: []string{"stun:turn.example.com:3478"},
		TURNURLs: []string{"turn:turn.example.com:3478?transport=udp"},
	}); err != nil {
		t.Fatal(err)
	}
}

func TestLoadSecretReadsFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "secret")
	if err := os.WriteFile(path, []byte("abc"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := LoadSecret(path)
	if err != nil || string(got) != "abc" {
		t.Fatalf("got %q err=%v", got, err)
	}
}

func TestDefaultTTL(t *testing.T) {
	if DefaultCredentialTTL != 24*time.Hour {
		t.Fatalf("ttl=%v", DefaultCredentialTTL)
	}
	_ = fmt.Sprintf
}
