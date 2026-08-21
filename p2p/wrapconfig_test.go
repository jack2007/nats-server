package p2p

import (
	"bytes"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

func TestSplitP2PBlock(t *testing.T) {
	src := []byte(`
port: 4222
p2p {
  stun_urls: ["stun:turn.example.com:3478"]
  turn_urls: ["turn:turn.example.com:3478?transport=udp"]
  secret_file: "/etc/nats/coturn-rest-secret"
  credential_ttl: 24h
}
`)
	natsConf, cfg, has, err := SplitP2PBlock(src)
	if err != nil || !has {
		t.Fatal(err, has)
	}
	if bytes.Contains(natsConf, []byte("p2p")) {
		t.Fatalf("p2p leaked: %s", natsConf)
	}
	if err := ValidateConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.SecretFile != "/etc/nats/coturn-rest-secret" {
		t.Fatal(cfg.SecretFile)
	}
}

func TestSplitP2PBlockAbsent(t *testing.T) {
	_, _, has, err := SplitP2PBlock([]byte(`port: 4222`))
	if err != nil || has {
		t.Fatalf("has=%v err=%v", has, err)
	}
}

func TestSplitP2PBlockLoopbackFailsValidate(t *testing.T) {
	_, cfg, has, err := SplitP2PBlock([]byte(`
p2p { turn_urls: ["turn:127.0.0.1:3478?transport=udp"] }
`))
	if err != nil || !has {
		t.Fatal(err, has)
	}
	if err := ValidateConfig(cfg); err == nil {
		t.Fatal("loopback must fail validate")
	}
}

func TestApplyClientPingDefaultsWhenOmitted(t *testing.T) {
	opts := &server.Options{
		PingInterval: 2 * time.Minute,
		MaxPingsOut:  2,
	}
	ApplyClientPingDefaults(opts, []byte("port: 4222\n"))
	if opts.PingInterval != 5*time.Second || opts.MaxPingsOut != 3 {
		t.Fatalf("got interval=%v max=%d", opts.PingInterval, opts.MaxPingsOut)
	}
}

func TestApplyClientPingDefaultsRespectsExplicit(t *testing.T) {
	opts := &server.Options{
		PingInterval: 20 * time.Second,
		MaxPingsOut:  8,
	}
	ApplyClientPingDefaults(opts, []byte(`
port: 4222
ping_interval: "20s"
ping_max: 8
`))
	if opts.PingInterval != 20*time.Second || opts.MaxPingsOut != 8 {
		t.Fatalf("explicit keys must keep ProcessConfigFile values, got interval=%v max=%d",
			opts.PingInterval, opts.MaxPingsOut)
	}
}
