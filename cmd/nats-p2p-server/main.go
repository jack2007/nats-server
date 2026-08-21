package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats-server/v2/p2p"
	"github.com/nats-io/nats-server/v2/server"
)

func main() {
	cfgPath := flag.String("c", "", "config file")
	flag.Parse()
	raw, err := os.ReadFile(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	natsRaw, pcfg, has, err := p2p.SplitP2PBlock(raw)
	if err != nil {
		log.Fatal(err)
	}
	tmp, err := os.CreateTemp("", "nats-p2p-*.conf")
	if err != nil {
		log.Fatal(err)
	}
	if _, err := tmp.Write(natsRaw); err != nil {
		log.Fatal(err)
	}
	tmp.Close()
	opts, err := server.ProcessConfigFile(tmp.Name())
	os.Remove(tmp.Name())
	if err != nil {
		log.Fatal(err)
	}
	s, err := server.NewServer(opts)
	if err != nil {
		log.Fatal(err)
	}
	s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		log.Fatal("nats-server not ready")
	}
	if has {
		if err := p2p.ValidateConfig(pcfg); err != nil {
			log.Fatal(err)
		}
		m, err := p2p.StartManager(s, pcfg)
		if err != nil {
			log.Fatal(err)
		}
		defer m.Stop()
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	s.Shutdown()
}
