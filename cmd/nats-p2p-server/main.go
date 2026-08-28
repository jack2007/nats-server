package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats-server/v2/p2p"
	"github.com/nats-io/nats-server/v2/server"
)

func main() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	if err := run(os.Args[1:], ch); err != nil {
		log.Fatal(err)
	}
}

func run(args []string, stop <-chan os.Signal) error {
	fs := flag.NewFlagSet("nats-p2p-server", flag.ContinueOnError)
	cfgPath := fs.String("c", "", "config file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	raw, err := os.ReadFile(*cfgPath)
	if err != nil {
		return err
	}
	natsRaw, pcfg, _, err := p2p.SplitP2PBlock(raw)
	if err != nil {
		return err
	}
	if err := p2p.ValidateConfig(pcfg); err != nil {
		return err
	}
	tmp, err := os.CreateTemp("", "nats-p2p-*.conf")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(natsRaw); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	tmp.Close()
	opts, err := server.ProcessConfigFile(tmp.Name())
	os.Remove(tmp.Name())
	if err != nil {
		return err
	}
	p2p.ApplyClientPingDefaults(opts, natsRaw)
	s, err := server.NewServer(opts)
	if err != nil {
		return err
	}
	s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		s.Shutdown()
		return fmt.Errorf("nats-server not ready")
	}
	var auth *p2p.AuthCalloutService
	if opts.AuthCallout != nil {
		if err := p2p.CheckIssuer(opts.AuthCallout.Issuer); err != nil {
			s.Shutdown()
			return err
		}
		user, pass, err := p2p.AuthInternalCredentials(opts)
		if err != nil {
			s.Shutdown()
			return err
		}
		auth, err = p2p.StartAuthCallout(s, user, pass)
		if err != nil {
			s.Shutdown()
			return err
		}
	}
	m, err := p2p.StartManager(s, pcfg)
	if err != nil {
		if auth != nil {
			auth.Stop()
		}
		s.Shutdown()
		return err
	}
	<-stop
	m.Stop()
	if auth != nil {
		auth.Stop()
	}
	s.Shutdown()
	return nil
}
