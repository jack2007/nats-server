package p2p

import (
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/nats-io/nats-server/v2/server"
)

const subjectDisconnect = "$SYS.ACCOUNT.*.DISCONNECT"

type Manager struct {
	s        *server.Server
	cfg      Config
	nc       *nats.Conn
	sysNC    *nats.Conn
	table    *Table
	secret   []byte
	now      func() time.Time
	serverID string
	peers    *peerTracker

	mu          sync.Mutex
	regMu       sync.Mutex
	clusterStop chan struct{}

	v2 *managerV2
}

func StartManager(s *server.Server, cfg Config) (*Manager, error) {
	if err := ValidateConfig(cfg); err != nil {
		return nil, err
	}
	opts := []nats.Option{nats.InProcessServer(s), nats.Name("p2p-manager"), nats.UseOldRequestStyle()}
	if cfg.AgentUsername != "" {
		opts = append(opts, nats.UserInfo(cfg.AgentUsername, cfg.AgentPassword))
	}
	nc, err := nats.Connect("", opts...)
	if err != nil {
		return nil, err
	}
	m := &Manager{
		s:        s,
		cfg:      cfg,
		nc:       nc,
		table:    NewTable(),
		now:      time.Now,
		serverID: s.ID(),
	}
	if cfg.SecretFile != "" {
		if secret, err := LoadSecret(cfg.SecretFile); err == nil {
			m.secret = secret
		}
	}
	if cfg.SysUsername != "" {
		sysNC, err := nats.Connect("",
			nats.InProcessServer(s),
			nats.Name("p2p-manager-sys"),
			nats.UserInfo(cfg.SysUsername, cfg.SysPassword),
		)
		if err != nil {
			nc.Close()
			return nil, err
		}
		m.sysNC = sysNC
		if err := sysNC.Flush(); err != nil {
			nc.Close()
			sysNC.Close()
			return nil, err
		}
	}
	if err := nc.Flush(); err != nil {
		if m.sysNC != nil {
			m.sysNC.Close()
		}
		nc.Close()
		return nil, err
	}
	if err := m.startCluster(); err != nil {
		m.stopCluster()
		if m.sysNC != nil {
			m.sysNC.Close()
		}
		nc.Close()
		return nil, err
	}
	if err := m.startV2(); err != nil {
		m.stopV2()
		m.stopCluster()
		if m.sysNC != nil {
			m.sysNC.Close()
		}
		nc.Close()
		return nil, err
	}
	return m, nil
}

func (m *Manager) Stop() {
	if m == nil {
		return
	}
	m.stopV2()
	m.stopCluster()
	if m.sysNC != nil {
		_ = m.sysNC.Drain()
	}
	if m.nc != nil {
		_ = m.nc.Drain()
	}
}

func (m *Manager) agentAccount() string {
	if m.cfg.AgentAccount != "" {
		return m.cfg.AgentAccount
	}
	return server.DEFAULT_GLOBAL_ACCOUNT
}
