package p2p

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/nats-io/nats.go"
)

const subjectStatsZV2Fmt = "$SYS.REQ.SERVER.%s.P2P.V2.STATSZ"

func statszSubjectV2(serverID string) string {
	return fmt.Sprintf(subjectStatsZV2Fmt, serverID)
}

type labeledCountV2 struct {
	State     string `json:"state,omitempty"`
	Role      string `json:"role,omitempty"`
	Result    string `json:"result,omitempty"`
	Type      string `json:"type,omitempty"`
	Direction string `json:"direction,omitempty"`
	Value     uint64 `json:"value"`
}

// StatsSnapshotV2 is the aggregate Coordinator view published on STATSZ.
// It contains counters and gauges only — never node/session identifiers or secrets.
type StatsSnapshotV2 struct {
	ServerID                string           `json:"server_id"`
	Sessions                []labeledCountV2 `json:"p2p_sessions"`
	Connections             []labeledCountV2 `json:"p2p_connections"`
	SessionCreateTotal      []labeledCountV2 `json:"p2p_session_create_total"`
	SignalTotal             []labeledCountV2 `json:"p2p_signal_total"`
	SignalRetryTotal        uint64           `json:"p2p_signal_retry_total"`
	SignalDeduplicatedTotal uint64           `json:"p2p_signal_deduplicated_total"`
	ConnectionLimitTotal    uint64           `json:"p2p_connection_limit_total"`
	CommandQueueDepth       int              `json:"p2p_command_queue_depth"`
	OwnerSessions           uint64           `json:"p2p_owner_sessions"`
	ReplicaSessions         uint64           `json:"p2p_replica_sessions"`
	RevisionSyncTotal       []labeledCountV2 `json:"p2p_revision_sync_total"`
}

type v2Stats struct {
	createOk    atomic.Uint64
	createBusy  atomic.Uint64
	createError atomic.Uint64
	signalRetry atomic.Uint64
	signalDedup atomic.Uint64
	connLimit   atomic.Uint64
	revOk       atomic.Uint64
	revStale    atomic.Uint64

	signalMu sync.Mutex
	signals  map[string]uint64

	connMu sync.Mutex
	conns  map[string]int64
}

func (st *v2Stats) noteSignal(typ, dir, result string) {
	if st == nil {
		return
	}
	key := typ + "|" + dir + "|" + result
	st.signalMu.Lock()
	if st.signals == nil {
		st.signals = map[string]uint64{}
	}
	st.signals[key]++
	st.signalMu.Unlock()
}

func (st *v2Stats) snapshotSignals() []labeledCountV2 {
	if st == nil {
		return []labeledCountV2{}
	}
	st.signalMu.Lock()
	defer st.signalMu.Unlock()
	out := make([]labeledCountV2, 0, len(st.signals))
	for key, value := range st.signals {
		parts := split3(key)
		out = append(out, labeledCountV2{
			Type:      parts[0],
			Direction: parts[1],
			Result:    parts[2],
			Value:     value,
		})
	}
	return out
}

func (st *v2Stats) noteConn(state, role string, delta int64) {
	if st == nil {
		return
	}
	key := state + "|" + role
	st.connMu.Lock()
	if st.conns == nil {
		st.conns = map[string]int64{}
	}
	st.conns[key] += delta
	if st.conns[key] < 0 {
		st.conns[key] = 0
	}
	st.connMu.Unlock()
}

func (st *v2Stats) snapshotConns() []labeledCountV2 {
	if st == nil {
		return []labeledCountV2{}
	}
	st.connMu.Lock()
	defer st.connMu.Unlock()
	out := make([]labeledCountV2, 0, len(st.conns))
	for key, value := range st.conns {
		if value <= 0 {
			continue
		}
		state, role, _ := split2(key)
		out = append(out, labeledCountV2{State: state, Role: role, Value: uint64(value)})
	}
	return out
}

func split2(key string) (string, string, bool) {
	for i := 0; i < len(key); i++ {
		if key[i] == '|' {
			return key[:i], key[i+1:], true
		}
	}
	return key, "", false
}

func split3(key string) [3]string {
	var out [3]string
	start := 0
	n := 0
	for i := 0; i <= len(key) && n < 3; i++ {
		if i == len(key) || key[i] == '|' {
			out[n] = key[start:i]
			n++
			start = i + 1
		}
	}
	return out
}

func (m *Manager) startMonitorV2() error {
	if m == nil || m.v2 == nil || m.sysNC == nil {
		return nil
	}
	subj := statszSubjectV2(m.serverID)
	sub, err := m.sysNC.Subscribe(subj, m.handleStatsZV2)
	if err != nil {
		return err
	}
	m.v2.subs = append(m.v2.subs, sub)
	return m.sysNC.Flush()
}

func (m *Manager) handleStatsZV2(msg *nats.Msg) {
	if msg == nil {
		return
	}
	snap := m.SnapshotV2()
	body, err := json.Marshal(snap)
	if err != nil {
		return
	}
	_ = msg.Respond(body)
}

// SnapshotV2 copies atomic counters and lightweight owner aggregates.
// It does not walk or serialize Store sessions and does not hold the Store lock.
func (m *Manager) SnapshotV2() StatsSnapshotV2 {
	snap := StatsSnapshotV2{
		Sessions:          []labeledCountV2{},
		Connections:       []labeledCountV2{},
		SessionCreateTotal: []labeledCountV2{
			{Result: "ok", Value: 0},
			{Result: "busy", Value: 0},
			{Result: "error", Value: 0},
		},
		SignalTotal:       []labeledCountV2{},
		RevisionSyncTotal: []labeledCountV2{{Result: "ok", Value: 0}, {Result: "stale", Value: 0}},
	}
	if m == nil {
		return snap
	}
	snap.ServerID = m.serverID
	if m.v2 == nil {
		return snap
	}
	st := &m.v2.stats
	snap.SessionCreateTotal = []labeledCountV2{
		{Result: "ok", Value: st.createOk.Load()},
		{Result: "busy", Value: st.createBusy.Load()},
		{Result: "error", Value: st.createError.Load()},
	}
	snap.SignalTotal = st.snapshotSignals()
	if snap.SignalTotal == nil {
		snap.SignalTotal = []labeledCountV2{}
	}
	snap.SignalRetryTotal = st.signalRetry.Load()
	snap.SignalDeduplicatedTotal = st.signalDedup.Load()
	snap.ConnectionLimitTotal = st.connLimit.Load()
	snap.CommandQueueDepth = len(m.v2.cmdQ)
	snap.Connections = st.snapshotConns()
	if snap.Connections == nil {
		snap.Connections = []labeledCountV2{}
	}
	snap.RevisionSyncTotal = []labeledCountV2{
		{Result: "ok", Value: st.revOk.Load()},
		{Result: "stale", Value: st.revStale.Load()},
	}

	type sessKey struct{ state, role string }
	gauges := map[sessKey]uint64{}
	var owners, replicas uint64
	m.v2.mu.Lock()
	for _, meta := range m.v2.owners {
		if meta == nil {
			continue
		}
		role := "replica"
		if meta.Owner == m.serverID {
			role = "owner"
			owners++
		} else {
			replicas++
		}
		state := string(meta.State)
		if state == "" {
			state = string(SessionStateAllocatedV2)
		}
		gauges[sessKey{state, role}]++
	}
	m.v2.mu.Unlock()
	snap.OwnerSessions = owners
	snap.ReplicaSessions = replicas
	for key, value := range gauges {
		snap.Sessions = append(snap.Sessions, labeledCountV2{
			State: key.state,
			Role:  key.role,
			Value: value,
		})
	}
	return snap
}

func (m *Manager) noteCreateV2(result string) {
	if m == nil || m.v2 == nil {
		return
	}
	switch result {
	case "ok":
		m.v2.stats.createOk.Add(1)
	case "busy":
		m.v2.stats.createBusy.Add(1)
	default:
		m.v2.stats.createError.Add(1)
	}
}

func (m *Manager) noteRevisionSyncV2(err error) {
	if m == nil || m.v2 == nil {
		return
	}
	if err != nil {
		m.v2.stats.revStale.Add(1)
		return
	}
	m.v2.stats.revOk.Add(1)
}
