package p2p

import (
	"encoding/json"
	"errors"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/nats-io/nats-server/v2/server"
)

const (
	FeatureBitsV2       uint64 = 1
	RetryInitialV2             = 100 * time.Millisecond
	RetryMaxV2                 = time.Second
	v2CoordinatorQueue         = "$P2P.V2.COORDINATORS"
	v2CoordinatorSender        = "coordinator"
	v2DefaultCmdQueue          = 64
	v2DefaultWorkers           = 8
	v2MaxConnections           = 128
)

var (
	v2TestOnCommand func(string)
	v2TestQueueSize int
	v2TestWorkers   int
	v2TestBlock     func()
)

type v2NodeBinding struct {
	registrationID string
	epoch          uint64
	cid            uint64
}

type managerV2 struct {
	store     *StoreV2
	cmdQ      chan *nats.Msg
	stop      chan struct{}
	wg        sync.WaitGroup
	subs      []*nats.Subscription
	mu        sync.Mutex
	nodes     map[string]*v2NodeBinding
	sessionMu sync.Map
}

func RetryAfterMsV2(attempt int) int64 {
	if attempt < 0 {
		attempt = 0
	}
	if attempt > 30 {
		attempt = 30
	}
	d := RetryInitialV2 * time.Duration(1<<uint(attempt))
	if d > RetryMaxV2 {
		d = RetryMaxV2
	}
	span := d / 5
	if span > 0 {
		j := time.Duration(rand.Int64N(int64(span) + 1))
		d -= j / 2
	}
	if d < RetryInitialV2 {
		d = RetryInitialV2
	}
	if d > RetryMaxV2 {
		d = RetryMaxV2
	}
	return d.Milliseconds()
}

func ValidateTotalTimeoutMsV2(ms int64) (time.Duration, error) {
	if ms <= 0 {
		return 0, invalidRequestV2()
	}
	return time.Duration(ms) * time.Millisecond, nil
}

func (m *Manager) startV2() error {
	qsize := v2DefaultCmdQueue
	if v2TestQueueSize > 0 {
		qsize = v2TestQueueSize
	}
	workers := v2DefaultWorkers
	if v2TestWorkers > 0 {
		workers = v2TestWorkers
	}
	m.v2 = &managerV2{
		store: NewStoreV2(m.now, v2MaxConnections),
		cmdQ:  make(chan *nats.Msg, qsize),
		stop:  make(chan struct{}),
		nodes: make(map[string]*v2NodeBinding),
	}
	regSubj := "$P2P.V2.CMD.*.REGISTER." + m.serverID
	sub, err := m.nc.Subscribe(regSubj, m.handleV2Command)
	if err != nil {
		return err
	}
	m.v2.subs = append(m.v2.subs, sub)
	for _, suffix := range []string{
		"SESSION.CREATE",
		"SESSION.COMMAND",
		"CONNECTION.COMMAND",
		"SIGNAL.SEND",
		"SIGNAL.ACK",
	} {
		sub, err = m.nc.QueueSubscribe("$P2P.V2.CMD.*."+suffix, v2CoordinatorQueue, m.handleV2Command)
		if err != nil {
			return err
		}
		m.v2.subs = append(m.v2.subs, sub)
	}
	for i := 0; i < workers; i++ {
		m.v2.wg.Add(1)
		go m.v2Worker()
	}
	return m.nc.Flush()
}

func (m *Manager) stopV2() {
	if m == nil || m.v2 == nil {
		return
	}
	for _, sub := range m.v2.subs {
		_ = sub.Unsubscribe()
	}
	select {
	case <-m.v2.stop:
	default:
		close(m.v2.stop)
	}
	m.v2.wg.Wait()
}

func (m *Manager) handleV2Command(msg *nats.Msg) {
	if m.v2 == nil || msg == nil {
		return
	}
	sender, suffix, err := ParseCommandSubjectV2(msg.Subject)
	if v2TestOnCommand != nil {
		v2TestOnCommand(suffix)
	}
	if err != nil {
		m.replyV2Error(msg, requestIDFromData(msg.Data), ErrInvalidRequestV2, nil)
		return
	}
	if strings.HasPrefix(suffix, "REGISTER.") {
		m.handleRegisterV2(msg, sender, suffix)
		return
	}
	select {
	case m.v2.cmdQ <- msg:
	default:
		ms := RetryAfterMsV2(0)
		m.replyV2Error(msg, requestIDFromData(msg.Data), ErrBusyV2, &ms)
	}
}

func (m *Manager) v2Worker() {
	defer m.v2.wg.Done()
	for {
		select {
		case <-m.v2.stop:
			return
		case msg := <-m.v2.cmdQ:
			if v2TestBlock != nil {
				v2TestBlock()
			}
			select {
			case <-m.v2.stop:
				return
			default:
			}
			m.dispatchV2(msg)
		}
	}
}

func (m *Manager) dispatchV2(msg *nats.Msg) {
	sender, suffix, err := ParseCommandSubjectV2(msg.Subject)
	if err != nil {
		m.replyV2Error(msg, requestIDFromData(msg.Data), ErrInvalidRequestV2, nil)
		return
	}
	switch suffix {
	case "SESSION.CREATE":
		m.handleCreateV2(msg, sender)
	case "SESSION.COMMAND":
		m.handleSessionV2(msg, sender)
	case "CONNECTION.COMMAND":
		m.handleConnectionV2(msg, sender)
	case "SIGNAL.SEND", "SIGNAL.ACK":
		m.replyV2Error(msg, requestIDFromData(msg.Data), ErrInvalidStateV2, nil)
	default:
		m.replyV2Error(msg, requestIDFromData(msg.Data), ErrInvalidRequestV2, nil)
	}
}

func (m *Manager) handleRegisterV2(msg *nats.Msg, sender, suffix string) {
	if suffix != "REGISTER."+m.serverID {
		m.replyV2Error(msg, requestIDFromData(msg.Data), ErrInvalidRequestV2, nil)
		return
	}
	dec, err := DecodeFrameV2(FrameKindRegisterV2, msg.Data)
	if err != nil {
		m.replyV2Error(msg, requestIDFromData(msg.Data), protocolCodeV2(err), nil)
		return
	}
	cmd := *dec.Register
	cmd.NodeKey = sender
	cids, err := m.lookupNamedAgentConns(sender)
	if err != nil || len(cids) != 1 {
		ms := RetryAfterMsV2(0)
		m.replyV2Error(msg, cmd.RequestID, ErrBusyV2, &ms)
		return
	}
	cid := cids[0]
	m.v2.mu.Lock()
	if existing, ok := m.v2.nodes[sender]; ok {
		if existing.registrationID == cmd.RegistrationID && existing.cid != cid {
			m.v2.mu.Unlock()
			m.replyV2Error(msg, cmd.RequestID, ErrInvalidRequestV2, nil)
			return
		}
	}
	m.v2.mu.Unlock()
	snap, err := m.v2.store.RegisterNode(cmd)
	if err != nil {
		m.replyV2Error(msg, cmd.RequestID, protocolCodeV2(err), nil)
		return
	}
	m.v2.mu.Lock()
	m.v2.nodes[sender] = &v2NodeBinding{
		registrationID: snap.RegistrationID,
		epoch:          snap.RegistrationEpoch,
		cid:            cid,
	}
	m.v2.mu.Unlock()
	body, err := encodeEnvelopeV2(cmd.RequestID, "", snap.RegistrationID, map[string]any{
		"registration_epoch": snap.RegistrationEpoch,
	})
	if err != nil {
		m.replyV2Error(msg, cmd.RequestID, ErrInternalErrorV2, nil)
		return
	}
	_ = msg.Respond(body)
}

func (m *Manager) handleCreateV2(msg *nats.Msg, sender string) {
	dec, err := DecodeFrameV2(FrameKindCreateV2, msg.Data)
	if err != nil {
		m.replyV2Error(msg, requestIDFromData(msg.Data), protocolCodeV2(err), nil)
		return
	}
	if dec.Create.TotalTimeoutMs != 0 {
		if _, err := ValidateTotalTimeoutMsV2(dec.Create.TotalTimeoutMs); err != nil {
			m.replyV2Error(msg, dec.Create.RequestID, ErrInvalidRequestV2, nil)
			return
		}
	}
	snap, err := m.v2.store.AllocateSession(sender, *dec.Create)
	if err != nil {
		m.replyV2Error(msg, dec.Create.RequestID, protocolCodeV2(err), nil)
		return
	}
	body, err := encodeEnvelopeV2(dec.Create.RequestID, "", "", map[string]any{
		"ok":              true,
		"session_id":      snap.SessionID,
		"connection_id":   snap.ConnectionID,
		"epoch":           snap.Epoch,
		"state":           snap.State,
		"revision":        snap.Revision,
		"owner_server_id": m.serverID,
	})
	if err != nil {
		m.replyV2Error(msg, dec.Create.RequestID, ErrInternalErrorV2, nil)
		return
	}
	_ = msg.Respond(body)
}

func (m *Manager) handleSessionV2(msg *nats.Msg, sender string) {
	dec, err := DecodeFrameV2(FrameKindSessionCommandV2, msg.Data)
	if err != nil {
		m.replyV2Error(msg, requestIDFromData(msg.Data), protocolCodeV2(err), nil)
		return
	}
	cmd := *dec.Session
	unlock := m.lockSessionV2(cmd.SessionID)
	defer unlock()
	events, err := m.v2.store.BindSession(sender, cmd)
	if err != nil {
		if m.replyIfClosedV2(msg, cmd.RequestID, cmd.IdentityV2) {
			return
		}
		m.replyV2Error(msg, cmd.RequestID, protocolCodeV2(err), nil)
		return
	}
	if err := m.publishEventsV2(events); err != nil {
		m.replyV2Error(msg, cmd.RequestID, ErrInternalErrorV2, nil)
		return
	}
	m.replyV2OK(msg, cmd.RequestID)
}

func (m *Manager) handleConnectionV2(msg *nats.Msg, sender string) {
	dec, err := DecodeFrameV2(FrameKindConnectionCommandV2, msg.Data)
	if err != nil {
		m.replyV2Error(msg, requestIDFromData(msg.Data), protocolCodeV2(err), nil)
		return
	}
	cmd := *dec.Connection
	unlock := m.lockSessionV2(cmd.SessionID)
	defer unlock()
	switch cmd.Command {
	case ConnectionCommandOpenV2:
		snap, err := m.v2.store.AllocateConnection(sender, cmd)
		if err != nil {
			m.replyConnectionAllocErrV2(msg, cmd, err)
			return
		}
		m.replyAllocatedV2(msg, cmd.RequestID, snap)
	case ConnectionCommandRestartV2:
		snap, err := m.v2.store.AllocateRestart(sender, cmd)
		if err != nil {
			m.replyConnectionAllocErrV2(msg, cmd, err)
			return
		}
		m.replyAllocatedV2(msg, cmd.RequestID, snap)
	case ConnectionCommandBindV2:
		events, err := m.v2.store.BindConnection(sender, cmd)
		if err != nil {
			if m.replyIfClosedV2(msg, cmd.RequestID, cmd.IdentityV2) {
				return
			}
			m.replyV2Error(msg, cmd.RequestID, protocolCodeV2(err), nil)
			return
		}
		if err := m.publishEventsV2(events); err != nil {
			m.replyV2Error(msg, cmd.RequestID, ErrInternalErrorV2, nil)
			return
		}
		m.replyV2OK(msg, cmd.RequestID)
	case ConnectionCommandReadyV2:
		events, err := m.v2.store.MarkConnectionReady(sender, cmd)
		if err != nil {
			if m.replyIfClosedV2(msg, cmd.RequestID, cmd.IdentityV2) {
				return
			}
			m.replyV2Error(msg, cmd.RequestID, protocolCodeV2(err), nil)
			return
		}
		if err := m.publishEventsV2(events); err != nil {
			m.replyV2Error(msg, cmd.RequestID, ErrInternalErrorV2, nil)
			return
		}
		m.replyV2OK(msg, cmd.RequestID)
	case ConnectionCommandCloseV2:
		events, err := m.v2.store.CloseConnection(sender, cmd)
		if err != nil {
			if m.replyIfClosedV2(msg, cmd.RequestID, cmd.IdentityV2) {
				return
			}
			m.replyV2Error(msg, cmd.RequestID, protocolCodeV2(err), nil)
			return
		}
		if err := m.publishEventsV2(events); err != nil {
			m.replyV2Error(msg, cmd.RequestID, ErrInternalErrorV2, nil)
			return
		}
		m.replyV2OK(msg, cmd.RequestID)
	default:
		m.replyV2Error(msg, cmd.RequestID, ErrInvalidStateV2, nil)
	}
}

func (m *Manager) publishEventsV2(events []EventV2) error {
	var prepares []EventV2
	var others []EventV2
	for _, ev := range events {
		if ev.Kind == FrameKindPrepareV2 {
			prepares = append(prepares, ev)
		} else {
			others = append(others, ev)
		}
	}
	if err := m.publishPrepareV2(prepares); err != nil {
		return err
	}
	for _, ev := range others {
		if err := m.publishV2Event(ev.TargetNodeKey, ev); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) publishPrepareV2(events []EventV2) error {
	if len(events) == 0 {
		return nil
	}
	var turn *TurnCredV2
	if len(events) > 0 {
		turn = m.issueTurnV2At(events[0].Identity, m.now())
	}
	for _, ev := range events {
		if ev.Prepare != nil {
			cp := *ev.Prepare
			cp.Turn = turn
			ev.Prepare = &cp
		}
		if err := m.publishV2Event(ev.TargetNodeKey, ev); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) publishV2Event(nodeKey string, event EventV2) error {
	reg := event.RegistrationID
	m.v2.mu.Lock()
	if b, ok := m.v2.nodes[nodeKey]; ok && b.registrationID != "" {
		reg = b.registrationID
	}
	m.v2.mu.Unlock()
	if event.TargetNodeKey != "" {
		nodeKey = event.TargetNodeKey
	}
	subj, err := EventSubjectV2(nodeKey, reg)
	if err != nil {
		return err
	}
	var body []byte
	switch event.Kind {
	case FrameKindPrepareV2:
		prep := event.Prepare
		if prep == nil {
			return errors.New("missing prepare")
		}
		payload := map[string]any{
			"session_id":        prep.SessionID,
			"connection_id":     prep.ConnectionID,
			"epoch":             prep.Epoch,
			"revision":          prep.Revision,
			"role":              prep.Role,
			"peer_node_key":     prep.PeerNodeKey,
			"sender_node_key":   v2CoordinatorSender,
			"stun_urls":         append([]string(nil), m.cfg.STUNURLs...),
			"setup_deadline_ms": deadlineMsV2(prep.SetupDeadlineMs),
			"protocol_version":  ProtocolVersionV2,
			"feature_bits":      FeatureBitsV2,
		}
		if event.Prepare.Turn != nil {
			payload["turn"] = event.Prepare.Turn
		}
		body, err = encodeEnvelopeV2("", event.MessageID, reg, payload)
	case FrameKindStartV2:
		start := event.Start
		if start == nil {
			start = &StartEventV2{
				MessageID:     event.MessageID,
				IdentityV2:    event.Identity,
				Revision:      event.Revision,
				SenderNodeKey: v2CoordinatorSender,
			}
		}
		payload := map[string]any{
			"session_id":      start.SessionID,
			"connection_id":   start.ConnectionID,
			"epoch":           start.Epoch,
			"revision":        start.Revision,
			"sender_node_key": v2CoordinatorSender,
		}
		body, err = encodeEnvelopeV2("", event.MessageID, reg, payload)
	case FrameKindCloseV2:
		payload := map[string]any{
			"session_id":      event.Identity.SessionID,
			"connection_id":   event.Identity.ConnectionID,
			"epoch":           event.Identity.Epoch,
			"revision":        event.Revision,
			"state":           string(SessionStateClosedV2),
			"sender_node_key": v2CoordinatorSender,
		}
		body, err = encodeEnvelopeV2("", event.MessageID, reg, payload)
	default:
		payload := map[string]any{
			"session_id":    event.Identity.SessionID,
			"connection_id": event.Identity.ConnectionID,
			"epoch":         event.Identity.Epoch,
			"revision":      event.Revision,
		}
		body, err = encodeEnvelopeV2("", event.MessageID, reg, payload)
	}
	if err != nil {
		return err
	}
	return m.nc.Publish(subj, body)
}

func deadlineMsV2(ms int64) int64 {
	if ms > 0 {
		return ms
	}
	return DefaultSetupTimeoutV2.Milliseconds()
}

func (m *Manager) issueTurnV2At(id IdentityV2, now time.Time) *TurnCredV2 {
	if len(m.secret) == 0 || len(m.cfg.TURNURLs) == 0 {
		return nil
	}
	ttl := m.cfg.CredentialTTL
	if ttl <= 0 {
		ttl = DefaultCredentialTTL
	}
	ttlSec := int64(ttl / time.Second)
	user, pass := IssueREST(m.secret, id.SessionID, id.ConnectionID, id.Epoch, now.Unix(), ttlSec)
	return &TurnCredV2{
		URLs:      append([]string(nil), m.cfg.TURNURLs...),
		Username:  user,
		Password:  pass,
		ExpiresAt: now.Unix() + ttlSec,
	}
}

func (m *Manager) lookupNamedAgentConns(name string) ([]uint64, error) {
	account := m.agentAccount()
	var cids []uint64
	offset := 0
	for {
		limit := server.DefaultConnListSize
		cz, err := m.s.Connz(&server.ConnzOptions{Offset: offset, Limit: limit, Account: account})
		if err != nil {
			return nil, err
		}
		if cz.Total > 0 && cz.Total > len(cz.Conns) && offset == 0 {
			cz, err = m.s.Connz(&server.ConnzOptions{Offset: 0, Limit: cz.Total, Account: account})
			if err != nil {
				return nil, err
			}
			cids = cids[:0]
			for _, c := range cz.Conns {
				if c.Name == name {
					cids = append(cids, c.Cid)
				}
			}
			return cids, nil
		}
		for _, c := range cz.Conns {
			if c.Name == name {
				cids = append(cids, c.Cid)
			}
		}
		if len(cz.Conns) == 0 || offset+len(cz.Conns) >= cz.Total {
			return cids, nil
		}
		offset += len(cz.Conns)
	}
}

func (m *Manager) lockSessionV2(sessionID string) func() {
	v, _ := m.v2.sessionMu.LoadOrStore(sessionID, &sync.Mutex{})
	mu := v.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

func (m *Manager) replyAllocatedV2(msg *nats.Msg, requestID string, snap ConnectionSnapshotV2) {
	body, err := encodeEnvelopeV2(requestID, "", "", map[string]any{
		"ok":              true,
		"session_id":      snap.SessionID,
		"connection_id":   snap.ConnectionID,
		"epoch":           snap.Epoch,
		"state":           snap.State,
		"revision":        snap.Revision,
		"owner_server_id": m.serverID,
	})
	if err != nil {
		m.replyV2Error(msg, requestID, ErrInternalErrorV2, nil)
		return
	}
	_ = msg.Respond(body)
}

func (m *Manager) replyConnectionAllocErrV2(msg *nats.Msg, cmd ConnectionCommandV2, err error) {
	if protocolCodeV2(err) == ErrConnectionLimitV2 {
		m.replyV2ErrorDetail(msg, cmd.RequestID, ErrConnectionLimitV2, map[string]any{
			"session_id":    cmd.SessionID,
			"connection_id": cmd.ConnectionID,
			"limit":         v2MaxConnections,
		})
		return
	}
	if m.replyIfClosedV2(msg, cmd.RequestID, cmd.IdentityV2) {
		return
	}
	m.replyV2Error(msg, cmd.RequestID, protocolCodeV2(err), nil)
}

func (m *Manager) replyIfClosedV2(msg *nats.Msg, requestID string, id IdentityV2) bool {
	snap, ok := m.v2.store.PeekSession(id.SessionID)
	if !ok || snap.State != SessionStateClosedV2 {
		return false
	}
	connID := id.ConnectionID
	if connID == "" {
		connID = snap.ConnectionID
	}
	epoch := id.Epoch
	if epoch == 0 {
		epoch = snap.Epoch
	}
	m.replyAllocatedV2(msg, requestID, ConnectionSnapshotV2{
		SessionID:    snap.SessionID,
		ConnectionID: connID,
		Epoch:        epoch,
		State:        ConnectionStateClosedV2,
		Revision:     snap.Revision,
	})
	return true
}

func (m *Manager) replyV2ErrorDetail(msg *nats.Msg, requestID string, code ErrorCodeV2, extra map[string]any) {
	if requestID == "" {
		requestID = requestIDFromData(msg.Data)
	}
	if ValidateUUIDV2(requestID) != nil {
		requestID = "00000000-0000-4000-8000-000000000001"
	}
	payload := map[string]any{"error": code}
	for k, v := range extra {
		payload[k] = v
	}
	body, err := encodeEnvelopeV2(requestID, "", "", payload)
	if err != nil {
		return
	}
	_ = msg.Respond(body)
}

func (m *Manager) replyV2OK(msg *nats.Msg, requestID string) {
	body, err := encodeEnvelopeV2(requestID, "", "", map[string]any{"ok": true})
	if err != nil {
		return
	}
	_ = msg.Respond(body)
}

func (m *Manager) replyV2Error(msg *nats.Msg, requestID string, code ErrorCodeV2, retryAfterMs *int64) {
	if requestID == "" {
		requestID = requestIDFromData(msg.Data)
	}
	if ValidateUUIDV2(requestID) != nil {
		requestID = "00000000-0000-4000-8000-000000000001"
	}
	body, err := encodeEnvelopeV2(requestID, "", "", ErrorPayloadV2{
		Code:         code,
		RetryAfterMs: retryAfterMs,
	})
	if err != nil {
		return
	}
	_ = msg.Respond(body)
}

func encodeEnvelopeV2(requestID, messageID, registrationID string, payload any) ([]byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return json.Marshal(EnvelopeV2{
		Version:        ProtocolVersionV2,
		RequestID:      requestID,
		MessageID:      messageID,
		RegistrationID: registrationID,
		Payload:        raw,
	})
}

func requestIDFromData(data []byte) string {
	var env EnvelopeV2
	if json.Unmarshal(data, &env) != nil {
		return ""
	}
	return env.RequestID
}

func protocolCodeV2(err error) ErrorCodeV2 {
	var pe *ProtocolErrorV2
	if errors.As(err, &pe) && pe.Code != "" {
		return pe.Code
	}
	return ErrInternalErrorV2
}
