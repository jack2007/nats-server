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
	v2TestOnCommand        func(string)
	v2TestQueueSize        int
	v2TestWorkers          int
	v2TestBlock            func()
	v2TestPublishHook      func(subj string, data []byte) error
	v2TestHoldJoinQueue    <-chan struct{}
	v2TestHoldRegisterSync <-chan struct{}
	v2TestHoldRegisterNode string
)

type v2NodeBinding struct {
	registrationID string
	epoch          uint64
	cid            uint64
	natsServerID   string
	requestID      string
	pending        bool
}

type signalRetryItemV2 struct {
	dir       DirectionKeyV2
	seq       uint64
	messageID string
	target    string
	attempt   int
	next      time.Time
}

type managerV2 struct {
	store           *StoreV2
	cmdQ            chan *nats.Msg
	stop            chan struct{}
	wg              sync.WaitGroup
	subs            []*nats.Subscription
	cmdSubs         []*nats.Subscription
	mgrSubs         []*nats.Subscription
	mu              sync.Mutex
	nodes           map[string]*v2NodeBinding
	owners          map[string]*v2SessionMeta
	requests        map[RequestKeyV2]string
	applied         map[string]struct{}
	registerSyncing int
	joinedQueue     bool
	unreachable     bool
	sessionMu       sync.Map
	retryMu         sync.Mutex
	retries         map[MessageKeyV2]signalRetryItemV2
	retryWake       chan struct{}
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
		store:     NewStoreV2(m.now, v2MaxConnections),
		cmdQ:      make(chan *nats.Msg, qsize),
		stop:      make(chan struct{}),
		nodes:     make(map[string]*v2NodeBinding),
		owners:    make(map[string]*v2SessionMeta),
		requests:  make(map[RequestKeyV2]string),
		applied:   make(map[string]struct{}),
		retries:   make(map[MessageKeyV2]signalRetryItemV2),
		retryWake: make(chan struct{}, 1),
	}
	regSubj := "$P2P.V2.CMD.*.REGISTER." + m.serverID
	sub, err := m.nc.Subscribe(regSubj, m.handleV2Command)
	if err != nil {
		return err
	}
	m.v2.subs = append(m.v2.subs, sub)
	if err := m.startClusterV2(); err != nil {
		return err
	}
	if err := m.catchUpV2(); err != nil {
		return err
	}
	for i := 0; i < workers; i++ {
		m.v2.wg.Add(1)
		go m.v2Worker()
	}
	m.v2.wg.Add(1)
	go m.signalRetryLoopV2()
	if v2TestHoldJoinQueue != nil {
		hold := v2TestHoldJoinQueue
		m.v2.wg.Add(1)
		go func() {
			defer m.v2.wg.Done()
			select {
			case <-hold:
			case <-m.v2.stop:
				return
			}
			_ = m.joinExternalQueueV2()
		}()
	} else if err := m.joinExternalQueueV2(); err != nil {
		return err
	}
	return m.nc.Flush()
}

func (m *Manager) stopV2() {
	if m == nil || m.v2 == nil {
		return
	}
	m.publishOwnerExitV2()
	for _, sub := range m.v2.mgrSubs {
		_ = sub.Unsubscribe()
	}
	for _, sub := range m.v2.cmdSubs {
		_ = sub.Unsubscribe()
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
	if m.v2ExternalBlocked(sender) {
		ms := RetryAfterMsV2(0)
		m.replyV2Error(msg, requestIDFromData(msg.Data), ErrBusyV2, &ms)
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
	if suffix != "SESSION.CREATE" {
		if sid := sessionIDFromV2Data(msg.Data); sid != "" && m.maybeForwardV2(msg, sid) {
			return
		}
	}
	switch suffix {
	case "SESSION.CREATE":
		m.handleCreateV2(msg, sender)
	case "SESSION.COMMAND":
		m.handleSessionV2(msg, sender)
	case "CONNECTION.COMMAND":
		m.handleConnectionV2(msg, sender)
	case "SIGNAL.SEND":
		m.handleSignalSendV2(msg, sender)
	case "SIGNAL.ACK":
		m.handleSignalAckV2(msg, sender)
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
		if existing.registrationID == cmd.RegistrationID && existing.cid != 0 && existing.cid != cid {
			m.v2.mu.Unlock()
			m.replyV2Error(msg, cmd.RequestID, ErrInvalidRequestV2, nil)
			return
		}
	}
	m.v2.registerSyncing++
	m.v2.mu.Unlock()
	defer func() {
		m.v2.mu.Lock()
		if m.v2.registerSyncing > 0 {
			m.v2.registerSyncing--
		}
		m.v2.mu.Unlock()
	}()
	snap, err := m.v2.store.RegisterNode(cmd)
	if err != nil {
		m.replyV2Error(msg, cmd.RequestID, protocolCodeV2(err), nil)
		return
	}
	pendingMut := v2MgrMutation{
		Kind:              v2MutationRegister,
		Owner:             m.serverID,
		RequestID:         cmd.RequestID,
		NodeKey:           sender,
		RegistrationID:    snap.RegistrationID,
		RegistrationEpoch: snap.RegistrationEpoch,
		CID:               cid,
		NatsServerID:      m.serverID,
		Pending:           true,
	}
	if err := m.syncMutationV2(pendingMut); err != nil {
		ms := RetryAfterMsV2(0)
		m.replyV2Error(msg, cmd.RequestID, ErrBusyV2, &ms)
		return
	}
	finalMut := pendingMut
	finalMut.MutationID = ""
	finalMut.MessageID = ""
	finalMut.Pending = false
	if err := m.syncMutationV2(finalMut); err != nil {
		ms := RetryAfterMsV2(0)
		m.replyV2Error(msg, cmd.RequestID, ErrBusyV2, &ms)
		return
	}
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
	key := RequestKeyV2{SenderNodeKey: sender, RequestID: dec.Create.RequestID}
	m.v2.mu.Lock()
	if sid, ok := m.v2.requests[key]; ok {
		meta := m.v2.owners[sid]
		m.v2.mu.Unlock()
		if meta != nil {
			if meta.Owner == m.serverID {
				_ = m.syncSessionLockedV2(meta.SessionID, dec.Create.RequestID, sender)
			}
			m.replyAllocatedOwnerV2(msg, dec.Create.RequestID, meta)
			return
		}
	} else {
		m.v2.mu.Unlock()
	}
	snap, err := m.v2.store.AllocateSession(sender, *dec.Create)
	if err != nil {
		m.replyV2Error(msg, dec.Create.RequestID, protocolCodeV2(err), nil)
		return
	}
	timeoutMs := DefaultSetupTimeoutV2.Milliseconds()
	if dec.Create.TotalTimeoutMs != 0 {
		timeoutMs = dec.Create.TotalTimeoutMs
	}
	m.v2.mu.Lock()
	m.v2.owners[snap.SessionID] = &v2SessionMeta{
		SessionID:      snap.SessionID,
		Owner:          m.serverID,
		Revision:       snap.Revision,
		RequestID:      dec.Create.RequestID,
		ClientNode:     snap.ClientNodeKey,
		ServerNode:     snap.ServerNodeKey,
		State:          snap.State,
		ConnectionID:   snap.ConnectionID,
		Epoch:          snap.Epoch,
		SetupTimeoutMs: timeoutMs,
	}
	m.v2.requests[key] = snap.SessionID
	m.v2.mu.Unlock()
	if err := m.syncSessionLockedV2(snap.SessionID, dec.Create.RequestID, sender); err != nil {
		ms := RetryAfterMsV2(0)
		m.replyV2Error(msg, dec.Create.RequestID, ErrBusyV2, &ms)
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
	if cmd.Command == SessionCommandCloseV2 {
		m.dropSignalRetriesForSessionV2(cmd.SessionID)
	}
	if err := m.publishEventsV2(events); err != nil {
		m.replyV2Error(msg, cmd.RequestID, ErrInternalErrorV2, nil)
		return
	}
	_ = m.syncSessionLockedV2(cmd.SessionID, cmd.RequestID, sender)
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
		_ = m.syncSessionLockedV2(cmd.SessionID, cmd.RequestID, sender)
		m.replyAllocatedV2(msg, cmd.RequestID, snap)
	case ConnectionCommandRestartV2:
		old := GenerationKeyV2{SessionID: cmd.SessionID, ConnectionID: cmd.ConnectionID, Epoch: cmd.Epoch}
		snap, err := m.v2.store.AllocateRestart(sender, cmd)
		if err != nil {
			m.replyConnectionAllocErrV2(msg, cmd, err)
			return
		}
		if old.Epoch == 0 {
			old.Epoch = snap.Epoch - 1
		}
		m.dropSignalRetriesForGenerationV2(old)
		_ = m.syncSessionLockedV2(cmd.SessionID, cmd.RequestID, sender)
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
		_ = m.syncSessionLockedV2(cmd.SessionID, cmd.RequestID, sender)
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
		_ = m.syncSessionLockedV2(cmd.SessionID, cmd.RequestID, sender)
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
		m.dropSignalRetriesForGenerationV2(GenerationKeyV2{SessionID: cmd.SessionID, ConnectionID: cmd.ConnectionID, Epoch: cmd.Epoch})
		if err := m.publishEventsV2(events); err != nil {
			m.replyV2Error(msg, cmd.RequestID, ErrInternalErrorV2, nil)
			return
		}
		_ = m.syncSessionLockedV2(cmd.SessionID, cmd.RequestID, sender)
		m.replyV2OK(msg, cmd.RequestID)
	default:
		m.replyV2Error(msg, cmd.RequestID, ErrInvalidStateV2, nil)
	}
}

func (m *Manager) handleSignalSendV2(msg *nats.Msg, sender string) {
	dec, err := DecodeFrameV2(FrameKindSignalSendV2, msg.Data)
	if err != nil {
		m.replyV2Error(msg, requestIDFromData(msg.Data), protocolCodeV2(err), nil)
		return
	}
	cmd := *dec.SignalSend
	unlock := m.lockSessionV2(cmd.SessionID)
	target, ev, err := m.v2.store.AcceptSignal(sender, cmd)
	unlock()
	if err != nil {
		m.replyV2Error(msg, cmd.RequestID, protocolCodeV2(err), nil)
		return
	}
	m.replyV2Accepted(msg, cmd.RequestID)
	if ev.MessageID == "" {
		return
	}
	m.enqueueSignalRetryV2(signalRetryItemV2{
		dir: DirectionKeyV2{
			GenerationKeyV2: GenerationKeyV2{SessionID: ev.Identity.SessionID, ConnectionID: ev.Identity.ConnectionID, Epoch: ev.Identity.Epoch},
			SenderNodeKey:   sender,
		},
		seq:       ev.Signal.Seq,
		messageID: ev.MessageID,
		target:    target,
	})
}

func (m *Manager) handleSignalAckV2(msg *nats.Msg, sender string) {
	dec, err := DecodeFrameV2(FrameKindSignalAckV2, msg.Data)
	if err != nil {
		m.replyV2Error(msg, requestIDFromData(msg.Data), protocolCodeV2(err), nil)
		return
	}
	cmd := *dec.SignalAck
	unlock := m.lockSessionV2(cmd.SessionID)
	err = m.v2.store.AckSignal(sender, cmd)
	unlock()
	if err != nil {
		m.replyV2Error(msg, cmd.RequestID, protocolCodeV2(err), nil)
		return
	}
	if snap, ok := m.v2.store.PeekSession(cmd.SessionID); ok {
		peer := snap.ClientNodeKey
		if sender == snap.ClientNodeKey {
			peer = snap.ServerNodeKey
		}
		m.dropSignalRetriesV2(DirectionKeyV2{
			GenerationKeyV2: GenerationKeyV2{SessionID: cmd.SessionID, ConnectionID: cmd.ConnectionID, Epoch: cmd.Epoch},
			SenderNodeKey:   peer,
		}, cmd.AckSeq)
	}
	m.replyV2OK(msg, cmd.RequestID)
}

func (m *Manager) replyV2Accepted(msg *nats.Msg, requestID string) {
	body, err := encodeEnvelopeV2(requestID, "", "", map[string]any{"ok": true, "accepted": true})
	if err != nil {
		return
	}
	_ = msg.Respond(body)
}

func (m *Manager) enqueueSignalRetryV2(item signalRetryItemV2) {
	if item.messageID == "" {
		return
	}
	item.next = m.now()
	key := MessageKeyV2{Direction: item.dir, MessageID: item.messageID}
	m.v2.retryMu.Lock()
	if _, ok := m.v2.retries[key]; !ok {
		m.v2.retries[key] = item
	}
	m.v2.retryMu.Unlock()
	m.wakeSignalRetryV2()
}

func (m *Manager) dropSignalRetriesV2(dir DirectionKeyV2, ackSeq uint64) {
	m.v2.retryMu.Lock()
	defer m.v2.retryMu.Unlock()
	for k, item := range m.v2.retries {
		if item.dir == dir && item.seq <= ackSeq {
			delete(m.v2.retries, k)
		}
	}
}

func (m *Manager) dropSignalRetriesForGenerationV2(gk GenerationKeyV2) {
	m.v2.retryMu.Lock()
	defer m.v2.retryMu.Unlock()
	for k, item := range m.v2.retries {
		if item.dir.GenerationKeyV2 == gk {
			delete(m.v2.retries, k)
		}
	}
}

func (m *Manager) dropSignalRetriesForSessionV2(sessionID string) {
	m.v2.retryMu.Lock()
	defer m.v2.retryMu.Unlock()
	for k, item := range m.v2.retries {
		if item.dir.SessionID == sessionID {
			delete(m.v2.retries, k)
		}
	}
}

func (m *Manager) wakeSignalRetryV2() {
	select {
	case m.v2.retryWake <- struct{}{}:
	default:
	}
}

func (m *Manager) signalRetryLoopV2() {
	defer m.v2.wg.Done()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-m.v2.stop:
			return
		case <-m.v2.retryWake:
			m.flushSignalRetriesV2()
		case <-ticker.C:
			m.flushSignalRetriesV2()
		}
	}
}

func (m *Manager) flushSignalRetriesV2() {
	now := m.now()
	if expired := m.v2.store.ExpirePendingSignals(now); len(expired) > 0 {
		_ = m.publishEventsV2(expired)
		m.dropSignalRetriesMatchingEventsV2(expired)
	}
	var due []signalRetryItemV2
	m.v2.retryMu.Lock()
	for k, item := range m.v2.retries {
		if item.next.After(now) {
			continue
		}
		due = append(due, item)
		delete(m.v2.retries, k)
	}
	m.v2.retryMu.Unlock()
	for _, item := range due {
		deadline, ok := m.v2.store.generationDeadline(item.dir)
		if !ok || !now.Before(deadline) {
			continue
		}
		ev, ok := m.v2.store.pendingSignal(item.dir, item.seq)
		if !ok {
			continue
		}
		if ev.TargetNodeKey == "" {
			ev.TargetNodeKey = item.target
		}
		if err := m.publishV2Event(item.target, ev); err != nil {
			m.requeueSignalRetryUntilDeadlineV2(item, now, deadline)
			continue
		}
		m.requeueSignalRetryUntilDeadlineV2(item, now, deadline)
	}
}

func (m *Manager) requeueSignalRetryUntilDeadlineV2(item signalRetryItemV2, now, deadline time.Time) {
	item.attempt++
	next := now.Add(time.Duration(RetryAfterMsV2(item.attempt)) * time.Millisecond)
	if !next.Before(deadline) {
		next = deadline
	}
	if !now.Before(deadline) {
		return
	}
	item.next = next
	m.requeueSignalRetryV2(item)
}

func (m *Manager) dropSignalRetriesMatchingEventsV2(events []EventV2) {
	for _, ev := range events {
		m.dropSignalRetriesForGenerationV2(GenerationKeyV2{
			SessionID:    ev.Identity.SessionID,
			ConnectionID: ev.Identity.ConnectionID,
			Epoch:        ev.Identity.Epoch,
		})
	}
}

func (m *Manager) requeueSignalRetryV2(item signalRetryItemV2) {
	if _, ok := m.v2.store.pendingSignal(item.dir, item.seq); !ok {
		return
	}
	key := MessageKeyV2{Direction: item.dir, MessageID: item.messageID}
	m.v2.retryMu.Lock()
	m.v2.retries[key] = item
	m.v2.retryMu.Unlock()
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
	if event.TargetNodeKey != "" {
		nodeKey = event.TargetNodeKey
	}
	reg := event.RegistrationID
	if event.Kind == FrameKindSignalSendV2 {
		snap, ok := m.v2.store.LookupNode(nodeKey)
		if !ok || snap.RegistrationID == "" {
			return errors.New("signal target registration missing")
		}
		reg = snap.RegistrationID
	} else {
		m.v2.mu.Lock()
		if b, ok := m.v2.nodes[nodeKey]; ok && b.registrationID != "" {
			reg = b.registrationID
		}
		m.v2.mu.Unlock()
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
	case FrameKindErrorV2:
		code := ErrSetupTimeoutV2
		if event.Error != nil && event.Error.Code != "" {
			code = event.Error.Code
		}
		payload := map[string]any{
			"error":           string(code),
			"session_id":      event.Identity.SessionID,
			"connection_id":   event.Identity.ConnectionID,
			"epoch":           event.Identity.Epoch,
			"revision":        event.Revision,
			"sender_node_key": v2CoordinatorSender,
		}
		body, err = encodeEnvelopeV2("", event.MessageID, reg, payload)
	case FrameKindSignalSendV2:
		sig := event.Signal
		if sig == nil {
			return errors.New("missing signal")
		}
		payload := map[string]any{
			"session_id":      sig.SessionID,
			"connection_id":   sig.ConnectionID,
			"epoch":           sig.Epoch,
			"seq":             sig.Seq,
			"type":            sig.Type,
			"payload":         sig.Payload,
			"sender_node_key": sig.SenderNodeKey,
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
	if v2TestPublishHook != nil {
		if err := v2TestPublishHook(subj, body); err != nil {
			return err
		}
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

func (m *Manager) v2ExternalBlocked(sender string) bool {
	if m.v2 == nil {
		return false
	}
	m.v2.mu.Lock()
	defer m.v2.mu.Unlock()
	if m.v2.registerSyncing > 0 {
		return true
	}
	if b, ok := m.v2.nodes[sender]; ok && b.pending {
		return true
	}
	return false
}

func (m *Manager) maybeForwardV2(msg *nats.Msg, sessionID string) bool {
	m.v2.mu.Lock()
	meta, ok := m.v2.owners[sessionID]
	m.v2.mu.Unlock()
	if !ok {
		return false
	}
	if meta.Lost || meta.Owner == "" {
		m.replyV2Error(msg, requestIDFromData(msg.Data), ErrSessionNotFoundV2, nil)
		return true
	}
	if meta.Owner == m.serverID {
		return false
	}
	if err := m.forwardV2Command(msg, meta.Owner); err != nil {
		m.replyV2Error(msg, requestIDFromData(msg.Data), ErrCoordinatorUnavailableV2, nil)
	}
	return true
}

func (m *Manager) forwardV2Command(msg *nats.Msg, owner string) error {
	body, err := json.Marshal(v2ForwardedCmd{Subject: msg.Subject, Data: msg.Data})
	if err != nil {
		return err
	}
	reply, err := m.mgrConn().Request(v2MgrCommandSubject(owner), body, v2ClusterSyncTimeout)
	if err != nil {
		return err
	}
	if msg.Reply != "" {
		_ = msg.Respond(reply.Data)
	}
	return nil
}

func (m *Manager) replyAllocatedOwnerV2(msg *nats.Msg, requestID string, meta *v2SessionMeta) {
	state := string(meta.State)
	if state == "" {
		state = string(SessionStateAllocatedV2)
	}
	connID := meta.ConnectionID
	if connID == "" {
		connID = "conn-0"
	}
	epoch := meta.Epoch
	if epoch == 0 {
		epoch = 1
	}
	rev := meta.Revision
	if rev == 0 {
		rev = 1
	}
	body, err := encodeEnvelopeV2(requestID, "", "", map[string]any{
		"ok":              true,
		"session_id":      meta.SessionID,
		"connection_id":   connID,
		"epoch":           epoch,
		"state":           state,
		"revision":        rev,
		"owner_server_id": meta.Owner,
	})
	if err != nil {
		m.replyV2Error(msg, requestID, ErrInternalErrorV2, nil)
		return
	}
	_ = msg.Respond(body)
}

func (m *Manager) SessionOwnerV2(sessionID string) (string, uint64, bool) {
	if m == nil || m.v2 == nil {
		return "", 0, false
	}
	m.v2.mu.Lock()
	defer m.v2.mu.Unlock()
	meta, ok := m.v2.owners[sessionID]
	if !ok || meta.Owner == "" {
		return "", 0, false
	}
	return meta.Owner, meta.Revision, true
}

func (m *Manager) dropQueueGroupV2() {
	if m == nil || m.v2 == nil {
		return
	}
	for _, sub := range m.v2.cmdSubs {
		_ = sub.Unsubscribe()
	}
	m.v2.cmdSubs = nil
	m.v2.mu.Lock()
	m.v2.joinedQueue = false
	m.v2.mu.Unlock()
}

func (m *Manager) setOwnerUnreachableV2(v bool) {
	if m == nil || m.v2 == nil {
		return
	}
	m.v2.mu.Lock()
	m.v2.unreachable = v
	m.v2.mu.Unlock()
}

func (m *Manager) joinedQueueGroupV2() bool {
	if m == nil || m.v2 == nil {
		return false
	}
	m.v2.mu.Lock()
	defer m.v2.mu.Unlock()
	return m.v2.joinedQueue
}

func (m *Manager) forgetSessionOwnerV2(sessionID string) {
	if m == nil || m.v2 == nil {
		return
	}
	m.v2.mu.Lock()
	delete(m.v2.owners, sessionID)
	m.v2.mu.Unlock()
	m.v2.store.mu.Lock()
	delete(m.v2.store.sessions, sessionID)
	m.v2.store.mu.Unlock()
}

func (m *Manager) injectV2Disconnect(node string, cid uint64) {
	body, _ := json.Marshal(map[string]any{"client": map[string]any{"name": node, "id": cid}})
	m.handleDisconnectV2(&nats.Msg{
		Subject: "$SYS.ACCOUNT." + m.agentAccount() + ".DISCONNECT",
		Data:    body,
	})
}

func protocolCodeV2(err error) ErrorCodeV2 {
	var pe *ProtocolErrorV2
	if errors.As(err, &pe) && pe.Code != "" {
		return pe.Code
	}
	return ErrInternalErrorV2
}
