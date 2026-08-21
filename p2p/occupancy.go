package p2p

import (
	"errors"
	"sync"
)

var ErrNodeKeyInUse = errors.New(CodeNodeKeyInUse)

type Record struct {
	ServerID  string
	ConnName  string
	Inbox     string
	ClaimID   string
	ClaimedAt int64
}

type Table struct {
	mu   sync.Mutex
	data map[string]map[string]Record
}

func NewTable() *Table {
	return &Table{
		data: make(map[string]map[string]Record),
	}
}

func (t *Table) Claim(account, nodeKey string, rec Record) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	acc, ok := t.data[account]
	if !ok {
		acc = make(map[string]Record)
		t.data[account] = acc
	}
	if existing, ok := acc[nodeKey]; ok {
		if existing.ConnName != rec.ConnName {
			return ErrNodeKeyInUse
		}
		return nil
	}
	acc[nodeKey] = rec
	return nil
}

func (t *Table) Get(account, nodeKey string) (Record, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	acc, ok := t.data[account]
	if !ok {
		return Record{}, false
	}
	rec, ok := acc[nodeKey]
	return rec, ok
}

func (t *Table) Release(account, nodeKey, connName string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	acc, ok := t.data[account]
	if !ok {
		return false
	}
	rec, ok := acc[nodeKey]
	if !ok || rec.ConnName != connName {
		return false
	}
	delete(acc, nodeKey)
	if len(acc) == 0 {
		delete(t.data, account)
	}
	return true
}

func (t *Table) DropServer(serverID string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for account, acc := range t.data {
		for nodeKey, rec := range acc {
			if rec.ServerID == serverID {
				delete(acc, nodeKey)
			}
		}
		if len(acc) == 0 {
			delete(t.data, account)
		}
	}
}
