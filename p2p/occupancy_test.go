package p2p

import "testing"

func TestTableFirstComeWins(t *testing.T) {
	tab := NewTable()
	rec := Record{ServerID: "s1", ConnName: "n1", Inbox: "$P2P.NODE.n1", ClaimID: "c1", ClaimedAt: 1}
	if err := tab.Claim("APP", "n1", rec); err != nil {
		t.Fatal(err)
	}
	if err := tab.Claim("APP", "n1", Record{ServerID: "s1", ConnName: "other", Inbox: "x", ClaimID: "c2"}); err != ErrNodeKeyInUse {
		t.Fatalf("err=%v", err)
	}
	if err := tab.Claim("APP", "n1", rec); err != nil {
		t.Fatal(err) // 同一 ConnName 幂等
	}
	if err := tab.Claim("OTHER", "n1", Record{ServerID: "s1", ConnName: "n1", Inbox: "y", ClaimID: "c3"}); err != nil {
		t.Fatal(err) // 账号隔离
	}
}

func TestTableReleaseAndDropServer(t *testing.T) {
	tab := NewTable()
	_ = tab.Claim("APP", "n1", Record{ServerID: "sA", ConnName: "n1", ClaimID: "c"})
	if !tab.Release("APP", "n1", "n1") {
		t.Fatal("release")
	}
	if _, ok := tab.Get("APP", "n1"); ok {
		t.Fatal("still present")
	}
	_ = tab.Claim("APP", "n2", Record{ServerID: "sB", ConnName: "n2"})
	tab.DropServer("sB")
	if _, ok := tab.Get("APP", "n2"); ok {
		t.Fatal("drop server")
	}
}
