package vu

import (
	"context"
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestFindOrphans(t *testing.T) {
	parent := func(id int) threadPost { return threadPost{ID: id} }
	reply := func(id, parentID int) threadPost {
		p := parentID
		return threadPost{ID: id, ReplyToID: &p}
	}
	tests := []struct {
		name    string
		replies []threadPost
		parents []threadPost
		wantLen int
		wantPID int
	}{
		{name: "no replies", replies: nil, parents: []threadPost{parent(1)}, wantLen: 0},
		{name: "all present", replies: []threadPost{reply(2, 1)}, parents: []threadPost{parent(1)}, wantLen: 0},
		{name: "one missing", replies: []threadPost{reply(2, 1)}, parents: nil, wantLen: 1, wantPID: 1},
		{
			name:    "nested reply parent present",
			replies: []threadPost{reply(2, 1), reply(3, 2)},
			parents: []threadPost{parent(1), parent(2)},
			wantLen: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := FindOrphans(tc.replies, tc.parents)
			if len(got) != tc.wantLen {
				t.Fatalf("len %d want %d", len(got), tc.wantLen)
			}
			if tc.wantLen > 0 && got[0].ParentID != tc.wantPID {
				t.Fatalf("parent %d want %d", got[0].ParentID, tc.wantPID)
			}
		})
	}
}

func TestThreadObserver_CountsViolation(t *testing.T) {
	body := threadReadResponse{
		RootID: 7,
		Replies: []threadPost{
			{ID: 9, ReplyToID: intPtr(8)},
		},
		Parents: []threadPost{{ID: 7}},
	}
	body.ServedBy.Replies = "replica-a"
	body.ServedBy.Parents = "replica-b"
	raw, _ := json.Marshal(body)

	var sawHeader bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(observerRequestHeader) == observerRequestHeaderVal {
			sawHeader = true
		}
		_, _ = w.Write(raw)
	}))
	defer srv.Close()

	feed := NewRootFeed()
	feed.Publish(RootPost{ID: "7", PostedAt: time.Now()})

	rec := NewRecorder(1)
	now := time.Now()
	v := &VU{
		ID:      0,
		UserID:  "1",
		BaseURL: srv.URL,
		Actions: map[string]Action{
			actionReadThread: {Method: "GET", Path: "/threads/{{.RootID}}"},
		},
		Client: srv.Client(),
		RNG:    rand.New(rand.NewSource(1)),
		Now:    func() time.Time { return now },
		Stats:  rec,
		Shared: &Shared{Roots: feed},
	}
	obs := threadObserver{interval: time.Second, maxAge: time.Minute}
	if err := obs.tick(context.Background(), v, behaviorThreadObserver); err != nil {
		t.Fatal(err)
	}
	if !sawHeader {
		t.Fatal("expected observer header")
	}
	snap := rec.Snapshot()
	if snap.Observer.Violations != 1 {
		t.Fatalf("violations %d", snap.Observer.Violations)
	}
	if len(snap.Observer.Examples) != 1 {
		t.Fatalf("examples %d", len(snap.Observer.Examples))
	}
	ex := snap.Observer.Examples[0]
	if ex.RootID != 7 || ex.ReplyID != 9 || ex.ParentID != 8 {
		t.Fatalf("example %+v", ex)
	}
	if ex.ServedBy.Replies != "replica-a" || ex.ServedBy.Parents != "replica-b" {
		t.Fatalf("servedBy %+v", ex.ServedBy)
	}
}

func TestThreadObserver_SendsObserverHeader(t *testing.T) {
	var mu sync.Mutex
	var headers []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		headers = append(headers, r.Header.Get(observerRequestHeader))
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(threadReadResponse{RootID: 1})
	}))
	defer srv.Close()

	feed := NewRootFeed()
	feed.Publish(RootPost{ID: "1", PostedAt: time.Now()})
	_, _, err := readThread(context.Background(), &VU{
		BaseURL: srv.URL,
		Actions: map[string]Action{actionReadThread: {Method: "GET", Path: "/threads/{{.RootID}}"}},
		Client:  srv.Client(),
		RNG:     rand.New(rand.NewSource(2)),
		UserID:  "1",
	}, "1")
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(headers) != 1 || headers[0] != observerRequestHeaderVal {
		t.Fatalf("headers %v", headers)
	}
}

func intPtr(n int) *int { return &n }
