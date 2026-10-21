package vu

import (
	"context"
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestConversation_ReplyUsesRootID(t *testing.T) {
	var mu sync.Mutex
	var bodies []map[string]any
	var ids []int
	nextID := 1
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var m map[string]any
		_ = json.Unmarshal(raw, &m)
		mu.Lock()
		bodies = append(bodies, m)
		id := nextID
		nextID++
		ids = append(ids, id)
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]int{"post_id": id})
	}))
	defer srv.Close()

	actions := map[string]Action{
		actionCreateRoot: {
			Method: "POST", Path: "/post",
			Body:    `{"user_id":{{.UserID}},"content":"root"}`,
			IDField: "post_id",
		},
		actionCreateReply: {
			Method: "POST", Path: "/post",
			Body:    `{"user_id":{{.UserID}},"reply_to_id":{{.ParentID}}}`,
			IDField: "post_id",
		},
	}
	cfg := Config{Behaviors: []BehaviorConfig{
		{Name: behaviorConversationStarter, Count: 1},
		{Name: behaviorReplier, Count: 1, Params: json.RawMessage(`{"replyWithinMs":50,"thinkMs":100}`)},
	}}
	runner, err := NewRunner(DefaultRegistry(), cfg, RunnerDeps{
		BaseURL: srv.URL,
		Actions: actions,
		UserIDs: []string{"1"},
		Client:  srv.Client(),
		Sleep:   func(ctx context.Context, d time.Duration) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := runner.Start(ctx); err != nil {
		t.Fatal(err)
	}
	runner.Stop()

	mu.Lock()
	defer mu.Unlock()
	idSet := make(map[int]struct{}, len(ids))
	for _, id := range ids {
		idSet[id] = struct{}{}
	}
	for _, b := range bodies {
		if v, ok := b["reply_to_id"]; ok {
			replyID, _ := v.(float64)
			if _, ok := idSet[int(replyID)]; !ok {
				t.Fatalf("reply_to_id %v not in server ids %v", replyID, ids)
			}
		}
	}
}

func TestReplier_GapUnderReplyWithin(t *testing.T) {
	postedAt := time.Unix(0, 0)
	now := postedAt
	actions := map[string]Action{
		actionCreateReply: {
			Method: "POST", Path: "/post", Body: "{}", IDField: "post_id",
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]int{"post_id": 2})
	}))
	defer srv.Close()

	rec := NewRecorder(1)
	shared := &Shared{Roots: NewRootFeed()}
	shared.Roots.Publish(RootPost{ID: "1", PostedAt: postedAt})

	v := &VU{
		ID: 0, UserID: "1", BaseURL: srv.URL, Actions: actions, Client: srv.Client(),
		RNG: rand.New(rand.NewSource(1)),
		Now: func() time.Time { return now },
		Sleep: func(ctx context.Context, d time.Duration) error {
			now = now.Add(d)
			return ctx.Err()
		},
		Stats: rec, Shared: shared,
	}
	// One reply iteration: wait 50ms then post.
	root, ok := v.Shared.Roots.TakeForReply()
	if !ok {
		t.Fatal("no root")
	}
	now = now.Add(50 * time.Millisecond)
	_, err := doAction(context.Background(), v, actionCreateReply, Vars{UserID: "1", ParentID: root.ID})
	if err != nil {
		t.Fatal(err)
	}
	rec.ObserveGap(v.Now().Sub(root.PostedAt))
	st := rec.Snapshot()
	if st.ReplyGapMs.Count != 1 || st.ReplyGapMs.P50 != 50 {
		t.Fatalf("gap stats: %+v", st.ReplyGapMs)
	}
}
