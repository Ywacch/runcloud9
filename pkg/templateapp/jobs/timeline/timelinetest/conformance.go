package timelinetest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/templateapp/jobs/timeline"
)

// Factory returns a fresh, empty store and a cleanup func.
type Factory func(t *testing.T) (timeline.Store, func())

// Reconnector is optional. Stores that implement it are tested for survival across reconnect.
type Reconnector interface {
	Reconnect(ctx context.Context) error
}

// RunConformance runs the shared timeline store tests against newStore.
func RunConformance(t *testing.T, newStore Factory) {
	t.Helper()
	ctx := context.Background()

	t.Run("append_then_range_newest_first", func(t *testing.T) {
		s, cleanup := newStore(t)
		defer cleanup()
		base := time.Unix(1_700_000_000, 0).UTC()
		if err := s.Append(ctx, []int64{1}, timeline.Item{PostID: 1, AuthorID: 10, CreatedAt: base}); err != nil {
			t.Fatalf("append: %v", err)
		}
		if err := s.Append(ctx, []int64{1}, timeline.Item{PostID: 2, AuthorID: 10, CreatedAt: base.Add(time.Minute)}); err != nil {
			t.Fatalf("append: %v", err)
		}
		got, err := s.Range(ctx, 1, 10)
		if err != nil {
			t.Fatalf("range: %v", err)
		}
		if len(got) != 2 || got[0].PostID != 2 || got[1].PostID != 1 {
			t.Fatalf("order: %+v", got)
		}
	})

	t.Run("range_honors_limit", func(t *testing.T) {
		s, cleanup := newStore(t)
		defer cleanup()
		base := time.Unix(1_700_000_100, 0).UTC()
		for i := int64(1); i <= 5; i++ {
			if err := s.Append(ctx, []int64{7}, timeline.Item{PostID: i, CreatedAt: base.Add(time.Duration(i) * time.Second)}); err != nil {
				t.Fatalf("append: %v", err)
			}
		}
		got, err := s.Range(ctx, 7, 3)
		if err != nil {
			t.Fatalf("range: %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("len: want 3, got %d", len(got))
		}
	})

	t.Run("range_limit_zero", func(t *testing.T) {
		s, cleanup := newStore(t)
		defer cleanup()
		_, err := s.Range(ctx, 1, 0)
		if !errors.Is(err, timeline.ErrInvalidLimit) {
			t.Fatalf("want ErrInvalidLimit, got %v", err)
		}
	})

	t.Run("trim_keeps_newest", func(t *testing.T) {
		s, cleanup := newStore(t)
		defer cleanup()
		base := time.Unix(1_700_000_200, 0).UTC()
		for i := int64(1); i <= 4; i++ {
			if err := s.Append(ctx, []int64{3}, timeline.Item{PostID: i, CreatedAt: base.Add(time.Duration(i) * time.Second)}); err != nil {
				t.Fatalf("append: %v", err)
			}
		}
		if err := s.Trim(ctx, 3, 2); err != nil {
			t.Fatalf("trim: %v", err)
		}
		got, err := s.Range(ctx, 3, 10)
		if err != nil {
			t.Fatalf("range: %v", err)
		}
		if len(got) != 2 || got[0].PostID != 4 || got[1].PostID != 3 {
			t.Fatalf("after trim: %+v", got)
		}
	})

	t.Run("reappend_same_post_no_duplicate", func(t *testing.T) {
		s, cleanup := newStore(t)
		defer cleanup()
		when := time.Unix(1_700_000_300, 0).UTC()
		item := timeline.Item{PostID: 99, AuthorID: 1, Payload: []byte(`{"post_id":99}`), CreatedAt: when}
		if err := s.Append(ctx, []int64{5}, item); err != nil {
			t.Fatalf("append: %v", err)
		}
		if err := s.Append(ctx, []int64{5}, item); err != nil {
			t.Fatalf("reappend: %v", err)
		}
		got, err := s.Range(ctx, 5, 10)
		if err != nil {
			t.Fatalf("range: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("want one entry, got %d", len(got))
		}
	})

	t.Run("fanout_500_users", func(t *testing.T) {
		s, cleanup := newStore(t)
		defer cleanup()
		ids := make([]int64, 500)
		for i := range ids {
			ids[i] = int64(i + 1)
		}
		when := time.Unix(1_700_000_400, 0).UTC()
		if err := s.Append(ctx, ids, timeline.Item{PostID: 1, CreatedAt: when}); err != nil {
			t.Fatalf("append: %v", err)
		}
		for _, uid := range []int64{1, 250, 500} {
			got, err := s.Range(ctx, uid, 1)
			if err != nil {
				t.Fatalf("range uid=%d: %v", uid, err)
			}
			if len(got) != 1 || got[0].PostID != 1 {
				t.Fatalf("uid=%d: %+v", uid, got)
			}
		}
	})

	t.Run("concurrent_appends", func(t *testing.T) {
		s, cleanup := newStore(t)
		defer cleanup()
		const workers = 8
		const perWorker = 25
		base := time.Unix(1_700_000_500, 0).UTC()
		var wg sync.WaitGroup
		wg.Add(workers)
		for w := 0; w < workers; w++ {
			w := w
			go func() {
				defer wg.Done()
				for i := 0; i < perWorker; i++ {
					postID := int64(w*perWorker + i + 1)
					_ = s.Append(ctx, []int64{42}, timeline.Item{
						PostID:    postID,
						CreatedAt: base.Add(time.Duration(postID) * time.Millisecond),
					})
				}
			}()
		}
		wg.Wait()
		got, err := s.Range(ctx, 42, workers*perWorker+10)
		if err != nil {
			t.Fatalf("range: %v", err)
		}
		if len(got) != workers*perWorker {
			t.Fatalf("want %d items, got %d", workers*perWorker, len(got))
		}
	})

	t.Run("payload_round_trip", func(t *testing.T) {
		s, cleanup := newStore(t)
		defer cleanup()
		payload := []byte{0, 255, 'x', '\n'}
		when := time.Unix(1_700_000_600, 0).UTC()
		if err := s.Append(ctx, []int64{8}, timeline.Item{PostID: 1, Payload: payload, CreatedAt: when}); err != nil {
			t.Fatalf("append: %v", err)
		}
		got, err := s.Range(ctx, 8, 1)
		if err != nil {
			t.Fatalf("range: %v", err)
		}
		if len(got) != 1 || string(got[0].Payload) != string(payload) {
			t.Fatalf("payload: %q vs %q", got[0].Payload, payload)
		}
	})

	t.Run("created_at_tie_post_id_desc", func(t *testing.T) {
		s, cleanup := newStore(t)
		defer cleanup()
		when := time.Unix(1_700_000_700, 0).UTC()
		if err := s.Append(ctx, []int64{9}, timeline.Item{PostID: 1, CreatedAt: when}); err != nil {
			t.Fatalf("append: %v", err)
		}
		if err := s.Append(ctx, []int64{9}, timeline.Item{PostID: 3, CreatedAt: when}); err != nil {
			t.Fatalf("append: %v", err)
		}
		if err := s.Append(ctx, []int64{9}, timeline.Item{PostID: 2, CreatedAt: when}); err != nil {
			t.Fatalf("append: %v", err)
		}
		got, err := s.Range(ctx, 9, 10)
		if err != nil {
			t.Fatalf("range: %v", err)
		}
		if len(got) != 3 || got[0].PostID != 3 || got[1].PostID != 2 || got[2].PostID != 1 {
			t.Fatalf("tie order: %+v", got)
		}
	})

	t.Run("reconnect_survival", func(t *testing.T) {
		s, cleanup := newStore(t)
		defer cleanup()
		rc, ok := s.(Reconnector)
		if !ok {
			t.Skip("store does not implement Reconnector")
		}
		when := time.Unix(1_700_000_800, 0).UTC()
		if err := s.Append(ctx, []int64{11}, timeline.Item{PostID: 7, CreatedAt: when}); err != nil {
			t.Fatalf("append: %v", err)
		}
		if err := rc.Reconnect(ctx); err != nil {
			t.Fatalf("reconnect: %v", err)
		}
		got, err := s.Range(ctx, 11, 5)
		if err != nil {
			t.Fatalf("range: %v", err)
		}
		if len(got) != 1 || got[0].PostID != 7 {
			t.Fatalf("after reconnect: %+v", got)
		}
	})
}
