package timeline_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/ywacch/cloud9/pkg/templateapp/jobs/timeline"
	"github.com/ywacch/cloud9/pkg/templateapp/jobs/timeline/timelinetest"
)

func TestRedisStore_Conformance(t *testing.T) {
	t.Parallel()
	timelinetest.RunConformance(t, func(t *testing.T) (timeline.Store, func()) {
		t.Helper()
		srv, err := miniredis.Run()
		if err != nil {
			t.Fatal(err)
		}
		client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
		store := timeline.NewRedis(client, timeline.RedisOptions{MaxEntries: 1000})
		return store, func() {
			_ = client.Close()
			srv.Close()
		}
	})
}

func TestRedis_MemberFormatUnchanged(t *testing.T) {
	t.Parallel()
	when := time.Unix(100, 0).UTC()
	payload := []byte(`{"post_id":1,"author_id":2,"username":"u","content":"hi","created_at":"1970-01-01T00:00:01.000000000Z"}`)
	item := timeline.Item{PostID: 1, AuthorID: 2, Payload: payload, CreatedAt: when}
	member, score, err := timeline.MemberStringForTest(item)
	if err != nil {
		t.Fatal(err)
	}
	if score != float64(when.UnixMilli()) {
		t.Fatalf("score: got %v want %v", score, float64(when.UnixMilli()))
	}
	const wantMember = `{"post_id":1,"author_id":2,"username":"u","content":"hi","created_at":"1970-01-01T00:00:01.000000000Z"}`
	if member != wantMember {
		t.Fatalf("member:\n got %q\nwant %q", member, wantMember)
	}
	updateGolden := os.Getenv("UPDATE_GOLDEN") != ""
	goldenPath := "testdata/redis_member.golden.txt"
	if updateGolden {
		if err := os.WriteFile(goldenPath, []byte(member), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	raw, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read golden (set UPDATE_GOLDEN=1 to create): %v", err)
	}
	if string(raw) != member {
		t.Fatalf("golden member mismatch:\n got %q\nwant %q", member, string(raw))
	}
}

func TestRedis_AppendTTL(t *testing.T) {
	t.Parallel()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	store := timeline.NewRedis(client, timeline.RedisOptions{
		MaxEntries: timeline.DefaultMaxEntries,
		TTL:        45 * time.Second,
	})
	item := timeline.Item{
		PostID:    1,
		Payload:   []byte(`{"post_id":1}`),
		CreatedAt: time.Unix(100, 0).UTC(),
	}
	if err := store.Append(ctx, []int64{7}, item); err != nil {
		t.Fatal(err)
	}
	key := timeline.DefaultKeyPrefix + "7"
	if !srv.Exists(key) {
		t.Fatalf("expected %s", key)
	}
	if got := srv.TTL(key); got != 45*time.Second {
		t.Fatalf("TTL = %v want 45s", got)
	}
}
