package main

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/ywacch/cloud9/pkg/templateapp/jobs/timeline"
)

func TestTimelineStore_AppendTTL(t *testing.T) {
	tests := []struct {
		name    string
		ttl     time.Duration
		wantTTL time.Duration
	}{
		{name: "zero TTL leaves timeline persistent", ttl: 0, wantTTL: 0},
		{name: "positive TTL sets expiry", ttl: 45 * time.Second, wantTTL: 45 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, err := miniredis.Run()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(srv.Close)

			rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
			t.Cleanup(func() { _ = rdb.Close() })

			ctx := context.Background()
			store := timeline.NewRedis(rdb, timeline.RedisOptions{
				MaxEntries: timeline.DefaultMaxEntries,
				TTL:        tt.ttl,
			})
			if err := store.Append(ctx, []int64{7}, timeline.Item{
				PostID:    1,
				Payload:   []byte(`{"post_id":1}`),
				CreatedAt: time.Unix(100, 0).UTC(),
			}); err != nil {
				t.Fatalf("append: %v", err)
			}

			key := timeline.DefaultKeyPrefix + "7"
			if !srv.Exists(key) {
				t.Fatalf("expected %s to exist", key)
			}
			if got := srv.TTL(key); got != tt.wantTTL {
				t.Fatalf("TTL(%s) = %v, want %v", key, got, tt.wantTTL)
			}
		})
	}
}

func TestParseCacheTTLSeconds(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		def  int
		want int
	}{
		{name: "absent uses default", raw: "", def: 0, want: 0},
		{name: "zero means no expiry", raw: "0", def: 30, want: 0},
		{name: "timed expiry", raw: "600", def: 0, want: 600},
		{name: "negative uses default", raw: "-1", def: 0, want: 0},
		{name: "malformed uses default", raw: "abc", def: 0, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseCacheTTLSeconds(tt.raw, tt.def); got != tt.want {
				t.Fatalf("parseCacheTTLSeconds(%q, %d) = %d, want %d", tt.raw, tt.def, got, tt.want)
			}
		})
	}
}

func TestCacheTTLSecondsFromEnv_FeedOverride(t *testing.T) {
	t.Setenv("FEED_CACHE_TTL", "15")
	t.Setenv("CACHE_TTL", "600")
	if got := cacheTTLSecondsFromEnv(0, "FEED_CACHE_TTL", "CACHE_TTL"); got != 15 {
		t.Fatalf("cacheTTLSecondsFromEnv = %d, want 15 (FEED_CACHE_TTL shadows CACHE_TTL)", got)
	}
}
