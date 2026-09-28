package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func testFeedCacheStats() (*feedCacheStats, *atomic.Int64, *atomic.Int64) {
	var hits, misses atomic.Int64
	return &feedCacheStats{hits: &hits, misses: &misses}, &hits, &misses
}

func TestFetchFeedItems_noCachePull(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	want := []FeedItem{{PostID: 1, AuthorID: 2, Username: "u", Content: "hi", CreatedAt: time.Unix(1, 0).UTC()}}
	items, mode, err := fetchFeedItems(ctx, nil, 10, 42, nil, func(c context.Context, userID, limit int) ([]FeedItem, error) {
		if userID != 42 || limit != 10 {
			t.Fatalf("pull args: got userID=%d limit=%d", userID, limit)
		}
		return want, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if mode != "pull" {
		t.Fatalf("mode: want pull, got %q", mode)
	}
	if len(items) != 1 || items[0].PostID != 1 {
		t.Fatalf("items: %+v", items)
	}
}

func TestFetchFeedItems_cacheEmptyReturnsEmptyPushRedis(t *testing.T) {
	t.Parallel()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)

	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	cache := newCache(rdb, false)

	ctx := context.Background()
	stats, hits, misses := testFeedCacheStats()
	pullHits := 0
	items, mode, err := fetchFeedItems(ctx, cache, 20, 7, stats, func(c context.Context, userID, limit int) ([]FeedItem, error) {
		pullHits++
		return []FeedItem{{PostID: 99, AuthorID: 3, Username: "x", Content: "db", CreatedAt: time.Unix(2, 0).UTC()}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 0 || misses.Load() != 1 {
		t.Fatalf("empty timeline: hits=%d misses=%d", hits.Load(), misses.Load())
	}
	if pullHits != 0 {
		t.Fatalf("push mode must not DB pull when Redis is present but empty, pulls=%d", pullHits)
	}
	if mode != "push-redis" {
		t.Fatalf("mode: want push-redis, got %q", mode)
	}
	if len(items) != 0 {
		t.Fatalf("want empty feed, got %+v", items)
	}
}

func TestFetchFeedItems_redisErrorPropagates(t *testing.T) {
	t.Parallel()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	cache := newCache(rdb, false)
	srv.Close()

	ctx := context.Background()
	pullHits := 0
	stats, hits, misses := testFeedCacheStats()
	_, mode, err := fetchFeedItems(ctx, cache, 10, 1, stats, func(c context.Context, userID, limit int) ([]FeedItem, error) {
		pullHits++
		return nil, nil
	})
	if err == nil {
		t.Fatal("expected error when Redis is unavailable")
	}
	if hits.Load() != 0 || misses.Load() != 0 {
		t.Fatalf("redis error must not count hit/miss: hits=%d misses=%d", hits.Load(), misses.Load())
	}
	if pullHits != 0 {
		t.Fatalf("pull must not run on Redis error, pulls=%d", pullHits)
	}
	if mode != "push-redis" {
		t.Fatalf("mode: want push-redis, got %q", mode)
	}
}

func TestFetchFeedItems_cacheHitPushRedis(t *testing.T) {
	t.Parallel()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)

	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	cache := newCache(rdb, false)

	wantItem := FeedItem{
		PostID:    10,
		AuthorID:  5,
		Username:  "alice",
		Content:   "hello",
		CreatedAt: time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC),
	}
	raw, err := json.Marshal(wantItem)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	userID := 11
	key := fmt.Sprintf("timeline:%d", userID)
	if err := rdb.ZAdd(ctx, key, redis.Z{
		Score:  float64(wantItem.CreatedAt.UnixMilli()),
		Member: string(raw),
	}).Err(); err != nil {
		t.Fatal(err)
	}

	stats, hits, misses := testFeedCacheStats()
	pullHits := 0
	items, mode, err := fetchFeedItems(ctx, cache, 20, userID, stats, func(c context.Context, uid, limit int) ([]FeedItem, error) {
		pullHits++
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 1 || misses.Load() != 0 {
		t.Fatalf("timeline hit: hits=%d misses=%d", hits.Load(), misses.Load())
	}
	if pullHits != 0 {
		t.Fatalf("should not DB fallback when Redis has timeline, pulls=%d", pullHits)
	}
	if mode != "push-redis" {
		t.Fatalf("mode: want push-redis, got %q", mode)
	}
	if len(items) != 1 {
		t.Fatalf("want 1 item, got %d", len(items))
	}
	if items[0].PostID != wantItem.PostID || items[0].Username != wantItem.Username {
		t.Fatalf("got %+v want %+v", items[0], wantItem)
	}
}
