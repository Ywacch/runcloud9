package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestFetchFeedItems_noCachePull(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	want := []FeedItem{{PostID: 1, AuthorID: 2, Username: "u", Content: "hi", CreatedAt: time.Unix(1, 0).UTC()}}
	items, mode, err := fetchFeedItems(ctx, nil, 10, 42, func(c context.Context, userID, limit int) ([]FeedItem, error) {
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

func TestFetchFeedItems_cacheEmptyFallback(t *testing.T) {
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
	pullHits := 0
	want := []FeedItem{{PostID: 99, AuthorID: 3, Username: "x", Content: "db", CreatedAt: time.Unix(2, 0).UTC()}}
	items, mode, err := fetchFeedItems(ctx, cache, 20, 7, func(c context.Context, userID, limit int) ([]FeedItem, error) {
		pullHits++
		if userID != 7 || limit != 20 {
			t.Fatalf("pull args: userID=%d limit=%d", userID, limit)
		}
		return want, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if pullHits != 1 {
		t.Fatalf("want 1 DB pull, got %d", pullHits)
	}
	if mode != "push-fallback" {
		t.Fatalf("mode: want push-fallback, got %q", mode)
	}
	if len(items) != 1 || items[0].PostID != 99 {
		t.Fatalf("items: %+v", items)
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

	pullHits := 0
	items, mode, err := fetchFeedItems(ctx, cache, 20, userID, func(c context.Context, uid, limit int) ([]FeedItem, error) {
		pullHits++
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
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
