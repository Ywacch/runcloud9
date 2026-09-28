package main

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestCountTimelineKeys(t *testing.T) {
	t.Parallel()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)

	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	ctx := context.Background()

	if n, err := countTimelineKeys(ctx, rdb); err != nil || n != 0 {
		t.Fatalf("empty: n=%d err=%v", n, err)
	}
	if err := rdb.Set(ctx, "timeline:1", "x", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, "timeline:2", "y", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, "other:3", "z", 0).Err(); err != nil {
		t.Fatal(err)
	}
	n, err := countTimelineKeys(ctx, rdb)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("want 2 timeline keys, got %d", n)
	}
}
