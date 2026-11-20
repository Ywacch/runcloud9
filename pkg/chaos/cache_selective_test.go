package chaos

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestCacheFlush_KeepsStream(t *testing.T) {
	t.Parallel()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer srv.Close()

	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	streamKey := "orders-stream"
	if err := client.XAdd(ctx, &redis.XAddArgs{Stream: streamKey, Values: map[string]any{"body": "x"}}).Err(); err != nil {
		t.Fatalf("XAdd: %v", err)
	}
	if err := client.Set(ctx, "product:1", "v", 0).Err(); err != nil {
		t.Fatalf("Set product: %v", err)
	}
	if err := client.Set(ctx, "timeline:1", "v", 0).Err(); err != nil {
		t.Fatalf("Set timeline: %v", err)
	}

	if err := selectiveUnlinkKeyPatterns(ctx, client, []string{"product:*"}); err != nil {
		t.Fatalf("selectiveUnlinkKeyPatterns: %v", err)
	}

	if srv.Exists("product:1") {
		t.Fatal("product:1 should be removed")
	}
	if !srv.Exists(streamKey) {
		t.Fatal("stream key should remain")
	}
	if !srv.Exists("timeline:1") {
		t.Fatal("timeline:1 should remain when pattern is product:* only")
	}
}
