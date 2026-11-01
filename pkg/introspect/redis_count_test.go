package introspect

import (
	"context"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestCountStudentKeys(t *testing.T) {
	t.Parallel()

	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	t.Cleanup(srv.Close)

	srv.Set("user:1", "a")
	srv.Set("session:abc", "b")
	srv.Set("_c9_probe", "probe")
	srv.Set("_c9_other", "other")

	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	driver := &RedisDriver{client: client}
	count, err := driver.CountStudentKeys(context.Background())
	if err != nil {
		t.Fatalf("CountStudentKeys: %v", err)
	}
	if count != 2 {
		t.Fatalf("CountStudentKeys() = %d, want 2", count)
	}
}

func TestGetKeyValueStreamPreview(t *testing.T) {
	t.Parallel()

	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	t.Cleanup(srv.Close)

	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	for i := 0; i < 25; i++ {
		if err := client.XAdd(ctx, &redis.XAddArgs{
			Stream: "fanout",
			Values: map[string]interface{}{"body": fmt.Sprintf("m-%d", i)},
		}).Err(); err != nil {
			t.Fatalf("XAdd: %v", err)
		}
	}

	driver := &RedisDriver{client: client}
	kv, err := driver.GetKeyValue(ctx, "fanout")
	if err != nil {
		t.Fatalf("GetKeyValue: %v", err)
	}
	if kv.Type != "stream" {
		t.Fatalf("type %q", kv.Type)
	}
	entries, ok := kv.Value.([]StreamEntry)
	if !ok {
		t.Fatalf("value type %T", kv.Value)
	}
	if len(entries) != streamPreviewLimit {
		t.Fatalf("len %d, want %d", len(entries), streamPreviewLimit)
	}
	if entries[0].Fields["body"] != "m-5" {
		t.Fatalf("oldest kept body %q, want m-5", entries[0].Fields["body"])
	}
	if entries[len(entries)-1].Fields["body"] != "m-24" {
		t.Fatalf("newest body %q, want m-24", entries[len(entries)-1].Fields["body"])
	}
}
