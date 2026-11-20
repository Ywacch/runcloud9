package introspect

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestIntrospect_StreamPeekShape(t *testing.T) {
	t.Parallel()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer srv.Close()

	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	const stream = "fanout"
	if _, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{"body": `{"postId":1}`},
	}).Result(); err != nil {
		t.Fatalf("XAdd: %v", err)
	}
	_ = client.XGroupCreateMkStream(ctx, stream, StreamsWorkerGroup, "0").Err()

	drv := &RedisDriver{client: client}
	peek, extra, err := drv.PeekStream(ctx, stream)
	if err != nil {
		t.Fatalf("PeekStream: %v", err)
	}
	if peek.Queue != stream || peek.Empty || peek.Requeued {
		t.Fatalf("peek: %+v", peek)
	}
	if peek.Body == "" {
		t.Fatal("expected body")
	}

	raw, err := json.Marshal(struct {
		PeekResult
		StreamPeekExtra
	}{peek, extra})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{"queue", "empty", "body", "contentType", "requeued", "pending", "lag"} {
		if _, ok := m[key]; !ok {
			t.Fatalf("missing key %q in %v", key, m)
		}
	}
}
