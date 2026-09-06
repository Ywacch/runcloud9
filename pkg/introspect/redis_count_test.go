package introspect

import (
	"context"
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
