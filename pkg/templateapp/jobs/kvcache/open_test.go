package kvcache_test

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/redis/go-redis/v9"
	"github.com/ywacch/cloud9/pkg/templateapp/jobs/kvcache"
)

func TestOpen_Redis(t *testing.T) {
	t.Parallel()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	c, err := kvcache.Open(kvcache.Config{
		EngineEnv:   "redis",
		HostEnv:     "ignored",
		PortEnv:     "6379",
		RedisClient: client,
	})
	if err != nil {
		t.Fatal(err)
	}
	if c.Engine() != "redis" {
		t.Fatalf("engine: %q", c.Engine())
	}
}

func TestOpen_UnsupportedEngine(t *testing.T) {
	t.Parallel()
	_, err := kvcache.Open(kvcache.Config{
		EngineEnv: "kafka",
		HostEnv:   "cache",
		PortEnv:   "9092",
	})
	if err == nil {
		t.Fatal("expected error for unknown engine")
	}
}

func TestOpen_Memcached(t *testing.T) {
	t.Parallel()
	c, err := kvcache.Open(kvcache.Config{
		EngineEnv:       "memcached",
		HostEnv:         "127.0.0.1",
		PortEnv:         "11211",
		MemcachedClient: memcache.New("127.0.0.1:11211"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if c.Engine() != "memcached" {
		t.Fatalf("engine: %q", c.Engine())
	}
}
