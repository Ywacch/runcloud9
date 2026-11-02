package kvcache_test

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/ywacch/cloud9/pkg/templateapp/jobs/kvcache"
	"github.com/ywacch/cloud9/pkg/templateapp/jobs/kvcache/kvcachetest"
)

func TestRedis_Conformance(t *testing.T) {
	t.Parallel()
	kvcachetest.RunConformance(t, func(t *testing.T) (kvcache.Cache, func()) {
		t.Helper()
		srv, err := miniredis.Run()
		if err != nil {
			t.Fatal(err)
		}
		client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
		c, err := kvcache.NewRedis(client)
		if err != nil {
			t.Fatal(err)
		}
		return c, func() {
			_ = client.Close()
			srv.Close()
		}
	})
}

func TestRedis_TTLAndLockExpiry(t *testing.T) {
	t.Parallel()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)
	client := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	c, err := kvcache.NewRedis(client)
	if err != nil {
		t.Fatal(err)
	}
	advance := func(d time.Duration) {
		srv.FastForward(d)
	}
	kvcachetest.RunTTLConformance(t, func(t *testing.T) (kvcache.Cache, func()) {
		t.Helper()
		return c, func() {}
	}, advance)
	kvcachetest.RunLockExpiryConformance(t, func(t *testing.T) (kvcache.Cache, func()) {
		t.Helper()
		return c, func() {}
	}, advance)
}
