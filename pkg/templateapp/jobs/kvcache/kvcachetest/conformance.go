package kvcachetest

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/templateapp/jobs/kvcache"
)

// Factory returns a fresh cache and cleanup.
type Factory func(t *testing.T) (kvcache.Cache, func())

// RunConformance exercises get/set, delete, TTL, and lock behavior.
func RunConformance(t *testing.T, newCache Factory) {
	t.Helper()
	t.Run("get_after_set", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		c, cleanup := newCache(t)
		defer cleanup()
		const key = "conformance-get"
		body := []byte("value")
		if err := c.Set(ctx, key, body, time.Minute); err != nil {
			t.Fatalf("set: %v", err)
		}
		got, ok, err := c.Get(ctx, key)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if !ok || !bytes.Equal(got, body) {
			t.Fatalf("get: ok=%v got=%q", ok, got)
		}
	})

	t.Run("delete", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		c, cleanup := newCache(t)
		defer cleanup()
		const key = "conformance-del"
		if err := c.Set(ctx, key, []byte("x"), time.Minute); err != nil {
			t.Fatalf("set: %v", err)
		}
		if err := c.Delete(ctx, key); err != nil {
			t.Fatalf("delete: %v", err)
		}
		_, ok, err := c.Get(ctx, key)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if ok {
			t.Fatal("expected miss after delete")
		}
	})

	t.Run("lock_exclusivity", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		c, cleanup := newCache(t)
		defer cleanup()
		const key = "conformance-lock"
		first, err := c.Lock(ctx, key, time.Minute)
		if err != nil {
			t.Fatalf("lock: %v", err)
		}
		if !first {
			t.Fatal("expected first lock to succeed")
		}
		second, err := c.Lock(ctx, key, time.Minute)
		if err != nil {
			t.Fatalf("lock again: %v", err)
		}
		if second {
			t.Fatal("expected second lock to fail")
		}
	})
}

// RunTTLConformance requires advance to expire keys. advance moves fake or miniredis time.
func RunTTLConformance(t *testing.T, newCache Factory, advance func(d time.Duration)) {
	t.Helper()
	ctx := context.Background()
	c, cleanup := newCache(t)
	defer cleanup()
	const key = "conformance-ttl"
	if err := c.Set(ctx, key, []byte("ttl"), 2*time.Second); err != nil {
		t.Fatalf("set: %v", err)
	}
	if advance != nil {
		advance(3 * time.Second)
	} else {
		time.Sleep(3 * time.Second)
	}
	_, ok, err := c.Get(ctx, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if ok {
		t.Fatal("expected TTL expiry miss")
	}
}

// RunLockExpiryConformance requires advance to release locks.
func RunLockExpiryConformance(t *testing.T, newCache Factory, advance func(d time.Duration)) {
	t.Helper()
	ctx := context.Background()
	c, cleanup := newCache(t)
	defer cleanup()
	const key = "conformance-lock-expiry"
	ok, err := c.Lock(ctx, key, time.Second)
	if err != nil || !ok {
		t.Fatalf("lock: ok=%v err=%v", ok, err)
	}
	if advance != nil {
		advance(2 * time.Second)
	} else {
		time.Sleep(2 * time.Second)
	}
	ok, err = c.Lock(ctx, key, time.Second)
	if err != nil {
		t.Fatalf("lock after expiry: %v", err)
	}
	if !ok {
		t.Fatal("expected lock after TTL expiry")
	}
}
