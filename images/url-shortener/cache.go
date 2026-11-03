package main

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ywacch/cloud9/pkg/templateapp/jobs/kvcache"
	"github.com/ywacch/cloud9/pkg/templateapp/trace"
)

type Cache struct {
	kv kvcache.Cache
	// UNDERSTAND: singleflightMu prevents cache stampede.
	inflightMu sync.Map // map[string]*sync.Mutex

	stampedeProtection bool
	ping               func(context.Context) error
	gate               *kvcache.FailGate
}

func newCache(kv kvcache.Cache, stampedeProtection bool, ping func(context.Context) error) *Cache {
	return &Cache{
		kv:                 kv,
		stampedeProtection: stampedeProtection,
		ping:               ping,
		gate:               kvcache.NewFailGate(nil),
	}
}

func (c *Cache) Ping(ctx context.Context) error {
	if c.ping == nil {
		return nil
	}
	return c.ping(ctx)
}

// errCacheUnavailable means the app did not get an answer from the cache.
// A miss is a nil error with ok false.
var errCacheUnavailable = errors.New("cache unavailable")

func (c *Cache) Get(ctx context.Context, code string) (string, bool, error) {
	start := time.Now()
	if c.gate != nil && !c.gate.Allow() {
		trace.LogCacheGet(ctx, code, false, start)
		return "", false, errCacheUnavailable
	}
	val, ok, err := c.kv.Get(ctx, code)
	if err != nil {
		if c.gate != nil {
			c.gate.Fail()
		}
		trace.LogCacheGet(ctx, code, false, start)
		return "", false, err
	}
	if c.gate != nil {
		c.gate.OK()
	}
	if !ok {
		trace.LogCacheGet(ctx, code, false, start)
		return "", false, nil
	}
	trace.LogCacheGet(ctx, code, true, start)
	return string(val), true, nil
}

func (c *Cache) Set(ctx context.Context, code string, url string, ttl time.Duration) error {
	start := time.Now()
	if c.gate != nil && !c.gate.Allow() {
		return nil
	}
	err := c.kv.Set(ctx, code, []byte(url), ttl)
	trace.LogCacheSet(ctx, code, start)
	if err != nil {
		c.gate.Fail()
		return err
	}
	c.gate.OK()
	return nil
}

// GetProtected wraps Get with a per-key mutex.
// If stampede protection is disabled, it falls through to a plain Get.
func (c *Cache) GetProtected(ctx context.Context, code string, fetchFn func() (string, error)) (string, error) {
	if !c.stampedeProtection {
		val, ok, err := c.Get(ctx, code)
		if err == nil && ok {
			return val, nil
		}
		return fetchFn()
	}

	// With protection: only one goroutine fetches per key.
	muAny, _ := c.inflightMu.LoadOrStore(code, &sync.Mutex{})
	mu := muAny.(*sync.Mutex)

	mu.Lock()
	defer mu.Unlock()

	// Another goroutine may have populated the cache while we were waiting.
	if val, ok, err := c.Get(ctx, code); err == nil && ok {
		return val, nil
	}
	return fetchFn()
}
