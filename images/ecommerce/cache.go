package main

import (
	"context"
	"sync"
	"time"

	"github.com/ywacch/cloud9/pkg/templateapp/jobs/kvcache"
	"github.com/ywacch/cloud9/pkg/templateapp/trace"
)

type Cache struct {
	kv kvcache.Cache
	// singleflight-like per-key mutex to avoid stampedes.
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

func (c *Cache) Get(ctx context.Context, key string) (string, bool) {
	start := time.Now()
	if c.gate != nil && !c.gate.Allow() {
		trace.LogCacheGet(ctx, key, false, start)
		return "", false
	}
	val, ok, err := c.kv.Get(ctx, key)
	if err != nil {
		c.gate.Fail()
		trace.LogCacheGet(ctx, key, false, start)
		return "", false
	}
	c.gate.OK()
	if !ok {
		trace.LogCacheGet(ctx, key, false, start)
		return "", false
	}
	trace.LogCacheGet(ctx, key, true, start)
	return string(val), true
}

func (c *Cache) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	start := time.Now()
	if c.gate != nil && !c.gate.Allow() {
		return nil
	}
	err := c.kv.Set(ctx, key, []byte(value), ttl)
	trace.LogCacheSet(ctx, key, start)
	if err != nil {
		c.gate.Fail()
		return err
	}
	c.gate.OK()
	return nil
}

func (c *Cache) Delete(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	if c.gate != nil && !c.gate.Allow() {
		return nil
	}
	start := time.Now()
	var err error
	for _, key := range keys {
		if delErr := c.kv.Delete(ctx, key); delErr != nil {
			err = delErr
		}
	}
	trace.LogCacheDelete(ctx, start)
	if err != nil {
		c.gate.Fail()
		return err
	}
	c.gate.OK()
	return nil
}

func (c *Cache) GetProtected(ctx context.Context, key string, fetchFn func() (string, error)) (string, error) {
	if !c.stampedeProtection {
		val, ok := c.Get(ctx, key)
		if ok {
			return val, nil
		}
		return fetchFn()
	}

	muAny, _ := c.inflightMu.LoadOrStore(key, &sync.Mutex{})
	mu := muAny.(*sync.Mutex)

	mu.Lock()
	defer mu.Unlock()

	if val, ok := c.Get(ctx, key); ok {
		return val, nil
	}
	return fetchFn()
}
