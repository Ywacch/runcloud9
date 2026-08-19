package main

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/ywacch/cloud9/pkg/templateapp/trace"
)

type Cache struct {
	client *redis.Client
	// UNDERSTAND: singleflightMu prevents cache stampede.
	inflightMu sync.Map // map[string]*sync.Mutex

	stampedeProtection bool
}

func newCache(client *redis.Client, stampedeProtection bool) *Cache {
	return &Cache{
		client:             client,
		stampedeProtection: stampedeProtection,
	}
}

func (c *Cache) Get(ctx context.Context, code string) (string, bool) {
	start := time.Now()
	val, err := c.client.Get(ctx, code).Result()
	if err == redis.Nil || err != nil {
		trace.LogCacheGet(ctx, code, false, start)
		return "", false
	}
	trace.LogCacheGet(ctx, code, true, start)
	return val, true
}

func (c *Cache) Set(ctx context.Context, code string, url string, ttl time.Duration) error {
	start := time.Now()
	err := c.client.Set(ctx, code, url, ttl).Err()
	trace.LogCacheSet(ctx, code, start)
	return err
}

// GetProtected wraps Get with a per-key mutex.
// If stampede protection is disabled, it falls through to a plain Get.
func (c *Cache) GetProtected(ctx context.Context, code string, fetchFn func() (string, error)) (string, error) {
	if !c.stampedeProtection {
		val, ok := c.Get(ctx, code)
		if ok {
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
	if val, ok := c.Get(ctx, code); ok {
		return val, nil
	}
	return fetchFn()
}
