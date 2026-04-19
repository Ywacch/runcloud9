package main

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Cache struct {
	client *redis.Client
	// singleflight-like per-key mutex to avoid stampedes.
	inflightMu sync.Map // map[string]*sync.Mutex

	stampedeProtection bool
}

func newCache(client *redis.Client, stampedeProtection bool) *Cache {
	return &Cache{
		client:             client,
		stampedeProtection: stampedeProtection,
	}
}

func (c *Cache) Get(ctx context.Context, key string) (string, bool) {
	start := time.Now()
	val, err := c.client.Get(ctx, key).Result()
	if err == redis.Nil || err != nil {
		logCacheGet(ctx, key, false, start)
		return "", false
	}
	logCacheGet(ctx, key, true, start)
	return val, true
}

func (c *Cache) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	start := time.Now()
	err := c.client.Set(ctx, key, value, ttl).Err()
	logCacheSet(ctx, key, start)
	return err
}

func (c *Cache) Delete(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	start := time.Now()
	err := c.client.Del(ctx, keys...).Err()
	logCacheDelete(ctx, start)
	return err
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
