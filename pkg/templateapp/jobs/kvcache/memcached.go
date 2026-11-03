package kvcache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)

// Memcached implements Cache against a Memcached server.
type Memcached struct {
	client *memcache.Client
}

// NewMemcached wraps an existing memcache client.
func NewMemcached(client *memcache.Client) (*Memcached, error) {
	if client == nil {
		return nil, fmt.Errorf("kvcache: memcached client is required")
	}
	return &Memcached{client: client}, nil
}

// Engine returns "memcached".
func (m *Memcached) Engine() string {
	return "memcached"
}

// Ping checks connectivity with the memcache client ping.
func (m *Memcached) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := m.client.Ping(); err != nil {
		return fmt.Errorf("kvcache: memcached ping: %w", err)
	}
	return nil
}

// Get loads key when present.
func (m *Memcached) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	start := time.Now()
	item, err := m.client.Get(key)
	if errors.Is(err, memcache.ErrCacheMiss) {
		observeCacheOp(m.Engine(), "get", "miss", start)
		return nil, false, nil
	}
	if err != nil {
		observeCacheOp(m.Engine(), "get", cacheOpResult("get", false, err), start)
		return nil, false, fmt.Errorf("kvcache: memcached get: %w", err)
	}
	observeCacheOp(m.Engine(), "get", "hit", start)
	return item.Value, true, nil
}

// Set stores value with an optional TTL.
func (m *Memcached) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	exp := int32(0)
	if ttl > 0 {
		sec := int32(ttl.Round(time.Second) / time.Second)
		if sec < 1 {
			sec = 1
		}
		exp = sec
	}
	start := time.Now()
	err := m.client.Set(&memcache.Item{Key: key, Value: value, Expiration: exp})
	observeCacheOp(m.Engine(), "set", cacheOpResult("set", false, err), start)
	if err != nil {
		return fmt.Errorf("kvcache: memcached set: %w", err)
	}
	return nil
}

// Delete removes key.
func (m *Memcached) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	start := time.Now()
	err := m.client.Delete(key)
	if errors.Is(err, memcache.ErrCacheMiss) {
		observeCacheOp(m.Engine(), "delete", "ok", start)
		return nil
	}
	observeCacheOp(m.Engine(), "delete", cacheOpResult("delete", false, err), start)
	if err != nil {
		return fmt.Errorf("kvcache: memcached delete: %w", err)
	}
	return nil
}

// Lock uses ADD with ttl (exclusive while the item lives).
func (m *Memcached) Lock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	exp := int32(1)
	if ttl > 0 {
		sec := int32(ttl.Round(time.Second) / time.Second)
		if sec < 1 {
			sec = 1
		}
		exp = sec
	}
	err := m.client.Add(&memcache.Item{
		Key:        lockKeyPrefix + key,
		Value:      []byte("1"),
		Expiration: exp,
	})
	if errors.Is(err, memcache.ErrNotStored) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("kvcache: memcached lock: %w", err)
	}
	return true, nil
}
