// Package kvcache provides TTL key-value caches for template hot_cache jobs.
package kvcache

import (
	"context"
	"time"
)

// Cache is a byte-oriented cache with optional distributed locks.
type Cache interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
	Lock(ctx context.Context, key string, ttl time.Duration) (bool, error)
	Engine() string
}
