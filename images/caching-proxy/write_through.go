package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// canonicalProxyStrategy folds legacy STRATEGY=read-through to write-through.
// GET miss-fill (proxyQueries) remains always-on regardless of write strategy.
func canonicalProxyStrategy(strategy string) string {
	s := strings.TrimSpace(strategy)
	if strings.EqualFold(s, "read-through") {
		return "write-through"
	}
	return s
}

// Sentinel errors so HTTP handlers can map failures without string matching.
var (
	errWriteThroughCache = errors.New("cache write failed")
	errWriteThroughDB    = errors.New("db write failed")
)

// urlPersister is the durable-store side of write-through (Postgres UPSERT).
type urlPersister interface {
	SaveURL(ctx context.Context, code, original string) error
}

// keyValueCache is the cache side of write-through (Redis SET / DEL).
type keyValueCache interface {
	Set(ctx context.Context, key, value string, ttl time.Duration) error
	Del(ctx context.Context, key string) error
}

// redisKV adapts *redis.Client to keyValueCache.
type redisKV struct {
	c *redis.Client
}

func (r redisKV) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	return r.c.Set(ctx, key, value, ttl).Err()
}

func (r redisKV) Del(ctx context.Context, key string) error {
	return r.c.Del(ctx, key).Err()
}

// writeThrough applies textbook write-through ordering:
//  1. SET cache entry
//  2. persist to DB
//  3. on DB failure, DEL the cache key (compensating rollback) so readers never see a phantom
//
// Ack is only successful when both stores are updated. Redis SET failure returns before any DB write.
func writeThrough(ctx context.Context, cache keyValueCache, store urlPersister, code, value string, ttl time.Duration) error {
	if err := cache.Set(ctx, code, value, ttl); err != nil {
		return fmt.Errorf("%w: %w", errWriteThroughCache, err)
	}
	if err := store.SaveURL(ctx, code, value); err != nil {
		if delErr := cache.Del(ctx, code); delErr != nil {
			slog.Error("write-through rollback DEL failed",
				"code", code,
				"db_error", err,
				"del_error", delErr,
			)
		}
		return fmt.Errorf("%w: %w", errWriteThroughDB, err)
	}
	return nil
}
