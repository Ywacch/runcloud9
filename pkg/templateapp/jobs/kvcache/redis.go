package kvcache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const lockKeyPrefix = "c9:lock:"

// Redis implements Cache with go-redis.
type Redis struct {
	client redis.UniversalClient
}

// NewRedis wraps an existing Redis client.
func NewRedis(client redis.UniversalClient) (*Redis, error) {
	if client == nil {
		return nil, fmt.Errorf("kvcache: redis client is required")
	}
	return &Redis{client: client}, nil
}

// Engine returns "redis".
func (r *Redis) Engine() string {
	return "redis"
}

// Ping checks connectivity to the Redis server.
func (r *Redis) Ping(ctx context.Context) error {
	if err := r.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("kvcache: redis ping: %w", err)
	}
	return nil
}

// Get loads key when present.
func (r *Redis) Get(ctx context.Context, key string) ([]byte, bool, error) {
	val, err := r.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("kvcache: redis get: %w", err)
	}
	return val, true, nil
}

// Set stores value with an optional TTL.
func (r *Redis) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if err := r.client.Set(ctx, key, value, ttl).Err(); err != nil {
		return fmt.Errorf("kvcache: redis set: %w", err)
	}
	return nil
}

// Delete removes key.
func (r *Redis) Delete(ctx context.Context, key string) error {
	if err := r.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("kvcache: redis del: %w", err)
	}
	return nil
}

// Lock tries SET NX with ttl on a dedicated lock key.
func (r *Redis) Lock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	lockKey := lockKeyPrefix + key
	ok, err := r.client.SetNX(ctx, lockKey, "1", ttl).Result()
	if err != nil {
		return false, fmt.Errorf("kvcache: redis lock: %w", err)
	}
	return ok, nil
}
