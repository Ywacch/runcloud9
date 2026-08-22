package probes

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisCacheProbe implements CacheProbe against a single Redis instance.
type RedisCacheProbe struct {
	client *redis.Client
}

// NewRedisCacheProbe connects to Redis at host:6379 (host is FQDN or service DNS).
func NewRedisCacheProbe(ctx context.Context, host string) (*RedisCacheProbe, error) {
	client := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:6379", host),
	})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis cache probe ping: %w", err)
	}
	return &RedisCacheProbe{client: client}, nil
}

// Get reads a logical version from Redis.
func (p *RedisCacheProbe) Get(ctx context.Context, key string) (ConsistencyToken, bool, error) {
	if p == nil || p.client == nil {
		return nil, false, fmt.Errorf("redis cache probe: not connected")
	}
	val, err := p.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	tok, err := ParseInt64Token(val)
	if err != nil {
		return nil, true, err
	}
	return tok, true, nil
}

// Set stores a logical version in Redis with TTL.
func (p *RedisCacheProbe) Set(ctx context.Context, key string, tok ConsistencyToken, ttl time.Duration) error {
	if p == nil || p.client == nil {
		return fmt.Errorf("redis cache probe: not connected")
	}
	it, ok := tok.(Int64Token)
	if !ok {
		return fmt.Errorf("redis cache probe: unsupported token type %T", tok)
	}
	return p.client.Set(ctx, key, FormatInt64Token(it), ttl).Err()
}

// Close releases the Redis client.
func (p *RedisCacheProbe) Close() error {
	if p == nil || p.client == nil {
		return nil
	}
	return p.client.Close()
}
