package probes

import (
	"context"
	"errors"
	"time"
)

const (
	// CacheProbeKey is the Redis key used for cache↔DB staleness checks.
	CacheProbeKey = ProbeTableName

	defaultCacheProbeTTL = 300 * time.Second
)

// ErrCacheProbeUnsupported is returned when the cache engine cannot be probed from ganymede.
var ErrCacheProbeUnsupported = errors.New("cache probe unsupported for this engine")

// CacheProbe reads and writes logical versions on a remote cache store.
type CacheProbe interface {
	Get(ctx context.Context, key string) (tok ConsistencyToken, hit bool, err error)
	Set(ctx context.Context, key string, tok ConsistencyToken, ttl time.Duration) error
	Close() error
}

// CacheDialOptions configures how ganymede dials the backing cache for probes.
type CacheDialOptions struct {
	Engine        string
	HasProxyCache bool
	ProbeTTL      time.Duration
}

// DefaultCacheDialOptions returns Redis probe defaults (url-shortener TTL when unset).
func DefaultCacheDialOptions(hasProxyCache bool) CacheDialOptions {
	return CacheDialOptions{
		Engine:        "redis",
		HasProxyCache: hasProxyCache,
		ProbeTTL:      defaultCacheProbeTTL,
	}
}
