package probes

import (
	"context"
	"fmt"
	"strings"
)

type cacheDialFunc func(ctx context.Context, namespace string, opts CacheDialOptions) (CacheProbe, error)

func defaultCacheDial(ctx context.Context, namespace string, opts CacheDialOptions) (CacheProbe, error) {
	engine := strings.TrimSpace(strings.ToLower(opts.Engine))
	if engine == "" {
		engine = "redis"
	}
	switch engine {
	case "redis":
		host := redisCacheServiceHost(opts.HasProxyCache)
		fqdn := fmt.Sprintf("%s.%s.svc.cluster.local", host, namespace)
		return NewRedisCacheProbe(ctx, fqdn)
	default:
		return nil, ErrCacheProbeUnsupported
	}
}

func redisCacheServiceHost(hasProxyCache bool) string {
	if hasProxyCache {
		return "redis-internal"
	}
	return "cache"
}
