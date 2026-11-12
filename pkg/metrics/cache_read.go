package metrics

import (
	"context"
	"fmt"

	"github.com/ywacch/cloud9/pkg/metrics/signals"
)

func (c *MetricsCollector) redisExporterCPURate(ctx context.Context, ns string) (float64, bool) {
	if c == nil || ns == "" {
		return 0, false
	}
	user, userOK := c.sig(ctx, ns, signals.RedisCPUUserRate)
	sys, sysOK := c.sig(ctx, ns, signals.RedisCPUSysRate)
	if !userOK && !sysOK {
		return 0, false
	}
	return user + sys, true
}

func (c *MetricsCollector) redisMemoryUsedBytes(ctx context.Context, ns string) (float64, bool) {
	return c.sig(ctx, ns, signals.RedisMemoryUsedBytes)
}

func (c *MetricsCollector) redisMemoryMaxBytes(ctx context.Context, ns string) (float64, bool) {
	return c.sig(ctx, ns, signals.RedisMemoryMaxBytes)
}

func (c *MetricsCollector) redisEvictedKeysRate(ctx context.Context, ns string) (float64, bool) {
	return c.sig(ctx, ns, signals.RedisEvictedKeysRate)
}

func (c *MetricsCollector) redisExpiredKeysRate(ctx context.Context, ns string) (float64, bool) {
	return c.sig(ctx, ns, signals.RedisExpiredKeysRate)
}

func (c *MetricsCollector) redisEvictedKeysByPod(ctx context.Context, ns string) map[string]float64 {
	if c == nil || ns == "" {
		return nil
	}
	m := c.sigByPod(ctx, ns, signals.RedisEvictedKeysTotal)
	if len(m) > 0 {
		return m
	}
	return c.queryMultiResult(ctx, fmt.Sprintf(
		`sum by (pod) (redis_evicted_keys_total{namespace=%q})`,
		ns,
	), "pod")
}

func (c *MetricsCollector) redisExpiredKeysByPod(ctx context.Context, ns string) map[string]float64 {
	if c == nil || ns == "" {
		return nil
	}
	m := c.sigByPod(ctx, ns, signals.RedisExpiredKeysTotal)
	if len(m) > 0 {
		return m
	}
	return c.queryMultiResult(ctx, fmt.Sprintf(
		`sum by (pod) (redis_expired_keys_total{namespace=%q})`,
		ns,
	), "pod")
}

func (c *MetricsCollector) memcachedCommandsRate(ctx context.Context, ns string) (float64, bool) {
	return c.sig(ctx, ns, signals.MemcachedCommandsRate)
}

func (c *MetricsCollector) memcachedGetHitRate(ctx context.Context, ns string) (float64, bool) {
	return c.sig(ctx, ns, signals.MemcachedGetHitRate)
}

func (c *MetricsCollector) memcachedGetMissRate(ctx context.Context, ns string) (float64, bool) {
	return c.sig(ctx, ns, signals.MemcachedGetMissRate)
}

func (c *MetricsCollector) memcachedCurrentConnections(ctx context.Context, ns string) (float64, bool) {
	return c.sig(ctx, ns, signals.MemcachedCurrentConnections)
}

func (c *MetricsCollector) memcachedConnectionsListenerDisabledRate(ctx context.Context, ns string) (float64, bool) {
	return c.sig(ctx, ns, signals.MemcachedConnectionsListenerDisabledRate)
}

func (c *MetricsCollector) memcachedItemsEvictedRate(ctx context.Context, ns string) (float64, bool) {
	return c.sig(ctx, ns, signals.MemcachedItemsEvictedRate)
}

func (c *MetricsCollector) memcachedItemsReclaimedRate(ctx context.Context, ns string) (float64, bool) {
	return c.sig(ctx, ns, signals.MemcachedItemsReclaimedRate)
}

func (c *MetricsCollector) memcachedItemsEvictedByPod(ctx context.Context, ns string) map[string]float64 {
	if c == nil || ns == "" {
		return nil
	}
	m := c.sigByPod(ctx, ns, signals.MemcachedItemsEvictedTotal)
	if len(m) > 0 {
		return m
	}
	return c.queryMultiResult(ctx, fmt.Sprintf(
		`sum by (pod) (memcached_items_evicted_total{namespace=%q})`,
		ns,
	), "pod")
}

func (c *MetricsCollector) memcachedItemsReclaimedByPod(ctx context.Context, ns string) map[string]float64 {
	if c == nil || ns == "" {
		return nil
	}
	m := c.sigByPod(ctx, ns, signals.MemcachedItemsReclaimedTotal)
	if len(m) > 0 {
		return m
	}
	return c.queryMultiResult(ctx, fmt.Sprintf(
		`sum by (pod) (memcached_items_reclaimed_total{namespace=%q})`,
		ns,
	), "pod")
}
