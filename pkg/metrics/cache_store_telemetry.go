package metrics

import (
	"context"
	"fmt"
)

// CacheStoreTelemetry is engine-agnostic backing-store telemetry (L0).
// Redis fills these today; Memcached/Scylla adapters map into the same fields later.
type CacheStoreTelemetry struct {
	// EvictionsPerSec is memory-pressure / policy-driven key removals per second.
	EvictionsPerSec float64
	// ExpirationsPerSec is TTL-driven key removals per second.
	ExpirationsPerSec float64
	EvictionsLast9m   float64
	ExpirationsLast9m float64
}

// fillRedisCacheStoreTelemetry reads redis_exporter series into normalized fields.
// Metric names stay inside this Redis filler; callers consume CacheStoreTelemetry only.
func (c *MetricsCollector) fillRedisCacheStoreTelemetry(ctx context.Context, labelSel string) CacheStoreTelemetry {
	ctx = withPromCaller(ctx, "cache")
	if c == nil || labelSel == "" {
		return CacheStoreTelemetry{}
	}
	ns := namespaceFromPromSel(labelSel)
	var out CacheStoreTelemetry
	if c.collectProm.cacheRedis != "" {
		evicted, present, _ := c.queryScalar(ctx, fmt.Sprintf(
			`sum(rate(redis_evicted_keys_total{%s}%s))`, labelSel, promRateRange(),
		))
		if present {
			out.EvictionsPerSec = evicted
		}
		expired, present, _ := c.queryScalar(ctx, fmt.Sprintf(
			`sum(rate(redis_expired_keys_total{%s}%s))`, labelSel, promRateRange(),
		))
		if present {
			out.ExpirationsPerSec = expired
		}
		evictedByPod := c.queryMultiResult(ctx, fmt.Sprintf(
			`sum by (pod) (redis_evicted_keys_total{%s})`, labelSel,
		), "pod")
		expiredByPod := c.queryMultiResult(ctx, fmt.Sprintf(
			`sum by (pod) (redis_expired_keys_total{%s})`, labelSel,
		), "pod")
		out.EvictionsLast9m = c.sessionAcc.Observe(ns, seriesRedisEvicted, evictedByPod)
		out.ExpirationsLast9m = c.sessionAcc.Observe(ns, seriesRedisExpired, expiredByPod)
		return out
	}
	evicted, evictedOK := c.redisEvictedKeysRate(ctx, ns)
	expired, expiredOK := c.redisExpiredKeysRate(ctx, ns)
	if evictedOK && evicted > 0 {
		out.EvictionsPerSec = evicted
	}
	if expiredOK && expired > 0 {
		out.ExpirationsPerSec = expired
	}
	evictedByPod := c.redisEvictedKeysByPod(ctx, ns)
	expiredByPod := c.redisExpiredKeysByPod(ctx, ns)
	out.EvictionsLast9m = c.sessionAcc.Observe(ns, seriesRedisEvicted, evictedByPod)
	out.ExpirationsLast9m = c.sessionAcc.Observe(ns, seriesRedisExpired, expiredByPod)
	return out
}

func applyCacheStoreTelemetry(cm *ComponentMetrics, tel CacheStoreTelemetry) {
	if cm == nil {
		return
	}
	cm.EvictionsPerSec = tel.EvictionsPerSec
	cm.ExpirationsPerSec = tel.ExpirationsPerSec
	cm.EvictionsLast9m = tel.EvictionsLast9m
	cm.ExpirationsLast9m = tel.ExpirationsLast9m
}
