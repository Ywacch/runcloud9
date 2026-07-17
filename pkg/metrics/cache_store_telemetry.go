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
}

// fillRedisCacheStoreTelemetry reads redis_exporter series into normalized fields.
// Metric names stay inside this Redis filler; callers consume CacheStoreTelemetry only.
func (c *MetricsCollector) fillRedisCacheStoreTelemetry(ctx context.Context, ns string) CacheStoreTelemetry {
	if c == nil || ns == "" {
		return CacheStoreTelemetry{}
	}
	var out CacheStoreTelemetry
	evicted, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(redis_evicted_keys_total{namespace=%q}%s))`,
		ns, promRateRange(),
	))
	expired, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(redis_expired_keys_total{namespace=%q}%s))`,
		ns, promRateRange(),
	))
	if evicted > 0 {
		out.EvictionsPerSec = evicted
	}
	if expired > 0 {
		out.ExpirationsPerSec = expired
	}
	return out
}

func applyCacheStoreTelemetry(cm *ComponentMetrics, tel CacheStoreTelemetry) {
	if cm == nil {
		return
	}
	cm.EvictionsPerSec = tel.EvictionsPerSec
	cm.ExpirationsPerSec = tel.ExpirationsPerSec
}
