package metrics

import (
	"context"
	"fmt"
	"math"
	"time"
)

// applyMemcachedCacheMetrics fills cache fields for a Memcached deployment.
// CPU and memory are the memcached container, not the exporter and not item bytes.
// Order matches Postgres: cAdvisor, then the metrics API, then kube requests.
// memcached_current_bytes is cached item payload. A quiet cache is a few KiB,
// so that gauge makes the memory tile look empty.
func (c *MetricsCollector) applyMemcachedCacheMetrics(ctx context.Context, ns string, cm *ComponentMetrics) {
	ctx = withPromCaller(ctx, "cache")
	if c == nil || cm == nil || ns == "" {
		return
	}
	cpuRes := c.queryContainerResourceCPU(ctx, ns, "memcached.*", "memcached")
	cm.CPUUsage = cpuRes.Usage
	cm.CPUIsEstimated = cpuRes.Estimated
	cm.CPUSource = cpuRes.Source
	memRes := c.queryContainerResourceMemory(ctx, ns, "memcached.*", "memcached")
	cm.MemoryBytes = int64(memRes.Usage)
	cm.MemIsEstimated = memRes.Estimated
	cm.MemSource = memRes.Source
	cm.MemoryLimit = c.kubeLimitMem(ctx, ns, "memcached.*", "memcached")
	ops, opsOK := c.memcachedCommandsRate(ctx, ns)
	if opsOK && ops > 0 {
		cm.RedisOpsPerSec = ops
		cm.RequestRate = ops
	}
	hits, _ := c.memcachedGetHitRate(ctx, ns)
	misses, _ := c.memcachedGetMissRate(ctx, ns)
	cm.RedisKeyspaceHits = hits
	cm.RedisKeyspaceMisses = misses
	conns, connsOK := c.memcachedCurrentConnections(ctx, ns)
	if connsOK && conns > 0 {
		cm.ActiveConnections = conns
	}
	rejected, _ := c.memcachedConnectionsListenerDisabledRate(ctx, ns)
	cm.RedisRejectedRate = rejected
	cm.CPULimit = c.kubeLimitCPU(ctx, ns, "memcached.*", "memcached")
	c.applyMemcachedItemLifecycle(ctx, ns, cm)
	// Redis latency is server-side and does not apply here.
	cm.P50Latency = 0
	cm.P95Latency = 0
	cm.P99Latency = 0
	cm.P99Latency9m = 0
	cm.AvgLatency = 0
	c.applyMemcachedClientLatency(ctx, ns, cm)
}

// applyMemcachedItemLifecycle reads slab evictions and expired-slot reclaims.
// TTL expiry does not increment evictions.
func (c *MetricsCollector) applyMemcachedItemLifecycle(ctx context.Context, ns string, cm *ComponentMetrics) {
	evicted, _ := c.memcachedItemsEvictedRate(ctx, ns)
	reclaimed, _ := c.memcachedItemsReclaimedRate(ctx, ns)
	cm.EvictionsPerSec = evicted
	cm.ExpirationsPerSec = reclaimed
	evictedByPod := c.memcachedItemsEvictedByPod(ctx, ns)
	reclaimedByPod := c.memcachedItemsReclaimedByPod(ctx, ns)
	cm.EvictionsLast9m = c.sessionAcc.Observe(ns, seriesMemcachedEvicted, evictedByPod)
	cm.ExpirationsLast9m = c.sessionAcc.Observe(ns, seriesMemcachedReclaimed, reclaimedByPod)
}

// applyMemcachedClientLatency reads cache_op_duration_seconds from the app.
// The number is the round trip, not time inside Memcached.
func (c *MetricsCollector) applyMemcachedClientLatency(ctx context.Context, ns string, cm *ComponentMetrics) {
	live := fmt.Sprintf(
		`sum(rate(cache_op_duration_seconds_bucket{namespace=%q,engine="memcached"}%s)) by (le)`,
		ns, promRateRange(),
	)
	p50, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.50, %s)`, live))
	p95, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.95, %s)`, live))
	p99, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.99, %s)`, live))
	if finitePositive(p99) {
		cm.P50Latency = p50
		cm.P95Latency = p95
		cm.P99Latency = p99
	}
	sessionRange := sessionP99PromRange(c.sessionAcc.Elapsed(ns, time.Now()))
	session := fmt.Sprintf(
		`sum(rate(cache_op_duration_seconds_bucket{namespace=%q,engine="memcached"}%s)) by (le)`,
		ns, sessionRange,
	)
	p99Session, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.99, %s)`, session))
	if finitePositive(p99Session) {
		cm.P99Latency9m = p99Session
	}
	sum, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(cache_op_duration_seconds_sum{namespace=%q,engine="memcached"}%s))`,
		ns, promRateRange(),
	))
	count, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(cache_op_duration_seconds_count{namespace=%q,engine="memcached"}%s))`,
		ns, promRateRange(),
	))
	if count > 0 && finitePositive(sum/count) {
		cm.AvgLatency = sum / count
	}
}

func finitePositive(v float64) bool {
	return v > 0 && !math.IsNaN(v) && !math.IsInf(v, 0)
}
