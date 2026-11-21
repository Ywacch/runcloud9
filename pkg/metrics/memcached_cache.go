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
func (c *MetricsCollector) applyMemcachedCacheMetrics(ctx context.Context, labelSel string, cm *ComponentMetrics) bool {
	ctx = withPromCaller(ctx, "cache")
	if c == nil || cm == nil || labelSel == "" {
		return false
	}
	ns := namespaceFromPromSel(labelSel)
	if ns == "" {
		return false
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
	ops, opsOK := c.memcachedExporterRate(ctx, labelSel, "memcached_commands_total")
	if opsOK && ops > 0 {
		cm.RedisOpsPerSec = ops
		cm.RequestRate = ops
	}
	hits, _ := c.memcachedExporterRate(ctx, labelSel, `memcached_commands_total`, `cmd="get_hits"`)
	misses, _ := c.memcachedExporterRate(ctx, labelSel, `memcached_commands_total`, `cmd="get_misses"`)
	cm.RedisKeyspaceHits = hits
	cm.RedisKeyspaceMisses = misses
	conns, connsOK := c.memcachedExporterScalar(ctx, labelSel, `memcached_current_connections`)
	if connsOK && conns > 0 {
		cm.ActiveConnections = conns
	}
	rejected, _ := c.memcachedExporterRate(ctx, labelSel, `memcached_connections_listener_disabled_total`)
	cm.RedisRejectedRate = rejected
	cm.CPULimit = c.kubeLimitCPU(ctx, ns, "memcached.*", "memcached")
	c.applyMemcachedItemLifecycle(ctx, labelSel, ns, cm)
	return c.applyMemcachedClientLatency(ctx, labelSel, ns, cm)
}

func (c *MetricsCollector) memcachedExporterRate(ctx context.Context, labelSel, metric string, extraLabels ...string) (float64, bool) {
	extra := ""
	if len(extraLabels) > 0 && extraLabels[0] != "" {
		extra = "," + extraLabels[0]
	}
	v, present, _ := c.queryScalar(ctx, fmt.Sprintf(
		`sum(rate(%s{%s%s}%s))`, metric, labelSel, extra, promRateRange(),
	))
	return v, present
}

func (c *MetricsCollector) memcachedExporterScalar(ctx context.Context, labelSel, metric string) (float64, bool) {
	v, present, _ := c.queryScalar(ctx, fmt.Sprintf(`sum(%s{%s})`, metric, labelSel))
	return v, present
}

// applyMemcachedItemLifecycle reads slab evictions and expired-slot reclaims.
// TTL expiry does not increment evictions.
func (c *MetricsCollector) applyMemcachedItemLifecycle(ctx context.Context, labelSel, ns string, cm *ComponentMetrics) {
	evicted, _ := c.memcachedExporterRate(ctx, labelSel, `memcached_items_evicted_total`)
	reclaimed, _ := c.memcachedExporterRate(ctx, labelSel, `memcached_items_reclaimed_total`)
	cm.EvictionsPerSec = evicted
	cm.ExpirationsPerSec = reclaimed
	evictedByPod := c.memcachedItemsEvictedByPod(ctx, ns)
	reclaimedByPod := c.memcachedItemsReclaimedByPod(ctx, ns)
	cm.EvictionsLast9m = c.sessionAcc.Observe(ns, seriesMemcachedEvicted, evictedByPod)
	cm.ExpirationsLast9m = c.sessionAcc.Observe(ns, seriesMemcachedReclaimed, reclaimedByPod)
}

// applyMemcachedClientLatency reads cache_op_duration_seconds from the app.
// The number is the round trip, not time inside Memcached.
func (c *MetricsCollector) applyMemcachedClientLatency(ctx context.Context, labelSel, ns string, cm *ComponentMetrics) bool {
	live := fmt.Sprintf(
		`sum(rate(cache_op_duration_seconds_bucket{%s,engine="memcached"}%s)) by (le)`,
		labelSel, promRateRange(),
	)
	p50, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.50, %s)`, live))
	p95, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.95, %s)`, live))
	p99, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.99, %s)`, live))
	had := false
	if finitePositive(p99) {
		cm.P50Latency = p50
		cm.P95Latency = p95
		cm.P99Latency = p99
		had = true
	} else {
		cm.P50Latency = 0
		cm.P95Latency = 0
		cm.P99Latency = 0
	}
	sessionRange := sessionP99PromRange(c.sessionAcc.Elapsed(ns, time.Now()))
	session := fmt.Sprintf(
		`sum(rate(cache_op_duration_seconds_bucket{%s,engine="memcached"}%s)) by (le)`,
		labelSel, sessionRange,
	)
	p99Session, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.99, %s)`, session))
	if finitePositive(p99Session) {
		cm.P99Latency9m = p99Session
	} else {
		cm.P99Latency9m = 0
	}
	sum, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(cache_op_duration_seconds_sum{%s,engine="memcached"}%s))`,
		labelSel, promRateRange(),
	))
	count, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(cache_op_duration_seconds_count{%s,engine="memcached"}%s))`,
		labelSel, promRateRange(),
	))
	if count > 0 && finitePositive(sum/count) {
		cm.AvgLatency = sum / count
		had = true
	} else {
		cm.AvgLatency = 0
	}
	return had
}

func finitePositive(v float64) bool {
	return v > 0 && !math.IsNaN(v) && !math.IsInf(v, 0)
}
