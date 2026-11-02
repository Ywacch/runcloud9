package metrics

import (
	"context"
	"fmt"
)

// applyMemcachedCacheMetrics fills cache fields for a Memcached deployment.
// CPU and memory are the memcached container, not the exporter and not item bytes.
// Order matches Postgres: cAdvisor, then the metrics API, then kube requests.
// memcached_current_bytes is cached item payload. A quiet cache is a few KiB,
// so that gauge makes the memory tile look empty.
func (c *MetricsCollector) applyMemcachedCacheMetrics(ctx context.Context, ns string, cm *ComponentMetrics) {
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
	ops, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(memcached_commands_total{namespace=%q}%s))`,
		ns, promRateRange(),
	))
	if ops > 0 {
		cm.RedisOpsPerSec = ops
		cm.RequestRate = ops
	}
	hits, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(memcached_commands_total{namespace=%q,command="get",status="hit"}%s))`,
		ns, promRateRange(),
	))
	misses, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(memcached_commands_total{namespace=%q,command="get",status="miss"}%s))`,
		ns, promRateRange(),
	))
	cm.RedisKeyspaceHits = hits
	cm.RedisKeyspaceMisses = misses
	evicted, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(memcached_items_evicted_total{namespace=%q}%s))`,
		ns, promRateRange(),
	))
	cm.EvictionsPerSec = evicted
	conns, _ := c.query(ctx, fmt.Sprintf(`sum(memcached_current_connections{namespace=%q})`, ns))
	if conns > 0 {
		cm.ActiveConnections = conns
	}
	rejected, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(memcached_connections_listener_disabled_total{namespace=%q}%s))`,
		ns, promRateRange(),
	))
	cm.RedisRejectedRate = rejected
	cm.CPULimit = c.kubeLimitCPU(ctx, ns, "memcached.*", "memcached")
	// memcached_exporter has no latency histogram. Clear Redis leftovers.
	cm.P50Latency = 0
	cm.P95Latency = 0
	cm.P99Latency = 0
	cm.P99Latency9m = 0
	cm.AvgLatency = 0
}
