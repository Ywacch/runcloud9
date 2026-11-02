package metrics

import (
	"context"
	"fmt"
)

// applyMemcachedCacheMetrics fills cache fields from memcached_exporter.
// Redis queries in the same collect leave these at zero when the cache is Memcached.
func (c *MetricsCollector) applyMemcachedCacheMetrics(ctx context.Context, ns string, cm *ComponentMetrics) {
	if c == nil || cm == nil || ns == "" {
		return
	}
	cpu, present := c.queryContainerCPUPrimaryOnly(ctx, ns, "memcached.*", "memcached")
	if present {
		cm.CPUUsage = cpu
		cm.CPUIsEstimated = false
	}
	used, _ := c.query(ctx, fmt.Sprintf(`sum(memcached_current_bytes{namespace=%q})`, ns))
	limit, _ := c.query(ctx, fmt.Sprintf(`max(memcached_limit_bytes{namespace=%q})`, ns))
	if used > 0 {
		cm.MemoryBytes = int64(used)
		cm.MemIsEstimated = false
	}
	if limit > 0 {
		cm.MemoryLimit = int64(limit)
	} else if cm.MemoryLimit == 0 {
		cm.MemoryLimit = c.kubeLimitMem(ctx, ns, "memcached.*", "memcached")
	}
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
