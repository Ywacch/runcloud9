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
	oldUser := fmt.Sprintf(`sum(rate(redis_cpu_user_seconds_total{namespace=%q}%s))`, ns, promRateRange())
	oldSys := fmt.Sprintf(`sum(rate(redis_cpu_sys_seconds_total{namespace=%q}%s))`, ns, promRateRange())
	user, userOK := c.readSignalCompat(ctx, ns, signals.RedisCPUUserRate, oldUser)
	sys, sysOK := c.readSignalCompat(ctx, ns, signals.RedisCPUSysRate, oldSys)
	if signalCompareEnabled() {
		c.compareSignalScalar(ctx, ns, signals.RedisCPUUserRate, user, userOK, oldUser)
		c.compareSignalScalar(ctx, ns, signals.RedisCPUSysRate, sys, sysOK, oldSys)
	}
	if !userOK && !sysOK {
		return 0, false
	}
	return user + sys, true
}

func (c *MetricsCollector) redisMemoryUsedBytes(ctx context.Context, ns string) (float64, bool) {
	old := fmt.Sprintf(`sum(redis_memory_used_bytes{namespace=%q})`, ns)
	return c.sigCompat(ctx, ns, signals.RedisMemoryUsedBytes, old)
}

func (c *MetricsCollector) redisMemoryMaxBytes(ctx context.Context, ns string) (float64, bool) {
	old := fmt.Sprintf(`max(redis_memory_max_bytes{namespace=%q})`, ns)
	return c.sigCompat(ctx, ns, signals.RedisMemoryMaxBytes, old)
}

func (c *MetricsCollector) redisEvictedKeysRate(ctx context.Context, ns string) (float64, bool) {
	old := fmt.Sprintf(`sum(rate(redis_evicted_keys_total{namespace=%q}%s))`, ns, promRateRange())
	return c.sigCompat(ctx, ns, signals.RedisEvictedKeysRate, old)
}

func (c *MetricsCollector) redisExpiredKeysRate(ctx context.Context, ns string) (float64, bool) {
	old := fmt.Sprintf(`sum(rate(redis_expired_keys_total{namespace=%q}%s))`, ns, promRateRange())
	return c.sigCompat(ctx, ns, signals.RedisExpiredKeysRate, old)
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
	old := fmt.Sprintf(`sum(rate(memcached_commands_total{namespace=%q}%s))`, ns, promRateRange())
	return c.sigCompat(ctx, ns, signals.MemcachedCommandsRate, old)
}

func (c *MetricsCollector) memcachedGetHitRate(ctx context.Context, ns string) (float64, bool) {
	old := fmt.Sprintf(
		`sum(rate(memcached_commands_total{namespace=%q,command="get",status="hit"}%s))`,
		ns, promRateRange(),
	)
	return c.sigCompat(ctx, ns, signals.MemcachedGetHitRate, old)
}

func (c *MetricsCollector) memcachedGetMissRate(ctx context.Context, ns string) (float64, bool) {
	old := fmt.Sprintf(
		`sum(rate(memcached_commands_total{namespace=%q,command="get",status="miss"}%s))`,
		ns, promRateRange(),
	)
	return c.sigCompat(ctx, ns, signals.MemcachedGetMissRate, old)
}

func (c *MetricsCollector) memcachedCurrentConnections(ctx context.Context, ns string) (float64, bool) {
	old := fmt.Sprintf(`sum(memcached_current_connections{namespace=%q})`, ns)
	return c.sigCompat(ctx, ns, signals.MemcachedCurrentConnections, old)
}

func (c *MetricsCollector) memcachedConnectionsListenerDisabledRate(ctx context.Context, ns string) (float64, bool) {
	old := fmt.Sprintf(
		`sum(rate(memcached_connections_listener_disabled_total{namespace=%q}%s))`,
		ns, promRateRange(),
	)
	return c.sigCompat(ctx, ns, signals.MemcachedConnectionsListenerDisabledRate, old)
}

func (c *MetricsCollector) memcachedItemsEvictedRate(ctx context.Context, ns string) (float64, bool) {
	old := fmt.Sprintf(`sum(rate(memcached_items_evicted_total{namespace=%q}%s))`, ns, promRateRange())
	return c.sigCompat(ctx, ns, signals.MemcachedItemsEvictedRate, old)
}

func (c *MetricsCollector) memcachedItemsReclaimedRate(ctx context.Context, ns string) (float64, bool) {
	old := fmt.Sprintf(`sum(rate(memcached_items_reclaimed_total{namespace=%q}%s))`, ns, promRateRange())
	return c.sigCompat(ctx, ns, signals.MemcachedItemsReclaimedRate, old)
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
