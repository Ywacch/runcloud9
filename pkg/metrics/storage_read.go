package metrics

import (
	"context"
	"fmt"

	"github.com/ywacch/cloud9/pkg/metrics/signals"
)

func redisStorageReadRateSignal(template string) signals.ID {
	if template == "social-feed" {
		return signals.RedisStorageReadCommandsRateZSet
	}
	return signals.RedisStorageReadCommandsRate
}

func redisStorageWriteRateSignal(template string) signals.ID {
	if template == "social-feed" {
		return signals.RedisStorageWriteCommandsRateZSet
	}
	return signals.RedisStorageWriteCommandsRate
}

func redisStorageReadLatencySignal(template string) signals.ID {
	if template == "social-feed" {
		return signals.RedisStorageReadLatencyP99ZSet
	}
	return signals.RedisStorageReadLatencyP99
}

func redisStorageWriteLatencySignal(template string) signals.ID {
	if template == "social-feed" {
		return signals.RedisStorageWriteLatencyP99ZSet
	}
	return signals.RedisStorageWriteLatencyP99
}

func redisStorageReadTotalSignal(template string) signals.ID {
	if template == "social-feed" {
		return signals.RedisStorageReadCommandsTotalZSet
	}
	return signals.RedisStorageReadCommandsTotal
}

func redisStorageWriteTotalSignal(template string) signals.ID {
	if template == "social-feed" {
		return signals.RedisStorageWriteCommandsTotalZSet
	}
	return signals.RedisStorageWriteCommandsTotal
}

func (c *MetricsCollector) redisStorageReadRate(ctx context.Context, ns, template string) (float64, bool) {
	return c.sigCompat(ctx, ns, redisStorageReadRateSignal(template), redisStorageReadRateQuery(ns, template))
}

func (c *MetricsCollector) redisStorageWriteRate(ctx context.Context, ns, template string) (float64, bool) {
	return c.sigCompat(ctx, ns, redisStorageWriteRateSignal(template), redisStorageWriteRateQuery(ns, template))
}

func (c *MetricsCollector) redisStorageReadLatency(ctx context.Context, ns, template string) (float64, bool) {
	return c.sigCompat(ctx, ns, redisStorageReadLatencySignal(template), redisStorageReadLatencyQuery(ns, template))
}

func (c *MetricsCollector) redisStorageWriteLatency(ctx context.Context, ns, template string) (float64, bool) {
	return c.sigCompat(ctx, ns, redisStorageWriteLatencySignal(template), redisStorageWriteLatencyQuery(ns, template))
}

func (c *MetricsCollector) redisStorageReadTotalByPod(ctx context.Context, ns, template string) map[string]float64 {
	if c == nil || ns == "" {
		return nil
	}
	id := redisStorageReadTotalSignal(template)
	m := c.sigByPod(ctx, ns, id)
	if len(m) > 0 {
		return m
	}
	return c.queryMultiResult(ctx, redisStorageReadTotal9mQuery(ns, template), "pod")
}

func (c *MetricsCollector) redisStorageWriteTotalByPod(ctx context.Context, ns, template string) map[string]float64 {
	if c == nil || ns == "" {
		return nil
	}
	id := redisStorageWriteTotalSignal(template)
	m := c.sigByPod(ctx, ns, id)
	if len(m) > 0 {
		return m
	}
	return c.queryMultiResult(ctx, redisStorageWriteTotal9mQuery(ns, template), "pod")
}

func (c *MetricsCollector) redisStorageRejectedRate(ctx context.Context, ns string) (float64, bool) {
	old := fmt.Sprintf(`sum(rate(redis_rejected_connections_total{namespace=%q}%s))`, ns, promRateRange())
	return c.sigCompat(ctx, ns, signals.RedisRejectedConnectionsRate, old)
}
