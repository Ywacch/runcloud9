package metrics

import (
	"context"
	"fmt"
	"strings"
)

const (
	layerRoleBackingStore = "backing-store"
	layerRoleSidecar      = "sidecar"
	layerEngineRedis      = "redis"
	layerEngineCacheProxy = "caching-proxy"
	layerIDRedisInternal  = "redis-internal"
	layerIDCacheProxy     = "cache-proxy"
)

// cacheLayerSplit holds per-layer resource metrics for proxy-mode cache deployments.
type cacheLayerSplit struct {
	proxyPresent bool
	redis        ComponentLayerMetrics
	proxy        ComponentLayerMetrics
}

// collectCacheLayerSplit queries redis-internal and cache-proxy metrics separately.
func (c *MetricsCollector) collectCacheLayerSplit(ctx context.Context, ns string) cacheLayerSplit {
	if c == nil {
		return cacheLayerSplit{}
	}
	out := cacheLayerSplit{
		redis: ComponentLayerMetrics{
			ID:     layerIDRedisInternal,
			Role:   layerRoleBackingStore,
			Engine: layerEngineRedis,
		},
		proxy: ComponentLayerMetrics{
			ID:     layerIDCacheProxy,
			Role:   layerRoleSidecar,
			Engine: layerEngineCacheProxy,
		},
	}

	redisCPUUser, redisUserPresent, _ := c.queryScalar(ctx, fmt.Sprintf(
		`sum(rate(redis_cpu_user_seconds_total{namespace=%q}%s))`,
		ns, promRateRange(),
	))
	redisCPUSys, redisSysPresent, _ := c.queryScalar(ctx, fmt.Sprintf(
		`sum(rate(redis_cpu_sys_seconds_total{namespace=%q}%s))`,
		ns, promRateRange(),
	))
	redisExporterPresent := redisUserPresent || redisSysPresent
	if redisExporterPresent {
		out.redis.CPUUsage = redisCPUUser + redisCPUSys
		out.redis.CPUIsEstimated = false
	} else {
		cpu, est := c.queryContainerCPUPrimaryOnly(ctx, ns, "redis-internal.*", "redis")
		out.redis.CPUUsage = cpu
		out.redis.CPUIsEstimated = !est
	}

	proxyCPU, proxyPresent, _ := c.queryScalar(ctx, fmt.Sprintf(
		`sum(rate(process_cpu_seconds_total{namespace=%q,pod=~%q}%s))`,
		ns, "cache-proxy.*", promRateRange(),
	))
	out.proxyPresent = proxyPresent
	if proxyPresent {
		out.proxy.CPUUsage = proxyCPU
		out.proxy.CPUIsEstimated = false
	}

	redisMem, redisMemPresent := c.queryContainerMemoryPrimaryOnly(ctx, ns, "redis-internal.*", "redis")
	out.redis.MemoryBytes = int64(redisMem)
	out.redis.MemIsEstimated = !redisMemPresent
	if v, _ := c.query(ctx, fmt.Sprintf(`sum(redis_memory_used_bytes{namespace=%q})`, ns)); v > 0 {
		out.redis.MemoryBytes = int64(v)
		out.redis.MemIsEstimated = false
	}

	proxyMem, proxyMemPresent, _ := c.queryScalar(ctx, fmt.Sprintf(
		`sum(process_resident_memory_bytes{namespace=%q,pod=~%q})`,
		ns, "cache-proxy.*",
	))
	if proxyPresent {
		out.proxy.MemoryBytes = int64(proxyMem)
		out.proxy.MemIsEstimated = !proxyMemPresent
	}

	out.redis.CPULimit = c.kubeLimitCPU(ctx, ns, "redis-internal.*", "redis")
	out.proxy.CPULimit = c.kubeLimitCPU(ctx, ns, "cache-proxy.*", "cache-proxy")
	out.redis.MemoryLimit = c.kubeLimitMem(ctx, ns, "redis-internal.*", "redis")
	if redisMax, _ := c.query(ctx, fmt.Sprintf(`max(redis_memory_max_bytes{namespace=%q})`, ns)); redisMax > 0 {
		out.redis.MemoryLimit = int64(redisMax)
	}
	out.proxy.MemoryLimit = c.kubeLimitMem(ctx, ns, "cache-proxy.*", "cache-proxy")

	return out
}

// applyCacheLayers attaches layers when a cache-proxy deployment is present.
func applyCacheLayers(cm *ComponentMetrics, split cacheLayerSplit) {
	if cm == nil || !split.proxyPresent {
		return
	}
	backing := split.redis
	backing.RedisOpsPerSec = cm.RedisOpsPerSec
	backing.RedisKeyspaceHits = cm.RedisKeyspaceHits
	backing.RedisKeyspaceMisses = cm.RedisKeyspaceMisses
	backing.RedisRejectedRate = cm.RedisRejectedRate
	cm.CacheMetricMode = "layered"
	cm.Layers = []ComponentLayerMetrics{backing, split.proxy}
}

// syncCacheLayerRestarts copies restart counts from runtimeHealth pod rows onto matching layers.
func syncCacheLayerRestarts(cm *ComponentMetrics) {
	if cm == nil || len(cm.Layers) == 0 || cm.RuntimeHealth == nil {
		return
	}
	for i := range cm.Layers {
		layer := &cm.Layers[i]
		prefix := layer.ID
		if prefix == layerIDRedisInternal {
			prefix = "redis-internal"
		}
		for _, pod := range cm.RuntimeHealth.Pods {
			if !strings.HasPrefix(pod.Name, prefix) {
				continue
			}
			if pod.RestartCount > layer.RestartCount {
				layer.RestartCount = pod.RestartCount
			}
		}
	}
}
