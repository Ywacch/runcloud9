package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestApplyCacheLayers_noProxy(t *testing.T) {
	t.Parallel()
	cm := ComponentMetrics{
		RedisOpsPerSec:      100,
		RedisKeyspaceHits:   10,
		RedisKeyspaceMisses: 2,
		RedisRejectedRate:   0.5,
	}
	applyCacheLayers(&cm, cacheLayerSplit{proxyPresent: false})
	if cm.CacheMetricMode != "" {
		t.Fatalf("CacheMetricMode: want empty, got %q", cm.CacheMetricMode)
	}
	if len(cm.Layers) != 0 {
		t.Fatalf("layers: want none, got %d", len(cm.Layers))
	}
}

func TestApplyCacheLayers_proxyPresent(t *testing.T) {
	t.Parallel()
	cm := ComponentMetrics{
		RedisOpsPerSec:      21000,
		RedisKeyspaceHits:   800,
		RedisKeyspaceMisses: 120,
		RedisRejectedRate:   3,
		EvictionsPerSec:     4,
		ExpirationsPerSec:   1.5,
	}
	split := cacheLayerSplit{
		proxyPresent: true,
		redis: ComponentLayerMetrics{
			ID:             layerIDRedisInternal,
			Role:           layerRoleBackingStore,
			Engine:         layerEngineRedis,
			CPUUsage:       0.12,
			MemoryBytes:    200 << 20,
			CPULimit:       0.5,
			MemoryLimit:    512 << 20,
			CPUIsEstimated: false,
		},
		proxy: ComponentLayerMetrics{
			ID:          layerIDCacheProxy,
			Role:        layerRoleSidecar,
			Engine:      layerEngineCacheProxy,
			CPUUsage:    0.04,
			MemoryBytes: 64 << 20,
		},
	}
	applyCacheLayers(&cm, split)
	if cm.CacheMetricMode != "layered" {
		t.Fatalf("CacheMetricMode: want layered, got %q", cm.CacheMetricMode)
	}
	if len(cm.Layers) != 2 {
		t.Fatalf("layers: want 2, got %d", len(cm.Layers))
	}
	backing := cm.Layers[0]
	if backing.ID != layerIDRedisInternal || backing.Role != layerRoleBackingStore || backing.Engine != layerEngineRedis {
		t.Fatalf("backing identity: %+v", backing)
	}
	if backing.RedisOpsPerSec != 21000 || backing.RedisKeyspaceHits != 800 ||
		backing.RedisKeyspaceMisses != 120 || backing.RedisRejectedRate != 3 {
		t.Fatalf("backing redis metrics not copied: %+v", backing)
	}
	if backing.EvictionsPerSec != 4 || backing.ExpirationsPerSec != 1.5 {
		t.Fatalf("backing store telemetry not copied: %+v", backing)
	}
	if backing.CPUUsage != 0.12 || backing.MemoryBytes != int64(200<<20) {
		t.Fatalf("backing resources: %+v", backing)
	}
	proxy := cm.Layers[1]
	if proxy.ID != layerIDCacheProxy || proxy.Engine != layerEngineCacheProxy {
		t.Fatalf("proxy identity: %+v", proxy)
	}
	if proxy.RedisOpsPerSec != 0 {
		t.Fatalf("proxy must not carry redis exporter fields: %+v", proxy)
	}
}

func TestSyncCacheLayerRestarts(t *testing.T) {
	t.Parallel()
	cm := ComponentMetrics{
		Layers: []ComponentLayerMetrics{
			{ID: layerIDRedisInternal},
			{ID: layerIDCacheProxy},
		},
		RuntimeHealth: &RuntimeHealth{
			Pods: []PodRuntimeHealth{
				{Name: "redis-internal-abc", RestartCount: 2},
				{Name: "redis-internal-xyz", RestartCount: 5},
				{Name: "cache-proxy-abc", RestartCount: 1},
				{Name: "cache-proxy-def", RestartCount: 3},
			},
		},
	}
	syncCacheLayerRestarts(&cm)
	if cm.Layers[0].RestartCount != 5 {
		t.Fatalf("redis-internal restarts: want 5, got %d", cm.Layers[0].RestartCount)
	}
	if cm.Layers[1].RestartCount != 3 {
		t.Fatalf("cache-proxy restarts: want 3, got %d", cm.Layers[1].RestartCount)
	}
}

func TestSyncCacheLayerRestarts_noRuntimeHealth(t *testing.T) {
	t.Parallel()
	cm := ComponentMetrics{
		Layers: []ComponentLayerMetrics{{ID: layerIDRedisInternal, RestartCount: 0}},
	}
	syncCacheLayerRestarts(&cm)
	if cm.Layers[0].RestartCount != 0 {
		t.Fatalf("expected unchanged restart count, got %d", cm.Layers[0].RestartCount)
	}
}

func TestCollectCacheLayerSplit_proxyMetrics(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "redis_cpu_user_seconds_total"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.08"))
		case strings.Contains(q, "redis_cpu_sys_seconds_total"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.02"))
		case strings.Contains(q, "process_cpu_seconds_total") && strings.Contains(q, "cache-proxy"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.05"))
		case strings.Contains(q, "redis_memory_used_bytes"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("134217728"))
		case strings.Contains(q, "redis_evicted_keys_total"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("2.5"))
		case strings.Contains(q, "redis_expired_keys_total"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.75"))
		case strings.Contains(q, "process_resident_memory_bytes") && strings.Contains(q, "cache-proxy"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("67108864"))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	split := c.collectCacheLayerSplit(context.Background(), promNamespaceSelector("ns-proxy"))
	if !split.proxyPresent {
		t.Fatal("expected proxyPresent when cache-proxy CPU series exists")
	}
	if split.redis.CPUUsage != 0.1 {
		t.Fatalf("redis CPU: want 0.1, got %v", split.redis.CPUUsage)
	}
	if split.proxy.CPUUsage != 0.05 {
		t.Fatalf("proxy CPU: want 0.05, got %v", split.proxy.CPUUsage)
	}
	if split.redis.MemoryBytes != 134217728 {
		t.Fatalf("redis mem: got %d", split.redis.MemoryBytes)
	}
	if split.proxy.MemoryBytes != 67108864 {
		t.Fatalf("proxy mem: got %d", split.proxy.MemoryBytes)
	}
	if split.redis.EvictionsPerSec != 2.5 {
		t.Fatalf("redis evictions: want 2.5, got %v", split.redis.EvictionsPerSec)
	}
	if split.redis.ExpirationsPerSec != 0.75 {
		t.Fatalf("redis expirations: want 0.75, got %v", split.redis.ExpirationsPerSec)
	}
}
