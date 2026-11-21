package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFillRedisCacheStoreTelemetry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		evicted       string
		expired       string
		wantEvictions float64
		wantExpires   float64
	}{
		{
			name:          "both rates present",
			evicted:       "12.5",
			expired:       "3.25",
			wantEvictions: 12.5,
			wantExpires:   3.25,
		},
		{
			name:          "zeros stay zero",
			evicted:       "0",
			expired:       "0",
			wantEvictions: 0,
			wantExpires:   0,
		},
		{
			name:          "evictions only",
			evicted:       "8",
			expired:       "0",
			wantEvictions: 8,
			wantExpires:   0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				q := r.URL.Query().Get("query")
				w.Header().Set("Content-Type", "application/json")
				switch {
				case strings.Contains(q, "rate(redis_evicted_keys_total"):
					_ = json.NewEncoder(w).Encode(promVectorJSON(tt.evicted))
				case strings.Contains(q, "rate(redis_expired_keys_total"):
					_ = json.NewEncoder(w).Encode(promVectorJSON(tt.expired))
				case strings.Contains(q, "redis_evicted_keys_total"):
					_ = json.NewEncoder(w).Encode(promVectorWithPodJSON("redis-0", tt.evicted))
				case strings.Contains(q, "redis_expired_keys_total"):
					_ = json.NewEncoder(w).Encode(promVectorWithPodJSON("redis-0", tt.expired))
				default:
					_ = json.NewEncoder(w).Encode(promEmptyJSON())
				}
			}))
			t.Cleanup(srv.Close)

			c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
			got := c.fillRedisCacheStoreTelemetry(context.Background(), promNamespaceSelector("ns-cache"))
			if got.EvictionsPerSec != tt.wantEvictions {
				t.Fatalf("EvictionsPerSec: got %v want %v", got.EvictionsPerSec, tt.wantEvictions)
			}
			if got.ExpirationsPerSec != tt.wantExpires {
				t.Fatalf("ExpirationsPerSec: got %v want %v", got.ExpirationsPerSec, tt.wantExpires)
			}
			if tt.wantEvictions > 0 && got.EvictionsLast9m <= 0 {
				t.Fatalf("EvictionsLast9m: got %v want > 0", got.EvictionsLast9m)
			}
		})
	}
}

func promVectorWithPodJSON(pod, value string) map[string]any {
	return map[string]any{
		"status": "success",
		"data": map[string]any{
			"resultType": "vector",
			"result": []map[string]any{
				{
					"metric": map[string]string{"pod": pod},
					"value":  []any{float64(1), value},
				},
			},
		},
	}
}

func TestApplyCacheStoreTelemetry(t *testing.T) {
	t.Parallel()
	cm := ComponentMetrics{Component: "cache"}
	applyCacheStoreTelemetry(&cm, CacheStoreTelemetry{
		EvictionsPerSec:   4.2,
		ExpirationsPerSec: 1.1,
	})
	if cm.EvictionsPerSec != 4.2 || cm.ExpirationsPerSec != 1.1 {
		t.Fatalf("cm: %+v", cm)
	}
}

func TestApplyCacheLayers_copiesStoreTelemetry(t *testing.T) {
	t.Parallel()
	cm := ComponentMetrics{
		EvictionsPerSec:   9,
		ExpirationsPerSec: 2,
		RedisOpsPerSec:    100,
	}
	split := cacheLayerSplit{
		proxyPresent: true,
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
	applyCacheLayers(&cm, split)
	backing := cm.Layers[0]
	if backing.EvictionsPerSec != 9 || backing.ExpirationsPerSec != 2 {
		t.Fatalf("backing store telemetry: %+v", backing)
	}
	if cm.Layers[1].EvictionsPerSec != 0 {
		t.Fatalf("proxy must not inherit store telemetry")
	}
}

func TestApplyCacheLayers_prefersLayerCollectedTelemetry(t *testing.T) {
	t.Parallel()
	cm := ComponentMetrics{
		EvictionsPerSec:   1,
		ExpirationsPerSec: 1,
	}
	split := cacheLayerSplit{
		proxyPresent: true,
		redis: ComponentLayerMetrics{
			ID:                layerIDRedisInternal,
			Role:              layerRoleBackingStore,
			Engine:            layerEngineRedis,
			EvictionsPerSec:   20,
			ExpirationsPerSec: 5,
		},
		proxy: ComponentLayerMetrics{
			ID:     layerIDCacheProxy,
			Engine: layerEngineCacheProxy,
		},
	}
	applyCacheLayers(&cm, split)
	if cm.Layers[0].EvictionsPerSec != 20 || cm.Layers[0].ExpirationsPerSec != 5 {
		t.Fatalf("want layer values kept, got %+v", cm.Layers[0])
	}
}
