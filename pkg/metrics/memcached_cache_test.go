package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
)

func promEmptyVectorJSON() map[string]any {
	return map[string]any{
		"status": "success",
		"data": map[string]any{
			"resultType": "vector",
			"result":     []any{},
		},
	}
}

func TestApplyMemcachedCacheMetrics_SetsCPULimitLeavesP99(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		value := "0"
		if strings.Contains(q, `resource="cpu"`) && strings.Contains(q, `container="memcached"`) {
			value = "0.125"
		}
		_ = json.NewEncoder(w).Encode(promVectorJSON(value))
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	cm := &ComponentMetrics{P99Latency: 0.04, P99Latency9m: 0.09, AvgLatency: 0.01}
	c.applyMemcachedCacheMetrics(context.Background(), promNamespaceSelector("ns1"), cm)
	if cm.CPULimit != 0.125 {
		t.Fatalf("CPULimit: want 0.125, got %v", cm.CPULimit)
	}
	if cm.P99Latency != 0 || cm.P99Latency9m != 0 || cm.AvgLatency != 0 {
		t.Fatalf("latency must stay absent, got p99=%v p99_9m=%v avg=%v", cm.P99Latency, cm.P99Latency9m, cm.AvgLatency)
	}
}

func TestApplyMemcachedCacheMetrics_UsesContainerMetricsNotItemBytes(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "container_cpu_usage_seconds_total"),
			strings.Contains(q, "container_memory_working_set_bytes"):
			_ = json.NewEncoder(w).Encode(promEmptyVectorJSON())
		case strings.Contains(q, `resource="cpu"`) && strings.Contains(q, `container="memcached"`):
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.125"))
		case strings.Contains(q, `resource="memory"`) && strings.Contains(q, `container="memcached"`):
			_ = json.NewEncoder(w).Encode(promVectorJSON("268435456"))
		case strings.Contains(q, "memcached_current_bytes"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("18914"))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyVectorJSON())
		}
	}))
	t.Cleanup(srv.Close)

	provider := &fakePodMetricsProvider{list: &metricsv1beta1.PodMetricsList{
		Items: []metricsv1beta1.PodMetrics{{
			ObjectMeta: metav1.ObjectMeta{Name: "memcached-abc"},
			Containers: []metricsv1beta1.ContainerMetrics{
				{
					Name: "memcached-exporter",
					Usage: corev1.ResourceList{
						corev1.ResourceCPU:    *resource.NewMilliQuantity(6, resource.DecimalSI),
						corev1.ResourceMemory: *resource.NewQuantity(7<<20, resource.BinarySI),
					},
				},
				{
					Name: "memcached",
					Usage: corev1.ResourceList{
						corev1.ResourceCPU:    *resource.NewMilliQuantity(40, resource.DecimalSI),
						corev1.ResourceMemory: *resource.NewQuantity(32<<20, resource.BinarySI),
					},
				},
			},
		}},
	}}
	c := NewMetricsCollector(srv.URL, nil, nil, provider, nil)
	cm := &ComponentMetrics{
		CPUUsage:       0,
		CPUIsEstimated: true,
		MemoryBytes:    18914,
		MemIsEstimated: false,
		MemoryLimit:    230 << 20,
	}
	c.applyMemcachedCacheMetrics(context.Background(), promNamespaceSelector("ns1"), cm)
	if cm.CPUUsage != 0.04 || cm.CPUIsEstimated || cm.CPUSource != ResourceSourceMetricsAPI {
		t.Fatalf("cpu: usage=%v estimated=%v source=%q", cm.CPUUsage, cm.CPUIsEstimated, cm.CPUSource)
	}
	if cm.CPULimit != 0.125 {
		t.Fatalf("CPULimit: want 0.125, got %v", cm.CPULimit)
	}
	if cm.MemoryBytes != 32<<20 || cm.MemIsEstimated || cm.MemSource != ResourceSourceMetricsAPI {
		t.Fatalf("memory: bytes=%d estimated=%v source=%q", cm.MemoryBytes, cm.MemIsEstimated, cm.MemSource)
	}
	if cm.MemoryLimit != 268435456 {
		t.Fatalf("MemoryLimit: want container limit, got %d", cm.MemoryLimit)
	}
}

func TestApplyMemcachedCacheMetrics_LifecycleAndClientLatency(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "histogram_quantile(0.99") && strings.Contains(q, "cache_op_duration_seconds_bucket"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.004"))
		case strings.Contains(q, "histogram_quantile(0.95"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.002"))
		case strings.Contains(q, "histogram_quantile(0.50"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.001"))
		case strings.Contains(q, "rate(memcached_items_evicted_total"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("2"))
		case strings.Contains(q, "rate(memcached_items_reclaimed_total"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.5"))
		case strings.Contains(q, "memcached_items_evicted_total"):
			_ = json.NewEncoder(w).Encode(promVectorWithPodJSON("memcached-abc", "10"))
		case strings.Contains(q, "memcached_items_reclaimed_total"):
			_ = json.NewEncoder(w).Encode(promVectorWithPodJSON("memcached-abc", "4"))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyVectorJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	cm := &ComponentMetrics{P99Latency: 0.04, EvictionsLast9m: 99, ExpirationsLast9m: 99}
	c.applyMemcachedCacheMetrics(context.Background(), promNamespaceSelector("ns1"), cm)
	if cm.EvictionsPerSec != 2 {
		t.Fatalf("EvictionsPerSec: got %v", cm.EvictionsPerSec)
	}
	if cm.ExpirationsPerSec != 0.5 {
		t.Fatalf("ExpirationsPerSec: got %v", cm.ExpirationsPerSec)
	}
	if cm.EvictionsLast9m != 10 {
		t.Fatalf("EvictionsLast9m: got %v", cm.EvictionsLast9m)
	}
	if cm.ExpirationsLast9m != 4 {
		t.Fatalf("ExpirationsLast9m: got %v", cm.ExpirationsLast9m)
	}
	if cm.P99Latency != 0.004 || cm.P99Latency9m != 0.004 {
		t.Fatalf("p99 live=%v session=%v", cm.P99Latency, cm.P99Latency9m)
	}
}

func TestMemcached_LatencyAbsentNotZero(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(promEmptyVectorJSON())
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	cm := &ComponentMetrics{}
	c.applyMemcachedCacheMetrics(context.Background(), promNamespaceSelector("ns1"), cm)
	if cm.P99Latency != 0 || cm.AvgLatency != 0 {
		t.Fatalf("expected absent latency, got p99=%v avg=%v", cm.P99Latency, cm.AvgLatency)
	}
}
