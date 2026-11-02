package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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
	c.applyMemcachedCacheMetrics(context.Background(), "ns1", cm)
	if cm.CPULimit != 0.125 {
		t.Fatalf("CPULimit: want 0.125, got %v", cm.CPULimit)
	}
	if cm.P99Latency != 0 || cm.P99Latency9m != 0 || cm.AvgLatency != 0 {
		t.Fatalf("latency must stay absent, got p99=%v p99_9m=%v avg=%v", cm.P99Latency, cm.P99Latency9m, cm.AvgLatency)
	}
}
