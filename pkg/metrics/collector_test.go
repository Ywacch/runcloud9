package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func promVectorJSON(value string) map[string]any {
	return map[string]any{
		"status": "success",
		"data": map[string]any{
			"resultType": "vector",
			"result": []map[string]any{
				{
					"metric": map[string]string{},
					"value":  []any{float64(1), value},
				},
			},
		},
	}
}

func promEmptyJSON() map[string]any {
	return map[string]any{
		"status": "success",
		"data": map[string]any{
			"resultType": "vector",
			"result":     []any{},
		},
	}
}

func TestCollect_CacheIncreaseQueries(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		var val string
		switch {
		case strings.Contains(q, "increase(cache_hits_total"):
			val = "150"
		case strings.Contains(q, "increase(cache_misses_total"):
			val = "30"
		default:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(promVectorJSON(val))
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil)
	dm, err := c.Collect(context.Background(), "ns-test")
	if err != nil {
		t.Fatal(err)
	}
	app := dm.Components["app-server"]
	if app.CacheHitsLast9m != 150 {
		t.Fatalf("CacheHitsLast9m: want 150, got %v", app.CacheHitsLast9m)
	}
	if app.CacheMissesLast9m != 30 {
		t.Fatalf("CacheMissesLast9m: want 30, got %v", app.CacheMissesLast9m)
	}
}

func TestQueryWithFallback_PrimaryNonZeroSkipsFallback(t *testing.T) {
	t.Parallel()
	var n int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n++
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(q, "__PRIMARY__") {
			_ = json.NewEncoder(w).Encode(promVectorJSON("2.5"))
			return
		}
		if strings.Contains(q, "__FALLBACK__") {
			_ = json.NewEncoder(w).Encode(promVectorJSON("99"))
			return
		}
		_ = json.NewEncoder(w).Encode(promEmptyJSON())
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil)
	got := c.queryWithFallback(context.Background(), `sum(__PRIMARY__)`, `sum(__FALLBACK__)`)
	if got != 2.5 {
		t.Fatalf("want primary 2.5, got %v", got)
	}
	if n != 1 {
		t.Fatalf("want 1 HTTP request (no fallback), got %d", n)
	}
}

func TestQueryWithFallback_UsesFallbackWhenPrimaryZero(t *testing.T) {
	t.Parallel()
	var n int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n++
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(q, "__PRIMARY__") {
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
			return
		}
		if strings.Contains(q, "__FALLBACK__") {
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.25"))
			return
		}
		_ = json.NewEncoder(w).Encode(promEmptyJSON())
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil)
	got := c.queryWithFallback(context.Background(), `sum(__PRIMARY__)`, `sum(__FALLBACK__)`)
	if got != 0.25 {
		t.Fatalf("want fallback 0.25, got %v", got)
	}
	if n != 2 {
		t.Fatalf("want 2 HTTP requests (primary + fallback), got %d", n)
	}
}

func TestQueryWithFallback_BothZero(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(promEmptyJSON())
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil)
	got := c.queryWithFallback(context.Background(), `sum(__PRIMARY__)`, `sum(__FALLBACK__)`)
	if got != 0 {
		t.Fatalf("want 0, got %v", got)
	}
}

func TestQuery_EmptyPrometheusURL(t *testing.T) {
	t.Parallel()
	c := NewMetricsCollector("", nil, nil)
	v, err := c.query(context.Background(), "anything")
	if err != nil {
		t.Fatal(err)
	}
	if v != 0 {
		t.Fatalf("want 0, got %v", v)
	}
}

func TestCollect_NoDeprecatedRateWindows(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var queries []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		mu.Lock()
		queries = append(queries, q)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(promEmptyJSON())
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil)
	_, err := c.Collect(context.Background(), "ns-prod")
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, q := range queries {
		// Rate windows were widened to [30s] for reliable samples vs 10s scrape intervals.
		if strings.Contains(q, "[15s]") {
			t.Fatalf("query must not use deprecated [15s] rate window: %s", q)
		}
		if strings.Contains(q, "[1m]") {
			t.Fatalf("query must not use [1m] window: %s", q)
		}
	}
}

func TestCollect_HistogramQuantilesUse30sBucketRate(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		mu.Lock()
		seen = append(seen, q)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(q, "histogram_quantile(0.50") {
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.001"))
			return
		}
		if strings.Contains(q, "histogram_quantile(0.95") {
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.005"))
			return
		}
		if strings.Contains(q, "histogram_quantile(0.99") {
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.009"))
			return
		}
		_ = json.NewEncoder(w).Encode(promEmptyJSON())
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil)
	dm, err := c.Collect(context.Background(), "ns-x")
	if err != nil {
		t.Fatal(err)
	}
	app := dm.Components["app-server"]
	if app.P50Latency != 0.001 || app.P95Latency != 0.005 || app.P99Latency != 0.009 {
		t.Fatalf("latencies: p50=%v p95=%v p99=%v", app.P50Latency, app.P95Latency, app.P99Latency)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, q := range seen {
		if strings.Contains(q, "http_request_duration_seconds_bucket") && strings.Contains(q, "histogram_quantile") {
			if !strings.Contains(q, "[30s]") {
				t.Fatalf("histogram quantile query missing [30s] in bucket rate: %s", q)
			}
		}
	}
}

func TestCollect_DBCPUUsesCadvisorWhenNonZero(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "container_cpu_usage_seconds_total") && strings.Contains(q, `container="postgres"`):
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.07"))
		case strings.Contains(q, "kube_pod_container_resource_requests") && strings.Contains(q, `resource="cpu"`) && strings.Contains(q, `container="postgres"`):
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.25"))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil)
	dm, err := c.Collect(context.Background(), "ns1")
	if err != nil {
		t.Fatal(err)
	}
	if dm.Components["db"].CPUUsage != 0.07 {
		t.Fatalf("db CPU: want 0.07 (cAdvisor), got %v", dm.Components["db"].CPUUsage)
	}
}

func TestCollect_DBCPUFallbackToKubeWhenCadvisorEmpty(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "container_cpu_usage_seconds_total") && strings.Contains(q, `container="postgres"`):
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		case strings.Contains(q, "kube_pod_container_resource_requests") && strings.Contains(q, `resource="cpu"`) && strings.Contains(q, `container="postgres"`):
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.25"))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil)
	dm, err := c.Collect(context.Background(), "ns1")
	if err != nil {
		t.Fatal(err)
	}
	db := dm.Components["db"]
	if db.CPUUsage != 0.25 {
		t.Fatalf("db CPU: want 0.25 (kube fallback), got %v", db.CPUUsage)
	}
	if !db.CPUIsEstimated {
		t.Fatal("db CPUIsEstimated: want true when kube fallback used")
	}
}

func TestQueryCPUPair_PromQLFilters(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var queries []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		mu.Lock()
		queries = append(queries, q)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(promEmptyJSON())
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil)
	_, _ = c.queryCPUPair(context.Background(), "my-ns", "app-server.*", "app-server")
	mu.Lock()
	defer mu.Unlock()
	var primary string
	for _, q := range queries {
		if strings.Contains(q, "container_cpu_usage_seconds_total") {
			primary = q
			break
		}
	}
	if primary == "" {
		t.Fatalf("expected cAdvisor CPU query in %v", queries)
	}
	if !strings.Contains(primary, `namespace="my-ns"`) {
		t.Fatalf("want namespace in query: %s", primary)
	}
	if !strings.Contains(primary, `pod=~"app-server.*"`) {
		t.Fatalf("want pod regex in query: %s", primary)
	}
	if !strings.Contains(primary, `container="app-server"`) {
		t.Fatalf("want main container filter: %s", primary)
	}
	if !strings.Contains(primary, "[30s]") {
		t.Fatalf("want [30s] rate window: %s", primary)
	}
}
