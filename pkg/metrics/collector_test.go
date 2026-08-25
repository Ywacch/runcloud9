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

func promMultiVectorJSON(labelKey string, series map[string]string) map[string]any {
	result := make([]map[string]any, 0, len(series))
	for name, value := range series {
		result = append(result, map[string]any{
			"metric": map[string]string{labelKey: name},
			"value":  []any{float64(1), value},
		})
	}
	return map[string]any{
		"status": "success",
		"data": map[string]any{
			"resultType": "vector",
			"result":     result,
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

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	dm, err := c.Collect(context.Background(), "ns-test", "", TopologyFlags{})
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

func TestCollect_Session9mAggregates(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, `http_requests_total`) &&
			strings.Contains(q, `status=~"5.."`) &&
			!strings.Contains(q, "rate(") &&
			!strings.Contains(q, "increase("):
			_ = json.NewEncoder(w).Encode(promVectorJSON("12"))
		case strings.Contains(q, "sum by (pod)") &&
			strings.Contains(q, `http_requests_total`) &&
			strings.Contains(q, `pod=~"app-server.*"`) &&
			!strings.Contains(q, "rate(") &&
			!strings.Contains(q, "increase("):
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", map[string]string{
				"app-server-abc": "16240",
				"app-server-def": "16105",
			}))
		case strings.Contains(q, `sum(http_requests_total{`) &&
			strings.Contains(q, `pod=~"app-server.*"`) &&
			!strings.Contains(q, "rate(") &&
			!strings.Contains(q, "increase(") &&
			!strings.Contains(q, `status=~"5.."`):
			_ = json.NewEncoder(w).Encode(promVectorJSON("32345"))
		case strings.Contains(q, `increase(http_requests_total{namespace="ns-test",status=~"5.."}[9m])`):
			_ = json.NewEncoder(w).Encode(promVectorJSON("12"))
		case strings.Contains(q, `sum(increase(http_requests_total{namespace="ns-test"}[9m]))`):
			_ = json.NewEncoder(w).Encode(promVectorJSON("32345"))
		case strings.Contains(q, `histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket{namespace="ns-test"}[9m])) by (le))`):
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.042"))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	dm, err := c.Collect(context.Background(), "ns-test", "", TopologyFlags{})
	if err != nil {
		t.Fatal(err)
	}

	app := dm.Components["app-server"]
	if app.TotalRequests != 32345 {
		t.Fatalf("app TotalRequests: want 32345, got %v", app.TotalRequests)
	}
	if app.TotalErrors9m != 12 {
		t.Fatalf("app TotalErrors9m: want 12, got %v", app.TotalErrors9m)
	}
	if app.P99Latency9m != 0.042 {
		t.Fatalf("app P99Latency9m: want 0.042, got %v", app.P99Latency9m)
	}

	lb := dm.Components["lb"]
	if lb.TotalRequests != 32345 {
		t.Fatalf("lb TotalRequests: want 32345, got %v", lb.TotalRequests)
	}
	if lb.TotalErrors9m != 12 {
		t.Fatalf("lb TotalErrors9m: want 12, got %v", lb.TotalErrors9m)
	}
	if lb.P99Latency9m != 0.042 {
		t.Fatalf("lb P99Latency9m: want 0.042, got %v", lb.P99Latency9m)
	}

	if len(lb.BackendPods) != 2 {
		t.Fatalf("BackendPods len: want 2, got %d", len(lb.BackendPods))
	}
	byPod := make(map[string]BackendPod)
	for _, p := range lb.BackendPods {
		byPod[p.Name] = p
	}
	if byPod["app-server-abc"].TotalRequests != 16240 {
		t.Fatalf("pod abc totalRequests: want 16240, got %v", byPod["app-server-abc"].TotalRequests)
	}
	if byPod["app-server-def"].TotalRequests != 16105 {
		t.Fatalf("pod def totalRequests: want 16105, got %v", byPod["app-server-def"].TotalRequests)
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

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
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

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
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

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	got := c.queryWithFallback(context.Background(), `sum(__PRIMARY__)`, `sum(__FALLBACK__)`)
	if got != 0 {
		t.Fatalf("want 0, got %v", got)
	}
}

// Idle workloads report scalar 0 from rate(); that must not trigger kube request substitution.
func TestQueryWithFallback_PrimaryPresentZeroSkipsFallback(t *testing.T) {
	t.Parallel()
	var n int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n++
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(q, "__PRIMARY__") {
			_ = json.NewEncoder(w).Encode(promVectorJSON("0"))
			return
		}
		if strings.Contains(q, "__FALLBACK__") {
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.25"))
			return
		}
		_ = json.NewEncoder(w).Encode(promEmptyJSON())
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	got := c.queryWithFallback(context.Background(), `sum(__PRIMARY__)`, `sum(__FALLBACK__)`)
	if got != 0 {
		t.Fatalf("want primary 0, got %v (must not substitute kube requests)", got)
	}
	if n != 1 {
		t.Fatalf("want 1 HTTP request (no fallback), got %d", n)
	}
}

func TestQueryWithFallbackEstimated_PrimaryPresentZeroNotEstimated(t *testing.T) {
	t.Parallel()
	var n int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n++
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(q, "__PRIMARY__") {
			_ = json.NewEncoder(w).Encode(promVectorJSON("0"))
			return
		}
		if strings.Contains(q, "__FALLBACK__") {
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.25"))
			return
		}
		_ = json.NewEncoder(w).Encode(promEmptyJSON())
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	got, est := c.queryWithFallbackEstimated(context.Background(), `sum(__PRIMARY__)`, `sum(__FALLBACK__)`)
	if got != 0 || est {
		t.Fatalf("want (0, estimated=false), got (%v, %v)", got, est)
	}
	if n != 1 {
		t.Fatalf("want 1 HTTP request (no fallback), got %d", n)
	}
}

func TestQuery_EmptyPrometheusURL(t *testing.T) {
	t.Parallel()
	c := NewMetricsCollector("", nil, nil, nil, nil)
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

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	_, err := c.Collect(context.Background(), "ns-prod", "", TopologyFlags{})
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, q := range queries {
		// Live rate window is PromRateWindow (10s); requires scrapeInterval <= 5s.
		if strings.Contains(q, "[15s]") {
			t.Fatalf("query must not use deprecated [15s] rate window: %s", q)
		}
		if strings.Contains(q, "[1m]") {
			t.Fatalf("query must not use [1m] window: %s", q)
		}
	}
}

func TestCollect_HistogramQuantilesUsePromRateWindowBucketRate(t *testing.T) {
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

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	dm, err := c.Collect(context.Background(), "ns-x", "", TopologyFlags{})
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
			if strings.Contains(q, "[9m]") {
				continue // session p99 (P99Latency9m)
			}
			if !strings.Contains(q, promRateRange()) {
				t.Fatalf("histogram quantile query missing %s in bucket rate: %s", promRateRange(), q)
			}
		}
	}
}

func TestCollect_DBCPUUsesContainerMetricWhenPresent(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "container_cpu_usage_seconds_total") && strings.Contains(q, `container="postgres"`):
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.062"))
		case strings.Contains(q, "process_cpu_seconds_total") && strings.Contains(q, `pod=~"postgres.*"`):
			t.Error("db CPU must not use process_cpu_seconds_total for postgres pods")
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.07"))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	dm, err := c.Collect(context.Background(), "ns1", "", TopologyFlags{})
	if err != nil {
		t.Fatal(err)
	}
	db := dm.Components["db"]
	if db.CPUUsage != 0.062 {
		t.Fatalf("db CPU: want 0.062 (container metric), got %v", db.CPUUsage)
	}
	if db.CPUIsEstimated {
		t.Fatal("db CPUIsEstimated: want false for cAdvisor container metric")
	}
	if db.CPUSource != ResourceSourceCadvisor {
		t.Fatalf("db CPUSource: want %q, got %q", ResourceSourceCadvisor, db.CPUSource)
	}
}

func TestCollect_DBCPUUsesMetricsAPIWhenCadvisorEmpty(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(promEmptyJSON())
	}))
	t.Cleanup(srv.Close)

	provider := &fakePodMetricsProvider{list: podMetricsFixture(68, 216*1024*1024)}

	c := NewMetricsCollector(srv.URL, nil, nil, provider, nil)
	dm, err := c.Collect(context.Background(), "ns1", "", TopologyFlags{})
	if err != nil {
		t.Fatal(err)
	}
	db := dm.Components["db"]
	if db.CPUUsage != 0.068 {
		t.Fatalf("db CPU: want 0.068 from Metrics API, got %v", db.CPUUsage)
	}
	if db.CPUIsEstimated {
		t.Fatal("db CPUIsEstimated: want false for Metrics API usage")
	}
	if db.CPUSource != ResourceSourceMetricsAPI {
		t.Fatalf("db CPUSource: want %q, got %q", ResourceSourceMetricsAPI, db.CPUSource)
	}
	if db.MemoryBytes != 216*1024*1024 {
		t.Fatalf("db memory: want %d, got %d", 216*1024*1024, db.MemoryBytes)
	}
	if db.MemSource != ResourceSourceMetricsAPI {
		t.Fatalf("db MemSource: want %q, got %q", ResourceSourceMetricsAPI, db.MemSource)
	}
}

func TestCollect_DBCPUFallbackToKubeWhenContainerAndMetricsAPIEmpty(t *testing.T) {
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

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	dm, err := c.Collect(context.Background(), "ns1", "", TopologyFlags{})
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
	if db.CPUSource != ResourceSourceRequests {
		t.Fatalf("db CPUSource: want %q, got %q", ResourceSourceRequests, db.CPUSource)
	}
}

func TestCollect_SetsLBConnectionLimit(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(promEmptyJSON())
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	dm, err := c.Collect(context.Background(), "ns-cap", "", TopologyFlags{})
	if err != nil {
		t.Fatal(err)
	}
	lb := dm.Components["lb"]
	if lb.LBConnectionLimit != nginxWorkerConnections {
		t.Fatalf("LBConnectionLimit: want %v, got %v", nginxWorkerConnections, lb.LBConnectionLimit)
	}
}

func TestCollect_SocialFeedUsesSortedSetRedisMetrics(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var queries []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		mu.Lock()
		queries = append(queries, q)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "redis_commands_total") && strings.Contains(q, "zadd|zrevrange|zremrangebyrank"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("42"))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	dm, err := c.Collect(context.Background(), "ns-social", "social-feed", TopologyFlags{})
	if err != nil {
		t.Fatal(err)
	}
	cache := dm.Components["cache"]
	if cache.CacheStrategy != "timeline-store" {
		t.Fatalf("CacheStrategy: want timeline-store, got %q", cache.CacheStrategy)
	}
	if cache.CacheReadOperation != "ZREVRANGE" || cache.CacheWriteOperation != "ZADD" {
		t.Fatalf("cache operations: read=%q write=%q", cache.CacheReadOperation, cache.CacheWriteOperation)
	}
	if cache.RedisOpsPerSec != 42 || cache.RequestRate != 42 {
		t.Fatalf("redis rates: RedisOpsPerSec=%v RequestRate=%v", cache.RedisOpsPerSec, cache.RequestRate)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, q := range queries {
		if strings.Contains(q, "redis_commands_total") && strings.Contains(q, "get|set|del") {
			t.Fatalf("social-feed Redis command query must not use GET/SET cache-aside pattern: %s", q)
		}
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

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
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
	if !strings.Contains(primary, promRateRange()) {
		t.Fatalf("want %s rate window: %s", promRateRange(), primary)
	}
}

func TestCollect_StorageProfileFromTopology(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(promEmptyJSON())
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	topo := TopologyFlags{
		HasDB:       true,
		DBEngine:    "postgresql",
		DBRole:      "primary",
		HasCache:    true,
		CacheEngine: "redis",
		CacheRole:   "cache-aside",
	}
	dm, err := c.Collect(context.Background(), "ns-storage", "url-shortener", topo)
	if err != nil {
		t.Fatal(err)
	}
	db := dm.Components["db"]
	if db.StorageEngine != "postgresql" || db.StorageRole != "primary" {
		t.Fatalf("db profile: engine=%q role=%q", db.StorageEngine, db.StorageRole)
	}
	if db.StorageIO == nil || db.StorageIO.Unit != "rows/s" {
		t.Fatalf("db storageIo: %+v", db.StorageIO)
	}
	cache := dm.Components["cache"]
	if cache.StorageEngine != "redis" || cache.StorageRole != "cache-aside" {
		t.Fatalf("cache profile: engine=%q role=%q", cache.StorageEngine, cache.StorageRole)
	}
	if db.DbQuerySource != DbQuerySourceNone {
		t.Fatalf("dbQuerySource without app metrics: %q", db.DbQuerySource)
	}
	if db.StorageIO.ReadLabel == "" || db.StorageIO.Scope == "" {
		t.Fatalf("db storageIo presentation: %+v", db.StorageIO)
	}
	if cache.StorageIO.ReadLabel == "" {
		t.Fatalf("cache storageIo presentation: %+v", cache.StorageIO)
	}
}

func TestCollect_DbQuerySource_NoXactCommitBackfill(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		var val string
		switch {
		case strings.Contains(q, "db_query_duration_seconds_count"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
			return
		case strings.Contains(q, "xact_commit"):
			val = "6.5"
		default:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(promVectorJSON(val))
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	dm, err := c.Collect(context.Background(), "ns-db-src", "url-shortener", TopologyFlags{HasDB: true})
	if err != nil {
		t.Fatal(err)
	}
	db := dm.Components["db"]
	if db.DbQuerySource != DbQuerySourceNone {
		t.Fatalf("dbQuerySource: want none, got %q", db.DbQuerySource)
	}
	if db.DbQueryRate != 0 {
		t.Fatalf("dbQueryRate should not backfill xact_commit, got %v", db.DbQueryRate)
	}
	if db.RequestRate == 0 {
		t.Fatalf("requestRate should still reflect pg txn rate for debug paths")
	}
}

func TestCollect_DbQuerySource_App(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		if strings.Contains(q, "db_query_duration_seconds_count") {
			if !strings.Contains(q, `pod=~"app-server.*"`) {
				t.Errorf("db_query_duration query missing app-server pod filter: %s", q)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(promVectorJSON("42"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(promEmptyJSON())
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	dm, err := c.Collect(context.Background(), "ns-db-app", "", TopologyFlags{HasDB: true})
	if err != nil {
		t.Fatal(err)
	}
	db := dm.Components["db"]
	if db.DbQuerySource != DbQuerySourceApp || db.DbQueryRate != 42 {
		t.Fatalf("db query: source=%q rate=%v", db.DbQuerySource, db.DbQueryRate)
	}
}

func TestCollect_DbTotalQueriesUsesCounterSum(t *testing.T) {
	t.Parallel()
	var totalQ string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(q, "db_query_duration_seconds_count") {
			if strings.Contains(q, "rate(") {
				_ = json.NewEncoder(w).Encode(promVectorJSON("0.5"))
				return
			}
			if strings.Contains(q, "increase(") {
				t.Errorf("db total must not use increase(): %s", q)
			}
			totalQ = q
			_ = json.NewEncoder(w).Encode(promVectorJSON("10"))
			return
		}
		_ = json.NewEncoder(w).Encode(promEmptyJSON())
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	dm, err := c.Collect(context.Background(), "ns-db-total", "", TopologyFlags{HasDB: true})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(totalQ, `sum(db_query_duration_seconds_count{`) {
		t.Fatalf("want raw counter sum, got %q", totalQ)
	}
	if strings.Contains(totalQ, "[9m]") {
		t.Fatalf("total query still windowed: %s", totalQ)
	}
	if dm.Components["db"].DbTotalQueries != 10 {
		t.Fatalf("DbTotalQueries: want 10, got %v", dm.Components["db"].DbTotalQueries)
	}
}

func TestCollect_HttpTotalsUseCounterSum(t *testing.T) {
	t.Parallel()
	var totalQ, errQ, podQ string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(q, "http_requests_total") && !strings.Contains(q, "rate(") {
			if strings.Contains(q, "increase(") {
				t.Errorf("http total must not use increase(): %s", q)
			}
			switch {
			case strings.Contains(q, `status=~"5.."`):
				errQ = q
				_ = json.NewEncoder(w).Encode(promVectorJSON("2"))
			case strings.Contains(q, "sum by (pod)"):
				podQ = q
				_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", map[string]string{
					"app-server-abc": "13",
				}))
			default:
				totalQ = q
				_ = json.NewEncoder(w).Encode(promVectorJSON("13"))
			}
			return
		}
		_ = json.NewEncoder(w).Encode(promEmptyJSON())
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	dm, err := c.Collect(context.Background(), "ns-http-total", "", TopologyFlags{})
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{totalQ, errQ, podQ} {
		if !strings.Contains(q, `pod=~"app-server.*"`) {
			t.Fatalf("want app-server pod filter, got %q", q)
		}
		if strings.Contains(q, "[9m]") {
			t.Fatalf("http total still windowed: %s", q)
		}
	}
	app := dm.Components["app-server"]
	if app.TotalRequests != 13 {
		t.Fatalf("TotalRequests: want 13, got %v", app.TotalRequests)
	}
	if app.TotalErrors9m != 2 {
		t.Fatalf("TotalErrors9m: want 2, got %v", app.TotalErrors9m)
	}
	lb := dm.Components["lb"]
	if lb.TotalRequests != 13 || lb.TotalErrors9m != 2 {
		t.Fatalf("lb copies: req=%v err=%v", lb.TotalRequests, lb.TotalErrors9m)
	}
}
