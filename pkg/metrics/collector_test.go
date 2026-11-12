package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
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

func isSessionLatencyHistogramQuery(q string) bool {
	if !strings.Contains(q, "http_request_duration_seconds_bucket") &&
		!strings.Contains(q, "_worker_processing_duration_seconds_bucket") {
		return false
	}
	return strings.Contains(q, "histogram_quantile(0.50") ||
		strings.Contains(q, "histogram_quantile(0.95") ||
		strings.Contains(q, "histogram_quantile(0.99")
}

func isSessionP99HistogramQuery(q string) bool {
	return isSessionLatencyHistogramQuery(q) && strings.Contains(q, "histogram_quantile(0.99")
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

	var hitsQ, missesQ string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "cache_hits_total") && !strings.Contains(q, "rate("):
			if strings.Contains(q, "increase(") {
				t.Errorf("cache hits total must not use increase(): %s", q)
			}
			hitsQ = q
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", map[string]string{
				"app-server-abc": "150",
			}))
		case strings.Contains(q, "cache_misses_total") && !strings.Contains(q, "rate("):
			if strings.Contains(q, "increase(") {
				t.Errorf("cache misses total must not use increase(): %s", q)
			}
			missesQ = q
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", map[string]string{
				"app-server-abc": "30",
			}))
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
	if !strings.Contains(hitsQ, `sum by (pod) (cache_hits_total{`) {
		t.Fatalf("want sum by (pod) counter, got %q", hitsQ)
	}
	if !strings.Contains(hitsQ, `pod=~"app-server.*"`) {
		t.Fatalf("want app-server pod filter: %s", hitsQ)
	}
	if !strings.Contains(missesQ, `sum by (pod) (cache_misses_total{`) {
		t.Fatalf("want sum by (pod) counter, got %q", missesQ)
	}
	app := dm.Components["app-server"]
	if app.CacheHitsLast9m != 150 {
		t.Fatalf("CacheHitsLast9m: want 150, got %v", app.CacheHitsLast9m)
	}
	if app.CacheMissesLast9m != 30 {
		t.Fatalf("CacheMissesLast9m: want 30, got %v", app.CacheMissesLast9m)
	}
}

func TestCollect_CacheSessionAccumulatorSurvivesPodKill(t *testing.T) {
	t.Parallel()

	scrape := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "sum by (pod)") &&
			strings.Contains(q, "cache_hits_total") &&
			strings.Contains(q, `pod=~"app-server.*"`) &&
			!strings.Contains(q, "rate("):
			scrape++
			var pods map[string]string
			switch scrape {
			case 1:
				pods = map[string]string{"app-server-a": "100"}
			case 2:
				pods = map[string]string{"app-server-b": "0"}
			default:
				pods = map[string]string{"app-server-b": "20"}
			}
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", pods))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	ctx := context.Background()

	dm1, err := c.Collect(ctx, "ns-cache-kill", "", TopologyFlags{})
	if err != nil {
		t.Fatal(err)
	}
	if got := dm1.Components["app-server"].CacheHitsLast9m; got != 100 {
		t.Fatalf("collect 1 CacheHitsLast9m: want 100, got %v", got)
	}

	dm2, err := c.Collect(ctx, "ns-cache-kill", "", TopologyFlags{})
	if err != nil {
		t.Fatal(err)
	}
	if got := dm2.Components["app-server"].CacheHitsLast9m; got != 100 {
		t.Fatalf("collect 2 CacheHitsLast9m: want 100 (pod A gone), got %v", got)
	}

	dm3, err := c.Collect(ctx, "ns-cache-kill", "", TopologyFlags{})
	if err != nil {
		t.Fatal(err)
	}
	if got := dm3.Components["app-server"].CacheHitsLast9m; got != 120 {
		t.Fatalf("collect 3 CacheHitsLast9m: want 120, got %v", got)
	}
}

func TestCollect_Session9mAggregates(t *testing.T) {
	t.Parallel()

	var sessionP50Q, sessionP95Q, sessionP99Q string
	var podSessionP50Q, podSessionP99Q string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "sum by (pod)") &&
			strings.Contains(q, `http_requests_total`) &&
			strings.Contains(q, `status=~"5.."`) &&
			!strings.Contains(q, "rate(") &&
			!strings.Contains(q, "increase("):
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", map[string]string{
				"app-server-abc": "7",
				"app-server-def": "5",
			}))
		case strings.Contains(q, "sum by (pod)") &&
			strings.Contains(q, `http_requests_total`) &&
			strings.Contains(q, `pod=~"app-server.*"`) &&
			!strings.Contains(q, "rate(") &&
			!strings.Contains(q, "increase(") &&
			!strings.Contains(q, `status=~"5.."`):
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", map[string]string{
				"app-server-abc": "16240",
				"app-server-def": "16105",
			}))
		case strings.Contains(q, `histogram_quantile(0.50, sum by (pod, le) (rate(http_request_duration_seconds_bucket{namespace="ns-test"}`):
			podSessionP50Q = q
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", map[string]string{
				"app-server-abc": "0.010",
			}))
		case strings.Contains(q, `histogram_quantile(0.99, sum by (pod, le) (rate(http_request_duration_seconds_bucket{namespace="ns-test"}`):
			podSessionP99Q = q
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", map[string]string{
				"app-server-def": "0.042",
			}))
		case strings.Contains(q, `histogram_quantile(0.50, sum(rate(http_request_duration_seconds_bucket{namespace="ns-test"}`) &&
			strings.Contains(q, " by (le)"):
			sessionP50Q = q
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.010"))
		case strings.Contains(q, `histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{namespace="ns-test"}`) &&
			strings.Contains(q, " by (le)"):
			sessionP95Q = q
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.025"))
		case strings.Contains(q, `histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket{namespace="ns-test"}`) &&
			strings.Contains(q, " by (le)"):
			sessionP99Q = q
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

	for name, q := range map[string]string{
		"sessionP50": sessionP50Q,
		"sessionP95": sessionP95Q,
		"sessionP99": sessionP99Q,
	} {
		if q == "" {
			t.Fatalf("%s query not issued", name)
		}
		if strings.Contains(q, "[9m]") {
			t.Fatalf("%s still uses [9m]: %s", name, q)
		}
		if !strings.Contains(q, "[1m]") {
			t.Fatalf("first tick should use at least [1m] for %s: %s", name, q)
		}
	}
	if podSessionP50Q == "" || !strings.Contains(podSessionP50Q, "by (pod, le)") {
		t.Fatalf("expected per-pod session p50 query, got %q", podSessionP50Q)
	}
	if podSessionP99Q == "" || !strings.Contains(podSessionP99Q, "by (pod, le)") {
		t.Fatalf("expected per-pod session p99 query, got %q", podSessionP99Q)
	}

	app := dm.Components["app-server"]
	if app.TotalRequests != 32345 {
		t.Fatalf("app TotalRequests: want 32345, got %v", app.TotalRequests)
	}
	if app.TotalErrors9m != 12 {
		t.Fatalf("app TotalErrors9m: want 12, got %v", app.TotalErrors9m)
	}
	if app.P50Latency9m != 0.010 {
		t.Fatalf("app P50Latency9m: want 0.010, got %v", app.P50Latency9m)
	}
	if app.P95Latency9m != 0.025 {
		t.Fatalf("app P95Latency9m: want 0.025, got %v", app.P95Latency9m)
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
	if lb.P50Latency9m != 0.010 {
		t.Fatalf("lb P50Latency9m: want 0.010, got %v", lb.P50Latency9m)
	}
	if lb.P95Latency9m != 0.025 {
		t.Fatalf("lb P95Latency9m: want 0.025, got %v", lb.P95Latency9m)
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
	if byPod["app-server-abc"].TotalErrors9m != 7 {
		t.Fatalf("pod abc totalErrors9m: want 7, got %v", byPod["app-server-abc"].TotalErrors9m)
	}
	if byPod["app-server-abc"].P50Latency9m != 0.010 {
		t.Fatalf("pod abc P50Latency9m: want 0.010, got %v", byPod["app-server-abc"].P50Latency9m)
	}
	if byPod["app-server-def"].P99Latency9m != 0.042 {
		t.Fatalf("pod def P99Latency9m: want 0.042, got %v", byPod["app-server-def"].P99Latency9m)
	}
}

func TestCollect_WorkerReplicaMetrics(t *testing.T) {
	t.Parallel()

	var sessionP50Q, sessionP95Q, sessionP99Q string
	var podSessionP50Q, podSessionP99Q string
	var sawPodByWorker bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "sum by (pod)") &&
			strings.Contains(q, "_worker_errors_total") &&
			!strings.Contains(q, "rate("):
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", map[string]string{
				"worker-abc": "2",
				"worker-def": "1",
			}))
		case strings.Contains(q, "sum by (pod)") &&
			strings.Contains(q, "_worker_processed_total") &&
			!strings.Contains(q, "rate("):
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", map[string]string{
				"worker-abc": "500",
				"worker-def": "480",
			}))
		case strings.Contains(q, "sum by (pod)") &&
			strings.Contains(q, "_worker_processed_total") &&
			strings.Contains(q, "rate("):
			sawPodByWorker = true
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", map[string]string{
				"worker-abc": "10",
				"worker-def": "8",
			}))
		case strings.Contains(q, `{__name__=~".*_worker_processing_duration_seconds_bucket"`) &&
			strings.Contains(q, "by (pod, le)") &&
			strings.Contains(q, `histogram_quantile(0.50`) &&
			!strings.Contains(q, promRateRange()):
			podSessionP50Q = q
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", map[string]string{
				"worker-abc": "0.015",
			}))
		case strings.Contains(q, `{__name__=~".*_worker_processing_duration_seconds_bucket"`) &&
			strings.Contains(q, "by (pod, le)") &&
			strings.Contains(q, `histogram_quantile(0.99`) &&
			!strings.Contains(q, promRateRange()):
			podSessionP99Q = q
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", map[string]string{
				"worker-def": "0.050",
			}))
		case strings.Contains(q, `{__name__=~".*_worker_processing_duration_seconds_bucket"`) &&
			strings.Contains(q, `histogram_quantile(0.50`) &&
			strings.Contains(q, " by (le)") &&
			!strings.Contains(q, "by (pod") &&
			!strings.Contains(q, promRateRange()):
			sessionP50Q = q
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.012"))
		case strings.Contains(q, `{__name__=~".*_worker_processing_duration_seconds_bucket"`) &&
			strings.Contains(q, `histogram_quantile(0.95`) &&
			strings.Contains(q, " by (le)") &&
			!strings.Contains(q, "by (pod") &&
			!strings.Contains(q, promRateRange()):
			sessionP95Q = q
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.030"))
		case strings.Contains(q, `{__name__=~".*_worker_processing_duration_seconds_bucket"`) &&
			strings.Contains(q, `histogram_quantile(0.99`) &&
			strings.Contains(q, " by (le)") &&
			!strings.Contains(q, "by (pod") &&
			!strings.Contains(q, promRateRange()):
			sessionP99Q = q
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.045"))
		case strings.Contains(q, `{__name__=~".*_worker_processing_duration_seconds_bucket"`) &&
			strings.Contains(q, `histogram_quantile(0.50`) &&
			strings.Contains(q, promRateRange()) &&
			!strings.Contains(q, "by (pod"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.002"))
		case strings.Contains(q, `{__name__=~".*_worker_processing_duration_seconds_bucket"`) &&
			strings.Contains(q, `histogram_quantile(0.95`) &&
			strings.Contains(q, promRateRange()) &&
			!strings.Contains(q, "by (pod"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.008"))
		case strings.Contains(q, `{__name__=~".*_worker_processing_duration_seconds_bucket"`) &&
			strings.Contains(q, `histogram_quantile(0.99`) &&
			strings.Contains(q, promRateRange()) &&
			!strings.Contains(q, "by (pod"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.020"))
		case strings.Contains(q, `sum(rate({__name__=~".*_worker_processed_total",namespace="ns-worker"`) &&
			!strings.Contains(q, "by (pod)"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("18"))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	dm, err := c.Collect(context.Background(), "ns-worker", "", TopologyFlags{HasWorker: true})
	if err != nil {
		t.Fatal(err)
	}

	for name, q := range map[string]string{
		"sessionP50": sessionP50Q,
		"sessionP95": sessionP95Q,
		"sessionP99": sessionP99Q,
	} {
		if q == "" {
			t.Fatalf("%s query not issued", name)
		}
		if strings.Contains(q, "[9m]") {
			t.Fatalf("%s still uses [9m]: %s", name, q)
		}
		if !strings.Contains(q, "[1m]") {
			t.Fatalf("first tick should use at least [1m] for %s: %s", name, q)
		}
		if !strings.Contains(q, `{__name__=~".*_worker_processing_duration_seconds_bucket"`) {
			t.Fatalf("%s should query worker histogram: %s", name, q)
		}
	}
	if podSessionP50Q == "" || !strings.Contains(podSessionP50Q, "by (pod, le)") {
		t.Fatalf("expected per-pod session p50 query, got %q", podSessionP50Q)
	}
	if podSessionP99Q == "" || !strings.Contains(podSessionP99Q, "by (pod, le)") {
		t.Fatalf("expected per-pod session p99 query, got %q", podSessionP99Q)
	}
	if !sawPodByWorker {
		t.Fatal("expected per-pod worker processed rate query")
	}

	worker := dm.Components["worker"]
	if worker.P50Latency != 0.002 {
		t.Fatalf("worker P50Latency: want 0.002, got %v", worker.P50Latency)
	}
	if worker.P95Latency != 0.008 {
		t.Fatalf("worker P95Latency: want 0.008, got %v", worker.P95Latency)
	}
	if worker.P99Latency != 0.020 {
		t.Fatalf("worker P99Latency: want 0.020, got %v", worker.P99Latency)
	}
	if worker.P50Latency9m != 0.012 {
		t.Fatalf("worker P50Latency9m: want 0.012, got %v", worker.P50Latency9m)
	}
	if worker.P95Latency9m != 0.030 {
		t.Fatalf("worker P95Latency9m: want 0.030, got %v", worker.P95Latency9m)
	}
	if worker.P99Latency9m != 0.045 {
		t.Fatalf("worker P99Latency9m: want 0.045, got %v", worker.P99Latency9m)
	}
	if worker.RequestRate != 18 {
		t.Fatalf("worker RequestRate: want 18, got %v", worker.RequestRate)
	}
	if worker.TotalRequests != 980 {
		t.Fatalf("worker TotalRequests: want 980, got %v", worker.TotalRequests)
	}
	if worker.TotalErrors9m != 3 {
		t.Fatalf("worker TotalErrors9m: want 3, got %v", worker.TotalErrors9m)
	}
	if len(worker.Replicas) != 2 {
		t.Fatalf("worker replicas: want 2, got %d", len(worker.Replicas))
	}
	byPod := make(map[string]ReplicaMetrics)
	for _, r := range worker.Replicas {
		byPod[r.Name] = r
	}
	if byPod["worker-abc"].TotalRequests != 500 {
		t.Fatalf("worker abc totalRequests: want 500, got %v", byPod["worker-abc"].TotalRequests)
	}
	if byPod["worker-def"].TotalErrors9m != 1 {
		t.Fatalf("worker def totalErrors9m: want 1, got %v", byPod["worker-def"].TotalErrors9m)
	}
	if byPod["worker-abc"].P50Latency9m != 0.015 {
		t.Fatalf("worker abc P50Latency9m: want 0.015, got %v", byPod["worker-abc"].P50Latency9m)
	}
	if byPod["worker-def"].P99Latency9m != 0.050 {
		t.Fatalf("worker def P99Latency9m: want 0.050, got %v", byPod["worker-def"].P99Latency9m)
	}
}

func TestCollect_SessionP99UsesElapsedWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		elapsed   time.Duration
		wantRange string
	}{
		{name: "fifteenMinutes", elapsed: 15 * time.Minute, wantRange: "[15m]"},
		{name: "maxCapThirtyMinutes", elapsed: 45 * time.Minute, wantRange: "[30m]"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var sessionP99Q string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				q := r.URL.Query().Get("query")
				w.Header().Set("Content-Type", "application/json")
				if strings.Contains(q, `histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket{namespace="ns-p99"}`) {
					sessionP99Q = q
					_ = json.NewEncoder(w).Encode(promVectorJSON("0.050"))
					return
				}
				_ = json.NewEncoder(w).Encode(promEmptyJSON())
			}))
			t.Cleanup(srv.Close)

			c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
			now := time.Now()
			c.sessionAcc.mu.Lock()
			c.sessionAcc.ns["ns-p99"] = &nsSession{
				firstObserve: now.Add(-tc.elapsed),
				series:       make(map[string]*seriesAcc),
			}
			c.sessionAcc.mu.Unlock()

			dm, err := c.Collect(context.Background(), "ns-p99", "", TopologyFlags{})
			if err != nil {
				t.Fatal(err)
			}
			if sessionP99Q == "" {
				t.Fatal("session p99 query not issued")
			}
			if strings.Contains(sessionP99Q, "[9m]") {
				t.Fatalf("session p99 still uses [9m]: %s", sessionP99Q)
			}
			if !strings.Contains(sessionP99Q, tc.wantRange) {
				t.Fatalf("session p99 range: want %s in %s", tc.wantRange, sessionP99Q)
			}
			if got := dm.Components["app-server"].P99Latency9m; got != 0.050 {
				t.Fatalf("P99Latency9m: want 0.050, got %v", got)
			}
		})
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
		if strings.Contains(q, "[9m]") {
			t.Fatalf("query must not use deprecated [9m] session window: %s", q)
		}
		// Session p99 uses elapsed time clamped to [1m, 30m]; live rates stay [10s].
		if strings.Contains(q, "[1m]") && !isSessionLatencyHistogramQuery(q) {
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
	var sawPodLive bool
	for _, q := range seen {
		if strings.Contains(q, "http_request_duration_seconds_bucket") && strings.Contains(q, "histogram_quantile") {
			if strings.Contains(q, promRateRange()) {
				if strings.Contains(q, "by (pod, le)") {
					sawPodLive = true
					continue
				}
				// Catalog PerApp quantiles group by namespace and app. The le label stays on the inner sum.
				if strings.Contains(q, " by (le)") || strings.Contains(q, "by (namespace, app, le)") {
					continue
				}
				t.Fatalf("live histogram should use by (le), by (namespace, app, le), or by (pod, le): %s", q)
			}
			if strings.Contains(q, "[9m]") {
				t.Fatalf("session histogram should not use [9m]: %s", q)
			}
		}
	}
	if !sawPodLive {
		t.Fatal("expected per-pod live latency histogram queries")
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

func TestCollect_DBCPUUsesReplicaContainerWhenPrimaryGone(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "container_cpu_usage_seconds_total") && strings.Contains(q, `container="postgres-replica"`):
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.11"))
		case strings.Contains(q, "kube_pod_container_resource_limits") && strings.Contains(q, `container="postgres-replica"`) && strings.Contains(q, `resource="cpu"`):
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.5"))
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
	if db.CPUUsage != 0.11 {
		t.Fatalf("db CPU: want 0.11 from postgres-replica container, got %v", db.CPUUsage)
	}
	if db.CPUIsEstimated {
		t.Fatal("db CPUIsEstimated: want false for replica cAdvisor series")
	}
	if db.CPULimit != 0.5 {
		t.Fatalf("db CPULimit: want 0.5 from replica limits, got %v", db.CPULimit)
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
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", map[string]string{
				"app-server-abc": "10",
			}))
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
	if !strings.Contains(totalQ, `sum by (pod) (db_query_duration_seconds_count{`) {
		t.Fatalf("want sum by (pod) counter, got %q", totalQ)
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
	var httpPodQ, errPodQ string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(q, "http_requests_total") && !strings.Contains(q, "rate(") {
			if strings.Contains(q, "increase(") {
				t.Errorf("http total must not use increase(): %s", q)
			}
			switch {
			case strings.Contains(q, `status=~"5.."`) && strings.Contains(q, "sum by (pod)"):
				errPodQ = q
				_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", map[string]string{
					"app-server-abc": "2",
				}))
			case strings.Contains(q, "sum by (pod)"):
				httpPodQ = q
				_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", map[string]string{
					"app-server-abc": "13",
				}))
			default:
				t.Errorf("unexpected http total query: %s", q)
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
	for _, q := range []string{httpPodQ, errPodQ} {
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

func TestCollect_SessionAccumulatorSurvivesPodKill(t *testing.T) {
	t.Parallel()

	scrape := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "sum by (pod)") &&
			strings.Contains(q, `http_requests_total`) &&
			strings.Contains(q, `pod=~"app-server.*"`) &&
			!strings.Contains(q, `status=~"5.."`) &&
			!strings.Contains(q, "rate("):
			scrape++
			var pods map[string]string
			switch scrape {
			case 1:
				pods = map[string]string{"app-server-a": "10"}
			case 2:
				pods = map[string]string{"app-server-b": "0"}
			default:
				pods = map[string]string{"app-server-b": "4"}
			}
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", pods))
		case strings.Contains(q, "sum by (pod)") &&
			strings.Contains(q, `http_requests_total`) &&
			strings.Contains(q, `status=~"5.."`):
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		case strings.Contains(q, "sum by (pod)") &&
			strings.Contains(q, "db_query_duration_seconds_count"):
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	ctx := context.Background()

	dm1, err := c.Collect(ctx, "ns-kill", "", TopologyFlags{})
	if err != nil {
		t.Fatal(err)
	}
	if got := dm1.Components["app-server"].TotalRequests; got != 10 {
		t.Fatalf("collect 1 TotalRequests: want 10, got %v", got)
	}

	dm2, err := c.Collect(ctx, "ns-kill", "", TopologyFlags{})
	if err != nil {
		t.Fatal(err)
	}
	if got := dm2.Components["app-server"].TotalRequests; got != 10 {
		t.Fatalf("collect 2 TotalRequests: want 10 (pod A gone), got %v", got)
	}

	dm3, err := c.Collect(ctx, "ns-kill", "", TopologyFlags{})
	if err != nil {
		t.Fatal(err)
	}
	if got := dm3.Components["app-server"].TotalRequests; got != 14 {
		t.Fatalf("collect 3 TotalRequests: want 14, got %v", got)
	}
}

func TestCollect_WorkerSessionAccumulatorSurvivesPodKill(t *testing.T) {
	t.Parallel()

	scrape := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "sum by (pod)") &&
			strings.Contains(q, "_worker_processed_total") &&
			!strings.Contains(q, "rate("):
			scrape++
			var pods map[string]string
			switch scrape {
			case 1:
				pods = map[string]string{"worker-a": "100"}
			case 2:
				pods = map[string]string{"worker-b": "0"}
			default:
				pods = map[string]string{"worker-b": "25"}
			}
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", pods))
		case strings.Contains(q, "sum by (pod)") &&
			strings.Contains(q, "_worker_errors_total") &&
			!strings.Contains(q, "rate("):
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	ctx := context.Background()

	dm1, err := c.Collect(ctx, "ns-worker-kill", "", TopologyFlags{HasWorker: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := dm1.Components["worker"].TotalRequests; got != 100 {
		t.Fatalf("collect 1 TotalRequests: want 100, got %v", got)
	}

	dm2, err := c.Collect(ctx, "ns-worker-kill", "", TopologyFlags{HasWorker: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := dm2.Components["worker"].TotalRequests; got != 100 {
		t.Fatalf("collect 2 TotalRequests: want 100 (pod A gone), got %v", got)
	}

	dm3, err := c.Collect(ctx, "ns-worker-kill", "", TopologyFlags{HasWorker: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := dm3.Components["worker"].TotalRequests; got != 125 {
		t.Fatalf("collect 3 TotalRequests: want 125, got %v", got)
	}
}
