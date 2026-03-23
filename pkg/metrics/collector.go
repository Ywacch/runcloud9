package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// MetricsCollector queries the Prometheus HTTP API for a specific deployment namespace.
// We query Prometheus via its HTTP API, not a Go client library.
// The API is simple: GET /api/v1/query?query=<promql>&time=<unix>
// Response is JSON that we unmarshal into a typed struct.
type MetricsCollector struct {
	prometheusURL string
	httpClient    *http.Client
}

func NewMetricsCollector(prometheusURL string) *MetricsCollector {
	return &MetricsCollector{
		prometheusURL: prometheusURL,
		httpClient:    &http.Client{Timeout: 5 * time.Second},
	}
}

// prometheusResponse is the minimal shape of a Prometheus instant query response.
type prometheusResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  [2]any            `json:"value"` // [unix_timestamp, "value_string"]
		} `json:"result"`
	} `json:"data"`
}

// query executes a PromQL instant query and returns the first result value.
// Returns 0 and no error if the query returns no results (metric not yet available).
func (c *MetricsCollector) query(ctx context.Context, promql string) (float64, error) {
	if c.prometheusURL == "" {
		return 0, nil
	}
	endpoint := fmt.Sprintf("%s/api/v1/query", c.prometheusURL)
	params := url.Values{"query": {promql}}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+params.Encode(), nil)
	if err != nil {
		return 0, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("prometheus query: %w", err)
	}
	defer resp.Body.Close()

	var pr prometheusResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return 0, fmt.Errorf("decode prometheus response: %w", err)
	}

	if pr.Status != "success" || len(pr.Data.Result) == 0 {
		return 0, nil
	}

	valStr, ok := pr.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, nil
	}

	var val float64
	_, _ = fmt.Sscanf(valStr, "%f", &val)
	return val, nil
}

// Collect queries all metrics for a deployment namespace and returns a snapshot.
func (c *MetricsCollector) Collect(ctx context.Context, namespace string) (*DeploymentMetrics, error) {
	dm := &DeploymentMetrics{
		Timestamp:  time.Now(),
		Namespace:  namespace,
		Components: make(map[string]ComponentMetrics),
	}

	ns := namespace

	// DB (postgres)
	cmDB := ComponentMetrics{Component: "db"}
	// VERIFY: Confirm `namespace` and `pod` labels on container_* series match kube-prometheus-stack
	// relabeling (Prometheus UI → Status → Targets, then run an ad-hoc query on raw series).
	// Adjust PromQL if labels differ before marking Week 7 complete.
	cpu, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(container_cpu_usage_seconds_total{namespace=%q,pod=~"postgres.*",container!=""}[1m]))`,
		ns,
	))
	cmDB.CPUUsage = cpu
	mem, _ := c.query(ctx, fmt.Sprintf(
		`sum(container_memory_working_set_bytes{namespace=%q,pod=~"postgres.*",container!=""})`,
		ns,
	))
	cmDB.MemoryBytes = int64(mem)
	dm.Components["db"] = cmDB

	// Cache tier: cache-aside redis-* and proxy redis-internal-* (matched by redis.*) + cache-proxy-*
	cmCache := ComponentMetrics{Component: "cache"}
	// VERIFY: same as above for cAdvisor series in this namespace.
	cpu, _ = c.query(ctx, fmt.Sprintf(
		`sum(rate(container_cpu_usage_seconds_total{namespace=%q,pod=~"redis.*|cache-proxy.*",container!=""}[1m]))`,
		ns,
	))
	cmCache.CPUUsage = cpu
	mem, _ = c.query(ctx, fmt.Sprintf(
		`sum(container_memory_working_set_bytes{namespace=%q,pod=~"redis.*|cache-proxy.*",container!=""})`,
		ns,
	))
	cmCache.MemoryBytes = int64(mem)
	dm.Components["cache"] = cmCache

	// App-server: HTTP + cache counters from url-shortener /metrics
	cmApp := ComponentMetrics{Component: "app-server"}
	// VERIFY: cAdvisor CPU/memory — same label confirmation as above.
	cpu, _ = c.query(ctx, fmt.Sprintf(
		`sum(rate(container_cpu_usage_seconds_total{namespace=%q,pod=~"app-server.*",container!=""}[1m]))`,
		ns,
	))
	cmApp.CPUUsage = cpu
	mem, _ = c.query(ctx, fmt.Sprintf(
		`sum(container_memory_working_set_bytes{namespace=%q,pod=~"app-server.*",container!=""})`,
		ns,
	))
	cmApp.MemoryBytes = int64(mem)

	// VERIFY: Application metrics from Go `promhttp` may or may not include `namespace` after scrape.
	// Confirm in Prometheus UI that `http_requests_total`, histogram buckets, and cache_* counters
	// expose `namespace` (or update queries to match actual labels, e.g. `pod` only).
	rps, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(http_requests_total{namespace=%q}[30s]))`,
		ns,
	))
	cmApp.RequestRate = rps

	// UNDERSTAND: histogram_quantile needs the _bucket metric, not _total.
	p99, _ := c.query(ctx, fmt.Sprintf(
		`histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket{namespace=%q}[1m])) by (le))`,
		ns,
	))
	cmApp.P99Latency = p99

	hits, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(cache_hits_total{namespace=%q}[30s]))`,
		ns,
	))
	cmApp.CacheHits = hits

	misses, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(cache_misses_total{namespace=%q}[30s]))`,
		ns,
	))
	cmApp.CacheMisses = misses

	errRate, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(http_requests_total{namespace=%q,status=~"5.."}[30s]))`,
		ns,
	))
	cmApp.ErrorRate = errRate

	dm.Components["app-server"] = cmApp

	return dm, nil
}
