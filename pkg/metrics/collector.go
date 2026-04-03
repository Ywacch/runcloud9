package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"time"

	"github.com/ywacch/cloud9/pkg/loadgen"
)

// MetricsCollector queries the Prometheus HTTP API for a specific deployment namespace.
// We query Prometheus via its HTTP API, not a Go client library.
// The API is simple: GET /api/v1/query?query=<promql>&time=<unix>
// Response is JSON that we unmarshal into a typed struct.
type MetricsCollector struct {
	prometheusURL string
	httpClient    *http.Client
	loadgen       *loadgen.LoadGenerator
}

func NewMetricsCollector(prometheusURL string, lg *loadgen.LoadGenerator) *MetricsCollector {
	return &MetricsCollector{
		prometheusURL: prometheusURL,
		httpClient:    &http.Client{Timeout: 5 * time.Second},
		loadgen:       lg,
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
	// encoding/json can't encode NaN/Inf; sanitize so the WS stream never dies.
	if math.IsNaN(val) || math.IsInf(val, 0) {
		return 0, nil
	}
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
	// Application metrics from url-shortener /metrics use this label selector (K8s scrape adds namespace).
	nsSel := fmt.Sprintf(`namespace=%q`, ns)

	// DB (postgres) — resource requests from kube-state-metrics (cAdvisor unavailable on GKE Autopilot).
	// TODO: For true real-time usage, pull container metrics from Cloud Monitoring API.
	cmDB := ComponentMetrics{Component: "db"}
	cpu, _ := c.query(ctx, fmt.Sprintf(
		`sum(kube_pod_container_resource_requests{namespace=%q,pod=~"postgres.*",resource="cpu"})`,
		ns,
	))
	cmDB.CPUUsage = cpu
	mem, _ := c.query(ctx, fmt.Sprintf(
		`sum(kube_pod_container_resource_requests{namespace=%q,pod=~"postgres.*",resource="memory"})`,
		ns,
	))
	cmDB.MemoryBytes = int64(mem)

	dbQPS, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(db_query_duration_seconds_count{%s}[30s]))`,
		nsSel,
	))
	cmDB.DbQueryRate = dbQPS
	dbLatSum, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(db_query_duration_seconds_sum{%s}[30s]))`,
		nsSel,
	))
	dbLatCount, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(db_query_duration_seconds_count{%s}[30s]))`,
		nsSel,
	))
	if dbLatCount > 0 {
		cmDB.DbQueryLatency = dbLatSum / dbLatCount
	}
	dbTotal, _ := c.query(ctx, fmt.Sprintf(
		`sum(increase(db_query_duration_seconds_count{%s}[9m]))`,
		nsSel,
	))
	cmDB.DbTotalQueries = dbTotal

	dm.Components["db"] = cmDB

	// Cache tier: cache-aside redis-* and proxy redis-internal-* (matched by redis.*) + cache-proxy-*
	cmCache := ComponentMetrics{Component: "cache"}
	cpu, _ = c.query(ctx, fmt.Sprintf(
		`sum(kube_pod_container_resource_requests{namespace=%q,pod=~"redis.*|cache-proxy.*",resource="cpu"})`,
		ns,
	))
	cmCache.CPUUsage = cpu
	mem, _ = c.query(ctx, fmt.Sprintf(
		`sum(kube_pod_container_resource_requests{namespace=%q,pod=~"redis.*|cache-proxy.*",resource="memory"})`,
		ns,
	))
	cmCache.MemoryBytes = int64(mem)
	// UNDERSTAND: assign cache to dm.Components after app metrics so hit/miss counters can be mirrored.

	// App-server: HTTP + cache counters from url-shortener /metrics
	cmApp := ComponentMetrics{Component: "app-server"}
	cpu, _ = c.query(ctx, fmt.Sprintf(
		`sum(kube_pod_container_resource_requests{namespace=%q,pod=~"app-server.*",resource="cpu"})`,
		ns,
	))
	cmApp.CPUUsage = cpu
	mem, _ = c.query(ctx, fmt.Sprintf(
		`sum(kube_pod_container_resource_requests{namespace=%q,pod=~"app-server.*",resource="memory"})`,
		ns,
	))
	cmApp.MemoryBytes = int64(mem)

	rps, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(http_requests_total{%s}[30s]))`,
		nsSel,
	))
	cmApp.RequestRate = rps

	// UNDERSTAND: histogram_quantile needs the _bucket metric, not _total.
	p99, _ := c.query(ctx, fmt.Sprintf(
		`histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket{%s}[1m])) by (le))`,
		nsSel,
	))
	cmApp.P99Latency = p99

	hits, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(cache_hits_total{%s}[30s]))`,
		nsSel,
	))
	cmApp.CacheHits = hits

	misses, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(cache_misses_total{%s}[30s]))`,
		nsSel,
	))
	cmApp.CacheMisses = misses

	hits9m, _ := c.query(ctx, fmt.Sprintf(
		`sum(increase(cache_hits_total{%s}[9m]))`,
		nsSel,
	))
	cmApp.CacheHitsLast9m = hits9m

	misses9m, _ := c.query(ctx, fmt.Sprintf(
		`sum(increase(cache_misses_total{%s}[9m]))`,
		nsSel,
	))
	cmApp.CacheMissesLast9m = misses9m

	errRate, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(http_requests_total{%s,status=~"5.."}[30s]))`,
		nsSel,
	))
	cmApp.ErrorRate = errRate

	totalReqs, _ := c.query(ctx, fmt.Sprintf(
		`sum(increase(http_requests_total{%s}[9m]))`,
		nsSel,
	))
	cmApp.TotalRequests = totalReqs

	dm.Components["app-server"] = cmApp

	// Copy cache metrics from app-server onto the cache component so the CachePanel can display them.
	// UNDERSTAND: cache_hits_total and cache_misses_total are emitted by the app-server (not Redis).
	// We mirror them onto the cache component so the frontend doesn't need to cross-reference.
	cmCache.CacheHits = cmApp.CacheHits
	cmCache.CacheMisses = cmApp.CacheMisses
	cmCache.CacheHitsLast9m = cmApp.CacheHitsLast9m
	cmCache.CacheMissesLast9m = cmApp.CacheMissesLast9m
	dm.Components["cache"] = cmCache

	cmMQ := ComponentMetrics{Component: "mq"}
	cpu, _ = c.query(ctx, fmt.Sprintf(
		`sum(kube_pod_container_resource_requests{namespace=%q,pod=~"rabbitmq.*",resource="cpu"})`,
		ns,
	))
	cmMQ.CPUUsage = cpu
	mem, _ = c.query(ctx, fmt.Sprintf(
		`sum(kube_pod_container_resource_requests{namespace=%q,pod=~"rabbitmq.*",resource="memory"})`,
		ns,
	))
	cmMQ.MemoryBytes = int64(mem)
	dm.Components["mq"] = cmMQ

	cmWorker := ComponentMetrics{Component: "worker"}
	cpu, _ = c.query(ctx, fmt.Sprintf(
		`sum(kube_pod_container_resource_requests{namespace=%q,pod=~"worker.*",resource="cpu"})`,
		ns,
	))
	cmWorker.CPUUsage = cpu
	mem, _ = c.query(ctx, fmt.Sprintf(
		`sum(kube_pod_container_resource_requests{namespace=%q,pod=~"worker.*",resource="memory"})`,
		ns,
	))
	cmWorker.MemoryBytes = int64(mem)
	dm.Components["worker"] = cmWorker

	cmLB := ComponentMetrics{Component: "lb"}
	cpu, _ = c.query(ctx, fmt.Sprintf(
		`sum(kube_pod_container_resource_requests{namespace=%q,pod=~"lb.*",resource="cpu"})`,
		ns,
	))
	cmLB.CPUUsage = cpu
	mem, _ = c.query(ctx, fmt.Sprintf(
		`sum(kube_pod_container_resource_requests{namespace=%q,pod=~"lb.*",resource="memory"})`,
		ns,
	))
	cmLB.MemoryBytes = int64(mem)
	dm.Components["lb"] = cmLB

	// Attach load generator stats to the metrics snapshot.
	// UNDERSTAND: load stats are embedded in the existing metrics stream
	// so the frontend gets everything on one WebSocket connection.
	if c.loadgen != nil {
		stats := c.loadgen.GetStats(namespace)
		dm.LoadGen = &stats
	}

	return dm, nil
}
