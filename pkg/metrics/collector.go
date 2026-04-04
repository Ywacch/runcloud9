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
	scrapeClient  *http.Client // short timeout for in-cluster LB / RabbitMQ HTTP scrapes
	loadgen       *loadgen.LoadGenerator
}

func NewMetricsCollector(prometheusURL string, lg *loadgen.LoadGenerator) *MetricsCollector {
	return &MetricsCollector{
		prometheusURL: prometheusURL,
		httpClient:    &http.Client{Timeout: 5 * time.Second},
		scrapeClient:  &http.Client{Timeout: 2 * time.Second},
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

// queryWithFallback tries the primary PromQL first. If it errors, uses fallback.
// If primary returns 0, tries fallback; if fallback is >0, assumes primary metrics
// are unavailable (e.g. cAdvisor not scraped) and returns fallback.
func (c *MetricsCollector) queryWithFallback(ctx context.Context, primary, fallback string) float64 {
	val, err := c.query(ctx, primary)
	if err != nil {
		fb, _ := c.query(ctx, fallback)
		return fb
	}
	if val == 0 {
		fb, _ := c.query(ctx, fallback)
		if fb > 0 {
			return fb
		}
	}
	return val
}

func (c *MetricsCollector) queryCPUUsageOrRequest(ctx context.Context, ns, podRe string) float64 {
	primary := fmt.Sprintf(
		`sum(rate(container_cpu_usage_seconds_total{namespace=%q,pod=~%q,container!="POD",container!=""}[15s]))`,
		ns, podRe,
	)
	fallback := fmt.Sprintf(
		`sum(kube_pod_container_resource_requests{namespace=%q,pod=~%q,resource="cpu"})`,
		ns, podRe,
	)
	return c.queryWithFallback(ctx, primary, fallback)
}

func (c *MetricsCollector) queryMemoryWorkingSetOrRequest(ctx context.Context, ns, podRe string) float64 {
	primary := fmt.Sprintf(
		`sum(container_memory_working_set_bytes{namespace=%q,pod=~%q,container!="POD",container!=""})`,
		ns, podRe,
	)
	fallback := fmt.Sprintf(
		`sum(kube_pod_container_resource_requests{namespace=%q,pod=~%q,resource="memory"})`,
		ns, podRe,
	)
	return c.queryWithFallback(ctx, primary, fallback)
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

	// DB (postgres) — prefer cAdvisor usage; fall back to kube resource requests.
	cmDB := ComponentMetrics{Component: "db"}
	cmDB.CPUUsage = c.queryCPUUsageOrRequest(ctx, ns, "postgres.*")
	cmDB.MemoryBytes = int64(c.queryMemoryWorkingSetOrRequest(ctx, ns, "postgres.*"))

	dbQPS, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(db_query_duration_seconds_count{%s}[15s]))`,
		nsSel,
	))
	cmDB.DbQueryRate = dbQPS
	dbLatSum, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(db_query_duration_seconds_sum{%s}[15s]))`,
		nsSel,
	))
	dbLatCount, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(db_query_duration_seconds_count{%s}[15s]))`,
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
	cmCache.CPUUsage = c.queryCPUUsageOrRequest(ctx, ns, "redis.*|cache-proxy.*")
	cmCache.MemoryBytes = int64(c.queryMemoryWorkingSetOrRequest(ctx, ns, "redis.*|cache-proxy.*"))
	// UNDERSTAND: assign cache to dm.Components after app metrics so hit/miss counters can be mirrored.

	// App-server: HTTP + cache counters from url-shortener /metrics
	cmApp := ComponentMetrics{Component: "app-server"}
	cmApp.CPUUsage = c.queryCPUUsageOrRequest(ctx, ns, "app-server.*")
	cmApp.MemoryBytes = int64(c.queryMemoryWorkingSetOrRequest(ctx, ns, "app-server.*"))

	rps, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(http_requests_total{%s}[15s]))`,
		nsSel,
	))
	cmApp.RequestRate = rps

	// UNDERSTAND: histogram_quantile needs the _bucket metric, not _total.
	latBucketRate := fmt.Sprintf(`sum(rate(http_request_duration_seconds_bucket{%s}[15s])) by (le)`, nsSel)
	p50, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.50, %s)`, latBucketRate))
	p95, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.95, %s)`, latBucketRate))
	p99, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.99, %s)`, latBucketRate))
	cmApp.P50Latency = p50
	cmApp.P95Latency = p95
	cmApp.P99Latency = p99

	hits, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(cache_hits_total{%s}[15s]))`,
		nsSel,
	))
	cmApp.CacheHits = hits

	misses, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(cache_misses_total{%s}[15s]))`,
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
		`sum(rate(http_requests_total{%s,status=~"5.."}[15s]))`,
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
	cmMQ.CPUUsage = c.queryCPUUsageOrRequest(ctx, ns, "rabbitmq.*")
	cmMQ.MemoryBytes = int64(c.queryMemoryWorkingSetOrRequest(ctx, ns, "rabbitmq.*"))
	cmMQ.QueueDepth, cmMQ.ProduceRate, cmMQ.ConsumeRate, cmMQ.MessagesReady, cmMQ.MessagesUnacked =
		c.scrapeRabbitMQOverview(ctx, ns)
	dm.Components["mq"] = cmMQ

	cmWorker := ComponentMetrics{Component: "worker"}
	cmWorker.CPUUsage = c.queryCPUUsageOrRequest(ctx, ns, "worker.*")
	cmWorker.MemoryBytes = int64(c.queryMemoryWorkingSetOrRequest(ctx, ns, "worker.*"))
	cmWorker.RequestRate = cmMQ.ConsumeRate
	dm.Components["worker"] = cmWorker

	cmLB := ComponentMetrics{Component: "lb"}
	cmLB.CPUUsage = c.queryCPUUsageOrRequest(ctx, ns, "lb.*")
	cmLB.MemoryBytes = int64(c.queryMemoryWorkingSetOrRequest(ctx, ns, "lb.*"))
	cmLB.RequestRate = cmApp.RequestRate
	cmLB.ActiveConnections = c.scrapeNginxStubActiveConnections(ctx, ns)
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
