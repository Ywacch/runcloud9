package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ywacch/cloud9/pkg/loadgen"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

/*
GKE Autopilot cAdvisor limitations

On GKE Autopilot, self-managed Prometheus (kube-prometheus-stack) often cannot scrape
cAdvisor series (container_cpu_usage_seconds_total, container_memory_working_set_bytes)
because Autopilot does not expose the kubelet metrics endpoint to non-system workloads.

Fallback chain per component:
  - App-server: process_cpu_seconds_total / process_resident_memory_bytes from app /metrics.
  - DB/Cache/MQ/Worker: kube_pod_container_resource_requests (static requests, not live usage).
    ComponentMetrics sets CPUIsEstimated / MemIsEstimated when that path is used.

For real-time DB/Cache CPU on Autopilot, postgres_exporter/redis_exporter process_cpu
could be scraped in future; not wired here yet.
*/

// MetricsCollector queries the Prometheus HTTP API for a specific deployment namespace.
// We query Prometheus via its HTTP API, not a Go client library.
// The API is simple: GET /api/v1/query?query=<promql>&time=<unix>
// Response is JSON that we unmarshal into a typed struct.
type MetricsCollector struct {
	prometheusURL string
	httpClient    *http.Client
	scrapeClient  *http.Client // short timeout for in-cluster LB / RabbitMQ HTTP scrapes
	loadgen       *loadgen.LoadGenerator
	clientset     kubernetes.Interface // optional: LB metadata from ConfigMap
}

func NewMetricsCollector(prometheusURL string, lg *loadgen.LoadGenerator, clientset kubernetes.Interface) *MetricsCollector {
	return &MetricsCollector{
		prometheusURL: prometheusURL,
		httpClient:    &http.Client{Timeout: 5 * time.Second},
		scrapeClient:  &http.Client{Timeout: 2 * time.Second},
		loadgen:       lg,
		clientset:     clientset,
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

// queryMultiResult runs a PromQL instant query and returns one float per series, keyed by labelKey (e.g. "pod").
func (c *MetricsCollector) queryMultiResult(ctx context.Context, promql, labelKey string) map[string]float64 {
	out := make(map[string]float64)
	if c.prometheusURL == "" || labelKey == "" {
		return out
	}
	endpoint := fmt.Sprintf("%s/api/v1/query", c.prometheusURL)
	params := url.Values{"query": {promql}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+params.Encode(), nil)
	if err != nil {
		return out
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return out
	}
	defer resp.Body.Close()

	var pr prometheusResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return out
	}
	if pr.Status != "success" {
		return out
	}
	for i := range pr.Data.Result {
		r := &pr.Data.Result[i]
		key := r.Metric[labelKey]
		if key == "" {
			continue
		}
		valStr, ok := r.Value[1].(string)
		if !ok {
			continue
		}
		var val float64
		_, _ = fmt.Sscanf(valStr, "%f", &val)
		if math.IsNaN(val) || math.IsInf(val, 0) {
			continue
		}
		out[key] = val
	}
	return out
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

// queryWithFallbackEstimated is like queryWithFallback but reports when the kube-requests path was used.
func (c *MetricsCollector) queryWithFallbackEstimated(ctx context.Context, primary, fallback string) (float64, bool) {
	val, err := c.query(ctx, primary)
	if err != nil {
		fb, _ := c.query(ctx, fallback)
		return fb, true
	}
	if val == 0 {
		fb, _ := c.query(ctx, fallback)
		if fb > 0 {
			return fb, true
		}
	}
	return val, false
}

// queryCPUPair returns CPU cores for the main workload container only (excludes exporter sidecars).
func (c *MetricsCollector) queryCPUPair(ctx context.Context, ns, podRe, mainContainer string) (float64, bool) {
	primary := fmt.Sprintf(
		`sum(rate(container_cpu_usage_seconds_total{namespace=%q,pod=~%q,container="%s"}[30s]))`,
		ns, podRe, mainContainer,
	)
	fallback := fmt.Sprintf(
		`sum(kube_pod_container_resource_requests{namespace=%q,pod=~%q,container="%s",resource="cpu"})`,
		ns, podRe, mainContainer,
	)
	return c.queryWithFallbackEstimated(ctx, primary, fallback)
}

// queryMemPair returns working-set bytes for the main workload container only.
func (c *MetricsCollector) queryMemPair(ctx context.Context, ns, podRe, mainContainer string) (float64, bool) {
	primary := fmt.Sprintf(
		`sum(container_memory_working_set_bytes{namespace=%q,pod=~%q,container="%s"})`,
		ns, podRe, mainContainer,
	)
	fallback := fmt.Sprintf(
		`sum(kube_pod_container_resource_requests{namespace=%q,pod=~%q,container="%s",resource="memory"})`,
		ns, podRe, mainContainer,
	)
	return c.queryWithFallbackEstimated(ctx, primary, fallback)
}

func (c *MetricsCollector) kubeLimitCPU(ctx context.Context, ns, podRe, container string) float64 {
	v, _ := c.query(ctx, fmt.Sprintf(
		`sum(kube_pod_container_resource_limits{namespace=%q,pod=~%q,container="%s",resource="cpu"})`,
		ns, podRe, container,
	))
	return v
}

func (c *MetricsCollector) kubeLimitMem(ctx context.Context, ns, podRe, container string) int64 {
	v, _ := c.query(ctx, fmt.Sprintf(
		`sum(kube_pod_container_resource_limits{namespace=%q,pod=~%q,container="%s",resource="memory"})`,
		ns, podRe, container,
	))
	return int64(v)
}

// queryCPUForAppServer uses process_cpu_seconds_total from the app's /metrics (works on GKE Autopilot
// where cAdvisor series are often unavailable to self-managed Prometheus).
func (c *MetricsCollector) queryCPUForAppServer(ctx context.Context, ns, podRe string) (float64, bool) {
	primary := fmt.Sprintf(
		`sum(rate(process_cpu_seconds_total{namespace=%q,pod=~%q}[30s]))`,
		ns, podRe,
	)
	fallback := fmt.Sprintf(
		`sum(kube_pod_container_resource_requests{namespace=%q,pod=~%q,container="app-server",resource="cpu"})`,
		ns, podRe,
	)
	return c.queryWithFallbackEstimated(ctx, primary, fallback)
}

// queryMemoryForAppServer uses process_resident_memory_bytes from the app's /metrics.
func (c *MetricsCollector) queryMemoryForAppServer(ctx context.Context, ns, podRe string) (float64, bool) {
	primary := fmt.Sprintf(
		`sum(process_resident_memory_bytes{namespace=%q,pod=~%q})`,
		ns, podRe,
	)
	fallback := fmt.Sprintf(
		`sum(kube_pod_container_resource_requests{namespace=%q,pod=~%q,container="app-server",resource="memory"})`,
		ns, podRe,
	)
	return c.queryWithFallbackEstimated(ctx, primary, fallback)
}

// lbMetadataFromCluster reads deploy-time LB algorithm/backends from the nginx ConfigMap.
func (c *MetricsCollector) lbMetadataFromCluster(ctx context.Context, namespace string) (algorithm string, backends []string) {
	if c == nil || c.clientset == nil {
		return "", nil
	}
	cm, err := c.clientset.CoreV1().ConfigMaps(namespace).Get(ctx, "lb-config", metav1.GetOptions{})
	if err != nil || cm.Annotations == nil {
		return "", nil
	}
	algorithm = cm.Annotations["cloud9.io/lb-algorithm"]
	raw := cm.Annotations["cloud9.io/lb-backends"]
	if raw == "" {
		return algorithm, nil
	}
	for _, p := range strings.Split(raw, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			backends = append(backends, p)
		}
	}
	return algorithm, backends
}

// Collect queries all metrics for a deployment namespace and returns a snapshot.
// template is the deployment template id (e.g. social-feed); empty skips template-specific PromQL.
func (c *MetricsCollector) Collect(ctx context.Context, namespace, template string) (*DeploymentMetrics, error) {
	dm := &DeploymentMetrics{
		Timestamp:  time.Now(),
		Namespace:  namespace,
		Components: make(map[string]ComponentMetrics),
	}

	ns := namespace
	// Application metrics from url-shortener /metrics use this label selector (K8s scrape adds namespace).
	nsSel := fmt.Sprintf(`namespace=%q`, ns)

	// DB (postgres) — CPU/memory: cAdvisor or kube requests; connections: postgres_exporter when scraped.
	cmDB := ComponentMetrics{Component: "db"}
	dbCPU, dbCPUEst := c.queryCPUPair(ctx, ns, "postgres.*", "postgres")
	dbMem, dbMemEst := c.queryMemPair(ctx, ns, "postgres.*", "postgres")
	cmDB.CPUUsage = dbCPU
	cmDB.CPUIsEstimated = dbCPUEst
	cmDB.MemoryBytes = int64(dbMem)
	cmDB.MemIsEstimated = dbMemEst
	cmDB.CPULimit = c.kubeLimitCPU(ctx, ns, "postgres.*", "postgres")
	cmDB.MemoryLimit = c.kubeLimitMem(ctx, ns, "postgres.*", "postgres")

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

	dbActive, _ := c.query(ctx, fmt.Sprintf(
		`sum(pg_stat_activity_count{namespace=%q,state="active"})`,
		ns,
	))
	if dbActive == 0 {
		dbActive, _ = c.query(ctx, fmt.Sprintf(
			`sum(pg_stat_database_numbackends{namespace=%q})`,
			ns,
		))
	}
	cmDB.DbActiveConnections = dbActive

	maxConn, _ := c.query(ctx, fmt.Sprintf(
		`max(pg_settings{namespace=%q,name="max_connections"})`,
		ns,
	))
	cmDB.MaxConnections = maxConn

	dm.Components["db"] = cmDB

	// Cache tier: redis + cache-proxy (separate queries so exporter sidecars are not summed).
	cmCache := ComponentMetrics{Component: "cache"}
	cacheCPU1, cacheCPU1Est := c.queryCPUPair(ctx, ns, "redis.*", "redis")
	cacheCPU2, cacheCPU2Est := c.queryCPUPair(ctx, ns, "cache-proxy.*", "cache-proxy")
	cmCache.CPUUsage = cacheCPU1 + cacheCPU2
	cmCache.CPUIsEstimated = cacheCPU1Est || cacheCPU2Est
	cacheMem1, cacheMem1Est := c.queryMemPair(ctx, ns, "redis.*", "redis")
	cacheMem2, cacheMem2Est := c.queryMemPair(ctx, ns, "cache-proxy.*", "cache-proxy")
	cmCache.MemoryBytes = int64(cacheMem1 + cacheMem2)
	cmCache.MemIsEstimated = cacheMem1Est || cacheMem2Est
	cmCache.CPULimit = c.kubeLimitCPU(ctx, ns, "redis.*", "redis") + c.kubeLimitCPU(ctx, ns, "cache-proxy.*", "cache-proxy")
	cmCache.MemoryLimit = c.kubeLimitMem(ctx, ns, "redis.*", "redis") + c.kubeLimitMem(ctx, ns, "cache-proxy.*", "cache-proxy")
	// UNDERSTAND: assign cache to dm.Components after app metrics so hit/miss counters can be mirrored.

	// App-server: HTTP + cache counters from url-shortener /metrics
	cmApp := ComponentMetrics{Component: "app-server"}
	appCPU, appCPUEst := c.queryCPUForAppServer(ctx, ns, "app-server.*")
	appMem, appMemEst := c.queryMemoryForAppServer(ctx, ns, "app-server.*")
	cmApp.CPUUsage = appCPU
	cmApp.CPUIsEstimated = appCPUEst
	cmApp.MemoryBytes = int64(appMem)
	cmApp.MemIsEstimated = appMemEst

	rps, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(http_requests_total{%s}[30s]))`,
		nsSel,
	))
	cmApp.RequestRate = rps

	// UNDERSTAND: histogram_quantile needs the _bucket metric, not _total.
	latBucketRate := fmt.Sprintf(`sum(rate(http_request_duration_seconds_bucket{%s}[30s])) by (le)`, nsSel)
	p50, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.50, %s)`, latBucketRate))
	p95, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.95, %s)`, latBucketRate))
	p99, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.99, %s)`, latBucketRate))
	cmApp.P50Latency = p50
	cmApp.P95Latency = p95
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

	cmApp.CPULimit = c.kubeLimitCPU(ctx, ns, "app-server.*", "app-server")
	cmApp.MemoryLimit = c.kubeLimitMem(ctx, ns, "app-server.*", "app-server")

	dm.Components["app-server"] = cmApp

	// Copy cache metrics from app-server onto the cache component so the CachePanel can display them.
	// UNDERSTAND: cache_hits_total and cache_misses_total are emitted by the app-server (not Redis).
	// We mirror them onto the cache component so the frontend doesn't need to cross-reference.
	cmCache.CacheHits = cmApp.CacheHits
	cmCache.CacheMisses = cmApp.CacheMisses
	cmCache.CacheHitsLast9m = cmApp.CacheHitsLast9m
	cmCache.CacheMissesLast9m = cmApp.CacheMissesLast9m

	if v, _ := c.query(ctx, fmt.Sprintf(`sum(redis_memory_used_bytes{namespace=%q})`, ns)); v > 0 {
		cmCache.MemoryBytes = int64(v)
		cmCache.MemIsEstimated = false
		redisMax, _ := c.query(ctx, fmt.Sprintf(`max(redis_memory_max_bytes{namespace=%q})`, ns))
		if redisMax > 0 {
			cmCache.MemoryLimit = int64(redisMax)
		} else {
			cmCache.MemoryLimit = c.kubeLimitMem(ctx, ns, "redis.*", "redis")
		}
	}
	if v, _ := c.query(ctx, fmt.Sprintf(`sum(redis_connected_clients{namespace=%q})`, ns)); v > 0 {
		cmCache.ActiveConnections = v
	}
	rh, _ := c.query(ctx, fmt.Sprintf(`sum(rate(redis_keyspace_hits_total{namespace=%q}[30s]))`, ns))
	rm, _ := c.query(ctx, fmt.Sprintf(`sum(rate(redis_keyspace_misses_total{namespace=%q}[30s]))`, ns))
	cmCache.RedisKeyspaceHits = rh
	cmCache.RedisKeyspaceMisses = rm

	dm.Components["cache"] = cmCache

	cmMQ := ComponentMetrics{Component: "mq"}
	mqCPU, mqCPUEst := c.queryCPUPair(ctx, ns, "rabbitmq.*", "rabbitmq")
	mqMem, mqMemEst := c.queryMemPair(ctx, ns, "rabbitmq.*", "rabbitmq")
	cmMQ.CPUUsage = mqCPU
	cmMQ.CPUIsEstimated = mqCPUEst
	cmMQ.MemoryBytes = int64(mqMem)
	cmMQ.MemIsEstimated = mqMemEst
	cmMQ.CPULimit = c.kubeLimitCPU(ctx, ns, "rabbitmq.*", "rabbitmq")
	cmMQ.MemoryLimit = c.kubeLimitMem(ctx, ns, "rabbitmq.*", "rabbitmq")
	cmMQ.QueueDepth, cmMQ.ProduceRate, cmMQ.ConsumeRate, cmMQ.MessagesReady, cmMQ.MessagesUnacked =
		c.scrapeRabbitMQOverview(ctx, ns)
	dm.Components["mq"] = cmMQ

	cmWorker := ComponentMetrics{Component: "worker"}
	wCPU, wCPUEst := c.queryCPUPair(ctx, ns, "worker.*", "worker")
	wMem, wMemEst := c.queryMemPair(ctx, ns, "worker.*", "worker")
	cmWorker.CPUUsage = wCPU
	cmWorker.CPUIsEstimated = wCPUEst
	cmWorker.MemoryBytes = int64(wMem)
	cmWorker.MemIsEstimated = wMemEst
	cmWorker.CPULimit = c.kubeLimitCPU(ctx, ns, "worker.*", "worker")
	cmWorker.MemoryLimit = c.kubeLimitMem(ctx, ns, "worker.*", "worker")
	cmWorker.RequestRate = cmMQ.ConsumeRate
	dm.Components["worker"] = cmWorker

	cmLB := ComponentMetrics{Component: "lb"}
	lbCPU, lbCPUEst := c.queryCPUPair(ctx, ns, "lb.*", "nginx")
	lbMem, lbMemEst := c.queryMemPair(ctx, ns, "lb.*", "nginx")
	cmLB.CPUUsage = lbCPU
	cmLB.CPUIsEstimated = lbCPUEst
	cmLB.MemoryBytes = int64(lbMem)
	cmLB.MemIsEstimated = lbMemEst
	cmLB.CPULimit = c.kubeLimitCPU(ctx, ns, "lb.*", "nginx")
	cmLB.MemoryLimit = c.kubeLimitMem(ctx, ns, "lb.*", "nginx")
	cmLB.RequestRate = cmApp.RequestRate
	cmLB.ActiveConnections = c.scrapeNginxStubActiveConnections(ctx, ns)
	algo, backends := c.lbMetadataFromCluster(ctx, ns)
	cmLB.LBAlgorithm = algo
	cmLB.LBBackends = backends
	if c.prometheusURL != "" {
		cmLB.PerBackendRPS = c.queryMultiResult(ctx,
			fmt.Sprintf(`sum by (pod) (rate(http_requests_total{namespace=%q,pod=~"app-server.*"}[30s]))`, ns),
			"pod",
		)
	}
	dm.Components["lb"] = cmLB

	dm.TemplateMetrics = c.collectTemplateMetrics(ctx, nsSel, template)

	// Attach load generator stats to the metrics snapshot.
	// UNDERSTAND: load stats are embedded in the existing metrics stream
	// so the frontend gets everything on one WebSocket connection.
	if c.loadgen != nil {
		stats := c.loadgen.GetStats(namespace)
		dm.LoadGen = &stats
	}

	return dm, nil
}
