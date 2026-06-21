package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/ywacch/cloud9/pkg/loadgen"
	"github.com/ywacch/cloud9/pkg/templates"
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
	loadgen       loadgen.LoadGenerator
	clientset     kubernetes.Interface // optional: LB metadata from ConfigMap
	debugMetrics  bool
	eventBus      *EventBus
	thresholds    *ThresholdDetector
	cascade       *CascadeDetector
	historyMu     sync.RWMutex
	history       map[string]*RingBuffer // namespace → last 300 snapshots
	mqRateMu      sync.Mutex
	mqRates       map[string]rabbitCounterSnapshot
}

// NewMetricsCollector wires Prometheus scraping and optional event/threshold pipelines.
func NewMetricsCollector(prometheusURL string, lg loadgen.LoadGenerator, clientset kubernetes.Interface, eventBus *EventBus) *MetricsCollector {
	return &MetricsCollector{
		prometheusURL: prometheusURL,
		httpClient:    &http.Client{Timeout: 5 * time.Second},
		scrapeClient:  &http.Client{Timeout: 2 * time.Second},
		loadgen:       lg,
		clientset:     clientset,
		eventBus:      eventBus,
		thresholds:    NewThresholdDetector(),
		cascade:       NewCascadeDetector(),
		mqRates:       make(map[string]rabbitCounterSnapshot),
	}
}

// SetDebugMetrics enables/disables per-tick metric source comparison logs.
func (c *MetricsCollector) SetDebugMetrics(enabled bool) {
	if c == nil {
		return
	}
	c.debugMetrics = enabled
}

func (c *MetricsCollector) debugCompare(component, metric string, primary, fallback float64, primarySource, fallbackSource string) {
	if c == nil || !c.debugMetrics {
		return
	}
	diff := ""
	if fallback > 0 {
		pct := ((primary - fallback) / fallback) * 100
		diff = fmt.Sprintf(" (diff: %+.1f%%)", pct)
	}
	fmt.Printf("DEBUG metrics %s.%s: primary=%.4f [%s] fallback=%.4f [%s]%s\n",
		component, metric, primary, primarySource, fallback, fallbackSource, diff)
}

// Cleanup removes per-namespace event and threshold state (call when a deployment ends).
func (c *MetricsCollector) Cleanup(namespace string) {
	if c == nil {
		return
	}
	if c.eventBus != nil {
		c.eventBus.Cleanup(namespace)
	}
	if c.thresholds != nil {
		c.thresholds.Cleanup(namespace)
	}
	if c.cascade != nil {
		c.cascade.Cleanup(namespace)
	}
	c.historyMu.Lock()
	if c.history != nil {
		delete(c.history, namespace)
	}
	c.historyMu.Unlock()
	c.mqRateMu.Lock()
	delete(c.mqRates, namespace)
	c.mqRateMu.Unlock()
}

// RingBufferFor returns the snapshot ring for a namespace, or nil if none yet.
func (c *MetricsCollector) RingBufferFor(namespace string) *RingBuffer {
	if c == nil || namespace == "" {
		return nil
	}
	c.historyMu.RLock()
	defer c.historyMu.RUnlock()
	return c.history[namespace]
}

// SessionHistory returns oldest→newest snapshots for replay APIs (full buffer, not downsampled).
func (c *MetricsCollector) SessionHistory(namespace string) []DeploymentMetrics {
	rb := c.RingBufferFor(namespace)
	if rb == nil {
		return nil
	}
	return rb.All()
}

// PeakSummaryForNamespace prefers aggregates from session history when any snapshots exist;
// otherwise falls back to a single last Collect snapshot (e.g. first teardown tick).
func (c *MetricsCollector) PeakSummaryForNamespace(namespace string, last *DeploymentMetrics) map[string]any {
	if c == nil {
		return PeakSummary(last)
	}
	rb := c.RingBufferFor(namespace)
	if rb != nil && rb.Len() > 0 {
		return PeakSummaryFromHistory(rb)
	}
	return PeakSummary(last)
}

// DownsampledSessionHistory returns a compact series for JSON persistence at teardown.
func (c *MetricsCollector) DownsampledSessionHistory(namespace string) []DeploymentMetrics {
	rb := c.RingBufferFor(namespace)
	if rb == nil || rb.Len() == 0 {
		return nil
	}
	return rb.DownsampledHistory()
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

// queryScalar runs an instant PromQL query. present is false when the result vector is empty
// (no matching series). A scalar 0 with matching series sets present=true — distinct from “no data”.
func (c *MetricsCollector) queryScalar(ctx context.Context, promql string) (val float64, present bool, err error) {
	if c.prometheusURL == "" {
		return 0, false, nil
	}
	endpoint := fmt.Sprintf("%s/api/v1/query", c.prometheusURL)
	params := url.Values{"query": {promql}}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+params.Encode(), nil)
	if err != nil {
		return 0, false, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, false, fmt.Errorf("prometheus query: %w", err)
	}
	defer resp.Body.Close()

	var pr prometheusResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return 0, false, fmt.Errorf("decode prometheus response: %w", err)
	}

	if pr.Status != "success" || len(pr.Data.Result) == 0 {
		return 0, false, nil
	}

	valStr, ok := pr.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, true, nil
	}

	_, _ = fmt.Sscanf(valStr, "%f", &val)
	// encoding/json can't encode NaN/Inf; sanitize so the WS stream never dies.
	if math.IsNaN(val) || math.IsInf(val, 0) {
		val = 0
	}
	return val, true, nil
}

// query executes a PromQL instant query and returns the first result value.
// Returns 0 and no error if the query returns no results (metric not yet available).
func (c *MetricsCollector) query(ctx context.Context, promql string) (float64, error) {
	v, _, err := c.queryScalar(ctx, promql)
	return v, err
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
	val, present, err := c.queryScalar(ctx, primary)
	if err != nil {
		fb, _, _ := c.queryScalar(ctx, fallback)
		return fb
	}
	if present {
		return val
	}
	fb, fbPresent, _ := c.queryScalar(ctx, fallback)
	if fbPresent && fb > 0 {
		return fb
	}
	return 0
}

// queryWithFallbackEstimated is like queryWithFallback but reports when the kube-requests path was used.
func (c *MetricsCollector) queryWithFallbackEstimated(ctx context.Context, primary, fallback string) (float64, bool) {
	val, present, err := c.queryScalar(ctx, primary)
	if err != nil {
		fb, _, _ := c.queryScalar(ctx, fallback)
		return fb, true
	}
	if present {
		return val, false
	}
	fb, fbPresent, _ := c.queryScalar(ctx, fallback)
	if fbPresent && fb > 0 {
		return fb, true
	}
	return 0, false
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

// queryProcessCPU uses process_cpu_seconds_total from the app's /metrics (works on GKE Autopilot
// where cAdvisor series are often unavailable to self-managed Prometheus).
func (c *MetricsCollector) queryProcessCPU(ctx context.Context, ns, podRe, fallbackContainer string) (float64, bool) {
	primary := fmt.Sprintf(
		`sum(rate(process_cpu_seconds_total{namespace=%q,pod=~%q}[30s]))`,
		ns, podRe,
	)
	fallback := fmt.Sprintf(
		`sum(kube_pod_container_resource_requests{namespace=%q,pod=~%q,container="%s",resource="cpu"})`,
		ns, podRe, fallbackContainer,
	)
	return c.queryWithFallbackEstimated(ctx, primary, fallback)
}

// queryProcessMemory uses process_resident_memory_bytes from the app's /metrics.
func (c *MetricsCollector) queryProcessMemory(ctx context.Context, ns, podRe, fallbackContainer string) (float64, bool) {
	primary := fmt.Sprintf(
		`sum(process_resident_memory_bytes{namespace=%q,pod=~%q})`,
		ns, podRe,
	)
	fallback := fmt.Sprintf(
		`sum(kube_pod_container_resource_requests{namespace=%q,pod=~%q,container="%s",resource="memory"})`,
		ns, podRe, fallbackContainer,
	)
	return c.queryWithFallbackEstimated(ctx, primary, fallback)
}

func (c *MetricsCollector) queryContainerCPUPrimaryOnly(ctx context.Context, ns, podRe, container string) (float64, bool) {
	primary := fmt.Sprintf(
		`sum(rate(container_cpu_usage_seconds_total{namespace=%q,pod=~%q,container="%s"}[30s]))`,
		ns, podRe, container,
	)
	v, present, _ := c.queryScalar(ctx, primary)
	return v, present
}

func (c *MetricsCollector) queryContainerMemoryPrimaryOnly(ctx context.Context, ns, podRe, container string) (float64, bool) {
	primary := fmt.Sprintf(
		`sum(container_memory_working_set_bytes{namespace=%q,pod=~%q,container="%s"})`,
		ns, podRe, container,
	)
	v, present, _ := c.queryScalar(ctx, primary)
	return v, present
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

// nginxWorkerConnections matches worker_connections in pkg/deployer/k8s.go (LB nginx template).
const nginxWorkerConnections = 1024

func redisCommandPattern(template string) string {
	switch template {
	case "social-feed":
		return "^(zadd|zrevrange|zremrangebyrank)$"
	default:
		return "^(get|set|del|mget|mset)$"
	}
}

func redisLatencyCommandPattern(template string) string {
	switch template {
	case "social-feed":
		return "^(zadd|zrevrange|zremrangebyrank)$"
	default:
		return "^(get|set)$"
	}
}

// appServerPromSelector scopes app-instrumented Prometheus series to app-server pods.
// Worker and fanout-worker deployments live in the same namespace but are not app SQL.
func appServerPromSelector(ns string) string {
	return fmt.Sprintf(`namespace=%q,pod=~"app-server.*"`, ns)
}

// applyStorageProfiles sets storageEngine, storageRole, and storageIo stubs from design topology.
func applyStorageProfiles(dm *DeploymentMetrics, topo TopologyFlags) {
	if dm == nil {
		return
	}
	if topo.HasDB {
		if cm, ok := dm.Components["db"]; ok {
			engine := StorageEngineID(topo.DBEngine)
			if engine == "" {
				engine = StorageEnginePostgreSQL
			}
			role := topo.DBRole
			if role == "" {
				role = StorageRolePrimary
			}
			ApplyStorageProfile(&cm, engine, role)
			dm.Components["db"] = cm
		}
	}
	if topo.HasCache {
		if cm, ok := dm.Components["cache"]; ok {
			engine := StorageEngineID(topo.CacheEngine)
			if engine == "" {
				engine = StorageEngineRedis
			}
			role := topo.CacheRole
			if role == "" {
				role = StorageRoleCacheAside
			}
			ApplyStorageProfile(&cm, engine, role)
			dm.Components["cache"] = cm
		}
	}
}

func annotateCacheForTemplate(cm *ComponentMetrics, template string) {
	if cm == nil {
		return
	}
	switch template {
	case "social-feed":
		cm.CacheStrategy = "timeline-store"
		cm.CacheMetricMode = "sorted-set timelines"
		cm.CacheReadOperation = "ZREVRANGE"
		cm.CacheWriteOperation = "ZADD"
	case "url-shortener", "ecommerce":
		cm.CacheStrategy = "cache-aside"
		cm.CacheMetricMode = "keyspace hits/misses"
		cm.CacheReadOperation = "GET"
		cm.CacheWriteOperation = "SET"
	}
}

// Collect queries all metrics for a deployment namespace and returns a snapshot.
// template is the deployment template id (e.g. social-feed); empty skips template-specific PromQL.
// topo is derived from the saved deployment design (may be zero); used for narratives and cache-strategy overlay.
func (c *MetricsCollector) Collect(ctx context.Context, namespace, template string, topo TopologyFlags) (*DeploymentMetrics, error) {
	dm := &DeploymentMetrics{
		Timestamp:  time.Now(),
		Namespace:  namespace,
		Components: make(map[string]ComponentMetrics),
	}

	ns := namespace
	// Application metrics from url-shortener /metrics use this label selector (K8s scrape adds namespace).
	nsSel := fmt.Sprintf(`namespace=%q`, ns)
	// App-server SQL histogram only — excludes worker/fanout-worker pods when they export db_query_duration_seconds.
	appServerSel := appServerPromSelector(ns)

	// DB (postgres) — CPU/memory: cAdvisor or kube requests; connections: postgres_exporter when scraped.
	cmDB := ComponentMetrics{Component: "db"}
	dbCPU, dbCPUEst := c.queryProcessCPU(ctx, ns, "postgres.*", "postgres")
	dbMem, dbMemEst := c.queryProcessMemory(ctx, ns, "postgres.*", "postgres")
	cmDB.CPUUsage = dbCPU
	cmDB.CPUIsEstimated = dbCPUEst
	cmDB.MemoryBytes = int64(dbMem)
	cmDB.MemIsEstimated = dbMemEst
	cmDB.CPULimit = c.kubeLimitCPU(ctx, ns, "postgres.*", "postgres")
	cmDB.MemoryLimit = c.kubeLimitMem(ctx, ns, "postgres.*", "postgres")

	dbQPS, dbQPSPresent, _ := c.queryScalar(ctx, fmt.Sprintf(
		`sum(rate(db_query_duration_seconds_count{%s}[30s]))`,
		appServerSel,
	))
	if dbQPSPresent {
		cmDB.DbQueryRate = dbQPS
		cmDB.DbQuerySource = DbQuerySourceApp
	} else {
		cmDB.DbQueryRate = 0
		cmDB.DbQuerySource = DbQuerySourceNone
	}
	dbLatSum, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(db_query_duration_seconds_sum{%s}[30s]))`,
		appServerSel,
	))
	dbLatCount, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(db_query_duration_seconds_count{%s}[30s]))`,
		appServerSel,
	))
	if dbLatCount > 0 {
		cmDB.DbQueryLatency = dbLatSum / dbLatCount
	}
	cmDB.AvgLatency = cmDB.DbQueryLatency
	dbTotal, _ := c.query(ctx, fmt.Sprintf(
		`sum(increase(db_query_duration_seconds_count{%s}[9m]))`,
		appServerSel,
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
	// PostgreSQL-native throughput from postgres_exporter; fallback to app-side DB query rate.
	pgTxnRate, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(pg_stat_database_xact_commit{namespace=%q,datname!=""}[30s]))`,
		ns,
	))
	pgRollbackRate, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(pg_stat_database_xact_rollback{namespace=%q,datname!=""}[30s]))`,
		ns,
	))
	if pgTxnRate+pgRollbackRate > 0 {
		cmDB.RequestRate = pgTxnRate + pgRollbackRate
	} else if dbQPSPresent {
		cmDB.RequestRate = dbQPS
	}
	// DbQueryRate is app SQL throughput only (dbQuerySource=app). Do not backfill from
	// xact_commit — that includes postgres_exporter sidecar traffic (~5/s at idle).
	c.debugCompare("db", "rps", pgTxnRate+pgRollbackRate, dbQPS, "pg_stat_database_xact_commit+rollback", "app db_query_duration_count")
	c.debugCompare("db", "cpu", 0, dbCPU, "none (no native metric)", "kube_requests/cadvisor_or_fallback")

	dm.Components["db"] = cmDB

	// Cache tier: redis + cache-proxy (separate queries so exporter sidecars are not summed).
	cmCache := ComponentMetrics{Component: "cache"}
	redisCPUUser, redisUserPresent, _ := c.queryScalar(ctx, fmt.Sprintf(
		`sum(rate(redis_cpu_user_seconds_total{namespace=%q}[30s]))`,
		ns,
	))
	redisCPUSys, redisSysPresent, _ := c.queryScalar(ctx, fmt.Sprintf(
		`sum(rate(redis_cpu_sys_seconds_total{namespace=%q}[30s]))`,
		ns,
	))
	redisExporterPresent := redisUserPresent || redisSysPresent
	redisCPUFromExporter := redisCPUUser + redisCPUSys
	cacheCPU1 := 0.0
	cacheCPU1Est := true
	if redisExporterPresent {
		cacheCPU1 = redisCPUFromExporter
		cacheCPU1Est = false
	} else {
		cacheCPU1, cacheCPU1Est = c.queryContainerCPUPrimaryOnly(ctx, ns, "redis.*", "redis")
		cacheCPU1Est = !cacheCPU1Est
	}
	if c.debugMetrics {
		kubeEstCPU, _ := c.queryCPUPair(ctx, ns, "redis.*", "redis")
		c.debugCompare("cache", "cpu", redisCPUFromExporter, kubeEstCPU, "redis_cpu_user+sys", "kube_requests")
	}
	// Cache-proxy is a Go process; use scrape-side process_* series (works on Autopilot without cAdvisor).
	cacheCPU2, cacheProxyPresent, _ := c.queryScalar(ctx, fmt.Sprintf(
		`sum(rate(process_cpu_seconds_total{namespace=%q,pod=~%q}[30s]))`,
		ns, "cache-proxy.*",
	))
	cacheCPU2Est := !cacheProxyPresent
	cmCache.CPUUsage = cacheCPU1 + cacheCPU2
	// Mark cache CPU estimated only when both sources are missing.
	// If Redis exporter is live but cache-proxy process metric is absent (or vice versa),
	// report the live portion without labeling it as allocated.
	cmCache.CPUIsEstimated = cacheCPU1Est && cacheCPU2Est
	cacheMem1, cacheRedisMemPresent := c.queryContainerMemoryPrimaryOnly(ctx, ns, "redis.*", "redis")
	cacheMem1Est := !cacheRedisMemPresent
	cacheMem2, cacheProxyMemPresent, _ := c.queryScalar(ctx, fmt.Sprintf(
		`sum(process_resident_memory_bytes{namespace=%q,pod=~%q})`,
		ns, "cache-proxy.*",
	))
	cacheMem2Est := !cacheProxyMemPresent
	cmCache.MemoryBytes = int64(cacheMem1 + cacheMem2)
	cmCache.MemIsEstimated = cacheMem1Est && cacheMem2Est
	cmCache.CPULimit = c.kubeLimitCPU(ctx, ns, "redis.*", "redis") + c.kubeLimitCPU(ctx, ns, "cache-proxy.*", "cache-proxy")
	cmCache.MemoryLimit = c.kubeLimitMem(ctx, ns, "redis.*", "redis") + c.kubeLimitMem(ctx, ns, "cache-proxy.*", "cache-proxy")
	// UNDERSTAND: assign cache to dm.Components after app metrics so hit/miss counters can be mirrored.

	// App-server: HTTP + cache counters from url-shortener /metrics
	cmApp := ComponentMetrics{Component: "app-server"}
	appCPU, appCPUEst := c.queryProcessCPU(ctx, ns, "app-server.*", "app-server")
	appMem, appMemEst := c.queryProcessMemory(ctx, ns, "app-server.*", "app-server")
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

	rpsByPod, p99ByPod, cpuByPod, errByPod := c.appServerReplicaSeries(ctx, ns)
	podNames, readyByPod, backendReplicas := c.listAppServerPods(ctx, ns)
	replicaRows := mergePodMetricMaps(podNames, readyByPod, rpsByPod, p99ByPod, cpuByPod, errByPod)
	if len(replicaRows) > 0 {
		cmApp.Replicas = replicaRows
	}

	dm.Components["app-server"] = cmApp

	// Copy cache metrics from app-server onto the cache component so the CachePanel can display them.
	// UNDERSTAND: cache_hits_total and cache_misses_total are emitted by the app-server (not Redis).
	// We mirror them onto the cache component so the frontend doesn't need to cross-reference.
	cmCache.CacheHits = cmApp.CacheHits
	cmCache.CacheMisses = cmApp.CacheMisses
	cmCache.CacheHitsLast9m = cmApp.CacheHitsLast9m
	cmCache.CacheMissesLast9m = cmApp.CacheMissesLast9m
	redisCmdPattern := redisCommandPattern(template)
	redisOps, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(redis_commands_total{namespace=%q,cmd=~%q}[30s]))`,
		ns, redisCmdPattern,
	))
	cmCache.RedisOpsPerSec = redisOps
	if redisOps > 0 {
		cmCache.RequestRate = redisOps
	} else {
		cmCache.RequestRate = cmApp.CacheHits + cmApp.CacheMisses
	}
	c.debugCompare("cache", "rps", redisOps, cmApp.CacheHits+cmApp.CacheMisses, "redis_commands_total", "app cache_hits+misses")

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
	rejected, _ := c.query(ctx, fmt.Sprintf(`sum(rate(redis_rejected_connections_total{namespace=%q}[30s]))`, ns))
	cmCache.RedisRejectedRate = rejected
	// Redis latency percentiles from histogram buckets (microseconds -> seconds).
	redisLatCmdPattern := redisLatencyCommandPattern(template)
	redisLatBucket := fmt.Sprintf(
		`sum(rate(redis_commands_latencies_usec_bucket{namespace=%q,cmd=~%q}[30s])) by (le)`,
		ns, redisLatCmdPattern,
	)
	redisP50usec, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.50, %s)`, redisLatBucket))
	redisP95usec, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.95, %s)`, redisLatBucket))
	redisP99usec, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.99, %s)`, redisLatBucket))
	if redisP99usec > 0 && !math.IsNaN(redisP99usec) && !math.IsInf(redisP99usec, 0) {
		cmCache.P50Latency = redisP50usec / 1_000_000
		cmCache.P95Latency = redisP95usec / 1_000_000
		cmCache.P99Latency = redisP99usec / 1_000_000
	}

	// Average latency fallback from duration/count.
	redisCmdDur, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(redis_commands_duration_seconds_total{namespace=%q,cmd=~%q}[30s]))`,
		ns, redisLatCmdPattern,
	))
	redisCmdCount, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(redis_commands_total{namespace=%q,cmd=~%q}[30s]))`,
		ns, redisLatCmdPattern,
	))
	if redisCmdCount > 0 {
		cmCache.AvgLatency = redisCmdDur / redisCmdCount
	}
	c.debugCompare("cache", "p99", cmCache.P99Latency, cmCache.AvgLatency, "histogram_quantile(redis_latencies_usec_bucket)", "duration/count avg")

	if c.prometheusURL != "" {
		if mc, _ := c.query(ctx, fmt.Sprintf(`max(redis_config_maxclients{namespace=%q})`, ns)); mc > 0 {
			cmCache.RedisMaxClients = mc
		} else if mc2, _ := c.query(ctx, fmt.Sprintf(`max(redis_connected_clients_limit{namespace=%q})`, ns)); mc2 > 0 {
			cmCache.RedisMaxClients = mc2
		}
	}

	annotateCacheForTemplate(&cmCache, template)
	if topo.CacheStrategy != "" {
		cmCache.CacheStrategy = topo.CacheStrategy
	}
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
	cmMQ.QueueDepth, cmMQ.ProduceRate, cmMQ.ConsumeRate, cmMQ.MessagesReady, cmMQ.MessagesUnacked, cmMQ.MQMemoryUsed, cmMQ.MQMemoryLimit =
		c.scrapeRabbitMQPrometheus(ctx, ns)
	// Override estimated memory with live scrape data from rabbitmq_prometheus plugin.
	if cmMQ.MQMemoryUsed > 0 {
		cmMQ.MemoryBytes = int64(cmMQ.MQMemoryUsed)
		cmMQ.MemIsEstimated = false
		if cmMQ.MQMemoryLimit > 0 {
			cmMQ.MemoryLimit = int64(cmMQ.MQMemoryLimit)
		}
	}
	mqPubRate, _ := c.query(ctx, fmt.Sprintf(`sum(rate({__name__=~".*_mq_publish_total",namespace=%q}[30s]))`, ns))
	if mqPubRate > 0 {
		cmMQ.ProduceRate = mqPubRate
	}

	cmWorker := ComponentMetrics{Component: "worker"}
	wCPU, wCPUEst := c.queryProcessCPU(ctx, ns, "worker.*", "worker")
	wMem, wMemEst := c.queryProcessMemory(ctx, ns, "worker.*", "worker")
	cmWorker.CPUUsage = wCPU
	cmWorker.CPUIsEstimated = wCPUEst
	cmWorker.MemoryBytes = int64(wMem)
	cmWorker.MemIsEstimated = wMemEst
	cmWorker.CPULimit = c.kubeLimitCPU(ctx, ns, "worker.*", "worker")
	cmWorker.MemoryLimit = c.kubeLimitMem(ctx, ns, "worker.*", "worker")
	workerRPS, _ := c.query(ctx, fmt.Sprintf(`sum(rate({__name__=~".*_worker_processed_total",namespace=%q}[30s]))`, ns))
	if workerRPS > 0 {
		cmWorker.RequestRate = workerRPS
	} else {
		cmWorker.RequestRate = cmMQ.ConsumeRate
	}
	workerP99, _ := c.query(ctx, fmt.Sprintf(
		`histogram_quantile(0.99, sum(rate({__name__=~".*_worker_processing_duration_seconds_bucket",namespace=%q}[30s])) by (le))`,
		ns,
	))
	if workerP99 > 0 && !math.IsNaN(workerP99) && !math.IsInf(workerP99, 0) {
		cmWorker.P99Latency = workerP99
	}
	workerErrors, _ := c.query(ctx, fmt.Sprintf(`sum(rate({__name__=~".*_worker_errors_total",namespace=%q}[30s]))`, ns))
	cmWorker.ErrorRate = workerErrors
	cmWorker.QueueDepth = cmMQ.QueueDepth
	if cmMQ.QueueDepth > 0 {
		sat := cmMQ.QueueDepth / 100.0
		if sat > 1.0 {
			sat = 1.0
		}
		cmWorker.WorkerSaturation = sat
	}
	dm.Components["worker"] = cmWorker
	if workerRPS > 0 {
		cmMQ.ConsumeRate = workerRPS
	}
	if cmMQ.ConsumeRate > 0 {
		cmMQ.MQSaturationRatio = cmMQ.ProduceRate / cmMQ.ConsumeRate
	} else if cmMQ.ProduceRate > 0 {
		cmMQ.MQSaturationRatio = 10
	}
	dm.Components["mq"] = cmMQ

	cmLB := ComponentMetrics{Component: "lb"}
	lbCPU, lbCPUEst := c.queryCPUPair(ctx, ns, "lb.*", "nginx")
	lbMem, lbMemEst := c.queryMemPair(ctx, ns, "lb.*", "nginx")
	cmLB.CPUUsage = lbCPU
	cmLB.CPUIsEstimated = lbCPUEst
	cmLB.MemoryBytes = int64(lbMem)
	cmLB.MemIsEstimated = lbMemEst
	cmLB.CPULimit = c.kubeLimitCPU(ctx, ns, "lb.*", "nginx")
	cmLB.MemoryLimit = c.kubeLimitMem(ctx, ns, "lb.*", "nginx")
	cmLB.ActiveConnections = c.scrapeNginxStubActiveConnections(ctx, ns)
	cmLB.LBConnectionLimit = nginxWorkerConnections
	algo, backends := c.lbMetadataFromCluster(ctx, ns)
	cmLB.LBAlgorithm = algo
	cmLB.LBBackends = backends
	if c.prometheusURL != "" {
		cmLB.PerBackendRPS = c.queryMultiResult(ctx,
			fmt.Sprintf(`sum by (pod) (rate(http_requests_total{namespace=%q,pod=~"app-server.*"}[30s]))`, ns),
			"pod",
		)
	}
	if len(cmLB.PerBackendRPS) > 0 {
		var totalBackendRPS float64
		for _, rps := range cmLB.PerBackendRPS {
			totalBackendRPS += rps
		}
		cmLB.RequestRate = totalBackendRPS
	} else {
		cmLB.RequestRate = cmApp.RequestRate
	}
	if len(replicaRows) > 0 {
		br := backendReplicas
		cmLB.BackendReplicas = &br
		cmLB.BackendPods = replicaRows
	}
	c.debugCompare("lb", "rps", cmLB.RequestRate, cmApp.RequestRate, "sum(perBackendRPS)", "mirrored app requestRate")
	dm.Components["lb"] = cmLB

	applyStorageProfiles(dm, topo)
	if topo.HasDB {
		if cm, ok := dm.Components["db"]; ok {
			engine := StorageEngineID(topo.DBEngine)
			if engine == "" {
				engine = StorageEnginePostgreSQL
			}
			live := c.collectStorageIO(ctx, ns, nsSel, engine)
			cm.StorageIO = mergeStorageIO(cm.StorageIO, live)
			finalizeStorageIO(cm.StorageIO, engine, topo, template)
			if cm.StorageIO != nil {
				c.debugCompare("db", "storage_read_rows", cm.StorageIO.ReadRate, cm.DbQueryRate,
					"pg_stat_user_tables seq+idx read", "app db_query_duration_count")
				c.debugCompare("db", "storage_write_rows", cm.StorageIO.WriteRate, 0,
					"pg_stat_user_tables ins+upd+del", "none")
			}
			dm.Components["db"] = cm
		}
	}
	if topo.HasCache {
		if cm, ok := dm.Components["cache"]; ok {
			engine := StorageEngineID(topo.CacheEngine)
			if engine == "" {
				engine = StorageEngineRedis
			}
			live := c.collectStorageIO(ctx, ns, nsSel, engine)
			cm.StorageIO = mergeStorageIO(cm.StorageIO, live)
			finalizeStorageIO(cm.StorageIO, engine, topo, template)
			dm.Components["cache"] = cm
		}
	}

	dm.TemplateMetrics = c.collectTemplateMetrics(ctx, nsSel, template)
	c.augmentTemplateMetrics(dm, template)

	// Attach load generator stats to the metrics snapshot.
	// UNDERSTAND: load stats are embedded in the existing metrics stream
	// so the frontend gets everything on one WebSocket connection.
	if c.loadgen != nil {
		stats := c.loadgen.GetStats(namespace)
		dm.LoadGen = &stats
	}

	var ev []Event
	if c.eventBus != nil {
		ev = append(ev, c.eventBus.Drain(namespace)...)
	}
	tmpl := templates.GetTemplate(template)
	if c.thresholds != nil {
		ev = append(ev, c.thresholds.Eval(namespace, dm, topo, tmpl)...)
	}
	if c.cascade != nil {
		ev = append(ev, c.cascade.Eval(namespace, dm, topo, tmpl)...)
	}
	dm.Events = ev

	c.historyMu.Lock()
	if c.history == nil {
		c.history = make(map[string]*RingBuffer)
	}
	rb := c.history[namespace]
	if rb == nil {
		rb = &RingBuffer{}
		c.history[namespace] = rb
	}
	rb.Add(*dm)
	c.historyMu.Unlock()

	return dm, nil
}
