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

	"github.com/ywacch/cloud9/pkg/chaos"
	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/kubeobs"
	"github.com/ywacch/cloud9/pkg/loadgen"
	"github.com/ywacch/cloud9/pkg/probes"
	"github.com/ywacch/cloud9/pkg/templates"
	"golang.org/x/sync/singleflight"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
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
  - DB/MQ/LB (sidecar or non-Go workloads): container cAdvisor → Metrics API (metrics.k8s.io)
    → kube_pod_container_resource_requests (static requests). Never use process_* for postgres
    pods — that measures the exporter sidecar, not the database container.
  - Cache: redis_exporter redis_cpu_* when present; cache-proxy process_* for Go sidecar.
*/

// MetricsCollector queries the Prometheus HTTP API for a specific deployment namespace.
// We query Prometheus via its HTTP API, not a Go client library.
// The API is simple: GET /api/v1/query?query=<promql>&time=<unix>
// Response is JSON that we unmarshal into a typed struct.
type MetricsCollector struct {
	prometheusURL           string
	httpClient              *http.Client
	scrapeClient            *http.Client // short timeout for in-cluster LB / RabbitMQ HTTP scrapes
	loadgen                 loadgen.LoadGenerator
	clientset               kubernetes.Interface // optional: LB metadata from ConfigMap
	metricsClient           PodMetricsProvider   // optional: metrics.k8s.io for container CPU/mem on Autopilot
	debugMetrics            bool
	eventBus                *EventBus
	thresholds              *ThresholdDetector
	cascade                 *CascadeDetector
	invariants              *InvariantChecker
	observer                consistencyObserver
	consistency             *ConsistencyDetector
	historyMu               sync.RWMutex
	history                 map[string]*RingBuffer // namespace → last 300 snapshots
	clientLatencyMu         sync.Mutex
	clientLatency           map[string]*clientLatencyAccumulator
	traceSampleMu           sync.Mutex
	traceSamples            map[string]*traceSampleAccumulator
	traceCollectTick        map[string]int
	mqRateMu                sync.Mutex
	mqRates                 map[string]rabbitCounterSnapshot
	podHealthMu             sync.Mutex
	podHealth               map[string]*podHealthTracker
	sessionAcc              *sessionAccumulator
	pgHoldMu                sync.Mutex
	pgHold                  map[string]bool
	publishersMu            sync.Mutex
	publishers              map[string]*sessionPublisher
	runInfoMu               sync.RWMutex
	runInfo                 map[string]RunInfo
	scaleDesiredMu          sync.Mutex
	scaleDesired            map[string]map[string]int32 // namespace → component → last HPA desired
	scaleLagMu              sync.Mutex
	scaleLag                map[string]scaleLagState
	publisherCollectHook    func(ctx context.Context, namespace, template string, topo TopologyFlags) (*DeploymentMetrics, error) // tests only
	replSQLMu               sync.Mutex
	replSQLCache            map[string]replSQLCacheEntry // key = namespace
	replSQLFlight           singleflight.Group
	replicationAdminForTest func(namespace, pod, container string) deployer.PostgresAdmin // tests only

	clusterSampleEnabled bool
	sampleMu             sync.RWMutex
	clusterSample        *clusterSample
	sampleFlight         singleflight.Group
	metricsQueryObserver MetricsQueryObserver
	promQueryObserver    PromQueryObserver
	collectObserver      CollectObserver
	frameBytesObserver   FrameBytesObserver
}

// NewMetricsCollector wires Prometheus scraping and optional event/threshold pipelines.
// metricsClient may be nil; DB/MQ/LB container usage then falls back to kube requests when cAdvisor is empty.
func NewMetricsCollector(prometheusURL string, lg loadgen.LoadGenerator, clientset kubernetes.Interface, metricsClient PodMetricsProvider, eventBus *EventBus) *MetricsCollector {
	observer := probes.NewObserver(probes.WithKubeClient(clientset))
	return &MetricsCollector{
		prometheusURL:        prometheusURL,
		httpClient:           &http.Client{Timeout: 5 * time.Second},
		scrapeClient:         &http.Client{Timeout: 2 * time.Second},
		loadgen:              lg,
		clientset:            clientset,
		metricsClient:        metricsClient,
		eventBus:             eventBus,
		thresholds:           NewThresholdDetector(),
		cascade:              NewCascadeDetector(),
		invariants:           NewInvariantChecker(),
		observer:             observer,
		consistency:          NewConsistencyDetector(observer),
		mqRates:              make(map[string]rabbitCounterSnapshot),
		sessionAcc:           newSessionAccumulator(),
		pgHold:               make(map[string]bool),
		clusterSampleEnabled: false,
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

// RingBytes returns the sum of JSON-marshaled sizes of every snapshot in every session ring.
func (c *MetricsCollector) RingBytes() int {
	if c == nil {
		return 0
	}
	c.historyMu.Lock()
	defer c.historyMu.Unlock()
	if len(c.history) == 0 {
		return 0
	}
	var total int
	for _, rb := range c.history {
		for _, snap := range rb.All() {
			b, err := json.Marshal(snap)
			if err != nil {
				continue
			}
			total += len(b)
		}
	}
	return total
}

// Cleanup removes per-namespace event, threshold, cascade, and invariant state (call when a deployment ends).
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
	if c.invariants != nil {
		c.invariants.Cleanup(namespace)
	}
	if c.consistency != nil {
		c.consistency.Cleanup(namespace)
	}
	if c.observer != nil {
		c.observer.Stop(namespace)
	}
	c.StopPublisher(namespace)
	c.historyMu.Lock()
	if c.history != nil {
		delete(c.history, namespace)
	}
	c.historyMu.Unlock()
	c.clientLatencyMu.Lock()
	if c.clientLatency != nil {
		delete(c.clientLatency, namespace)
	}
	c.clientLatencyMu.Unlock()
	c.mqRateMu.Lock()
	delete(c.mqRates, namespace)
	c.mqRateMu.Unlock()
	c.podHealthMu.Lock()
	if c.podHealth != nil {
		delete(c.podHealth, namespace)
	}
	c.podHealthMu.Unlock()
	if c.sessionAcc != nil {
		c.sessionAcc.Cleanup(namespace)
	}
	c.scaleDesiredMu.Lock()
	if c.scaleDesired != nil {
		delete(c.scaleDesired, namespace)
	}
	c.scaleDesiredMu.Unlock()
	c.scaleLagMu.Lock()
	if c.scaleLag != nil {
		delete(c.scaleLag, namespace)
	}
	c.scaleLagMu.Unlock()
	c.clearRunInfo(namespace)
	c.OpenPostgresSession(namespace)
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

// AppendSessionSnapshots records snapshots into the namespace ring buffer.
func (c *MetricsCollector) AppendSessionSnapshots(namespace string, snapshots ...DeploymentMetrics) {
	if c == nil || namespace == "" || len(snapshots) == 0 {
		return
	}
	c.historyMu.Lock()
	if c.history == nil {
		c.history = make(map[string]*RingBuffer)
	}
	rb := c.history[namespace]
	if rb == nil {
		rb = &RingBuffer{}
		c.history[namespace] = rb
	}
	for _, dm := range snapshots {
		rb.Add(dm)
	}
	c.historyMu.Unlock()
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

// SessionSummary merges peak, steady-state, history, events, workload anchors,
// trajectories, and per-component freshness into base for metrics_summary persistence.
// base may be nil; existing keys (e.g. seeded) are preserved. Caller still sets
// terminated_at / duration.
func (c *MetricsCollector) SessionSummary(namespace string, base map[string]any, dm *DeploymentMetrics) map[string]any {
	summary := make(map[string]any, len(base)+9)
	for k, v := range base {
		summary[k] = v
	}
	if c == nil {
		if dm != nil {
			for k, v := range PeakSummary(dm) {
				summary[k] = v
			}
		}
		return appendSessionFreshness(summary, dm)
	}
	for k, v := range c.PeakSummaryForNamespace(namespace, dm) {
		summary[k] = v
	}
	if rb := c.RingBufferFor(namespace); rb != nil && rb.Len() > 0 {
		summary["steady_state"] = SteadyStateSummary(rb)
	}
	if hist := c.DownsampledSessionHistory(namespace); len(hist) > 0 {
		summary["session_history"] = hist
		summary["events_summary"] = BuildEventsSummary(hist, 10)
		if we := BuildWorkloadEventsSummary(hist, 20); len(we) > 0 {
			summary["workload_events"] = we
		}
		if traj := BuildTrajectories(hist); len(traj) > 0 {
			summary["trajectories"] = traj
		}
	}
	if clh := c.ClientLatencyHistoryFor(namespace); len(clh) > 0 {
		summary["client_latency_history"] = clh
	}
	if ts := c.TraceSamplesFor(namespace); len(ts) > 0 {
		summary["trace_samples"] = ts
	}
	if dm != nil && len(dm.Phenomena) > 0 {
		summary["phenomena"] = dm.Phenomena
	}
	return appendSessionFreshness(summary, dm)
}

func appendSessionFreshness(summary map[string]any, dm *DeploymentMetrics) map[string]any {
	if dm != nil {
		annotateFreshness(dm)
		if fresh := sessionFreshnessFromComponents(dm.Components); len(fresh) > 0 {
			summary["freshness"] = fresh
		}
	}
	return summary
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
	if c.clusterSampleEnabled && c.prometheusURL != "" {
		sample := c.ensureClusterSample(ctx)
		if sample != nil {
			if v, okPresent, ok := c.lookupSampleScalar(sample, ctx, promql); ok {
				return v, okPresent, nil
			}
			if time.Since(sample.fetchedAt) < clusterSampleTick {
				return 0, false, nil
			}
		}
	}
	return c.queryScalarDirect(ctx, promql)
}

func (c *MetricsCollector) queryScalarDirect(ctx context.Context, promql string) (val float64, present bool, err error) {
	if c.prometheusURL == "" {
		return 0, false, nil
	}
	pr, err := c.executePromInstant(ctx, promql)
	if err != nil {
		return 0, false, err
	}
	if pr.Status != "success" || len(pr.Data.Result) == 0 {
		return 0, false, nil
	}
	valStr, ok := pr.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, true, nil
	}
	_, _ = fmt.Sscanf(valStr, "%f", &val)
	if math.IsNaN(val) || math.IsInf(val, 0) {
		val = 0
	}
	return val, true, nil
}

func (c *MetricsCollector) executePromInstant(ctx context.Context, promql string) (prometheusResponse, error) {
	var pr prometheusResponse
	if c.prometheusURL == "" {
		return pr, nil
	}
	endpoint := fmt.Sprintf("%s/api/v1/query", c.prometheusURL)
	params := url.Values{"query": {promql}}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+params.Encode(), nil)
	if err != nil {
		return pr, err
	}

	c.notifyPromQuery(ctx)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return pr, fmt.Errorf("prometheus query: %w", err)
	}
	defer resp.Body.Close()

	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return pr, fmt.Errorf("decode prometheus response: %w", err)
	}
	return pr, nil
}

func (c *MetricsCollector) notifyPromQuery(ctx context.Context) {
	if c == nil || c.promQueryObserver == nil {
		return
	}
	meta := promMetaFrom(ctx)
	if meta.path == "" {
		return
	}
	template := meta.template
	if template == "" {
		template = "none"
	}
	caller := meta.caller
	if caller == "" {
		caller = "collect"
	}
	c.promQueryObserver(meta.path, caller, template)
}

// query executes a PromQL instant query and returns the first result value.
// Returns 0 and no error if the query returns no results (metric not yet available).
func (c *MetricsCollector) query(ctx context.Context, promql string) (float64, error) {
	v, _, err := c.queryScalar(ctx, promql)
	return v, err
}

// queryMultiResult runs a PromQL instant query and returns one float per series, keyed by labelKey (e.g. "pod").
func (c *MetricsCollector) queryMultiResult(ctx context.Context, promql, labelKey string) map[string]float64 {
	if c.clusterSampleEnabled && c.prometheusURL != "" {
		sample := c.ensureClusterSample(ctx)
		if sample != nil {
			if out, ok := c.lookupSampleMulti(sample, ctx, promql, labelKey); ok {
				return out
			}
			if time.Since(sample.fetchedAt) < clusterSampleTick {
				return map[string]float64{}
			}
		}
	}
	out := make(map[string]float64)
	if c.prometheusURL == "" || labelKey == "" {
		return out
	}
	pr, err := c.executePromInstant(ctx, promql)
	if err != nil {
		return out
	}
	if pr.Status != "success" {
		return out
	}
	for i := range pr.Data.Result {
		r := &pr.Data.Result[i]
		key := r.Metric[labelKey]
		if key == "" && labelKey != "target" {
			continue
		}
		out[key] = parsePromValue(r.Value[1])
	}
	return out
}

// queryMultiLabel runs an instant query and returns valueLabel per series keyed by keyLabel.
func (c *MetricsCollector) queryMultiLabel(ctx context.Context, promql, keyLabel, valueLabel string) map[string]string {
	out := make(map[string]string)
	if c.prometheusURL == "" || keyLabel == "" || valueLabel == "" {
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
		key := r.Metric[keyLabel]
		val := r.Metric[valueLabel]
		if key == "" || val == "" {
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
		`sum(rate(container_cpu_usage_seconds_total{namespace=%q,pod=~%q,container="%s"}%s))`,
		ns, podRe, mainContainer, promRateRange(),
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
	return c.kubeLimitCPUExcluding(ctx, ns, podRe, "", container)
}

func (c *MetricsCollector) kubeLimitCPUExcluding(ctx context.Context, ns, podRe, excludePodRe, container string) float64 {
	podSel := promPodLabelSelectors(podRe, excludePodRe)
	v, _ := c.query(ctx, fmt.Sprintf(
		`sum(kube_pod_container_resource_limits{namespace=%q,%s,container="%s",resource="cpu"})`,
		ns, podSel, container,
	))
	return v
}

func (c *MetricsCollector) kubeLimitMem(ctx context.Context, ns, podRe, container string) int64 {
	return c.kubeLimitMemExcluding(ctx, ns, podRe, "", container)
}

func (c *MetricsCollector) kubeLimitMemExcluding(ctx context.Context, ns, podRe, excludePodRe, container string) int64 {
	podSel := promPodLabelSelectors(podRe, excludePodRe)
	v, _ := c.query(ctx, fmt.Sprintf(
		`sum(kube_pod_container_resource_limits{namespace=%q,%s,container="%s",resource="memory"})`,
		ns, podSel, container,
	))
	return int64(v)
}

// queryProcessCPU uses process_cpu_seconds_total from the app's /metrics (works on GKE Autopilot
// where cAdvisor series are often unavailable to self-managed Prometheus).
func (c *MetricsCollector) queryProcessCPU(ctx context.Context, ns, podRe, fallbackContainer string) (float64, bool) {
	primary := fmt.Sprintf(
		`sum(rate(process_cpu_seconds_total{namespace=%q,pod=~%q}%s))`,
		ns, podRe, promRateRange(),
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
		`sum(rate(container_cpu_usage_seconds_total{namespace=%q,pod=~%q,container="%s"}%s))`,
		ns, podRe, container, promRateRange(),
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

// nginxWorkerConnections matches worker_connections in pkg/deployer/lb.go DefaultLBSpec.
const nginxWorkerConnections = 4096

func redisCommandPattern(template string) string {
	switch template {
	case "social-feed":
		return "^(zadd|zrevrange|zremrangebyrank)$"
	default:
		return "^(get|set|del|mget|mset|scan)$"
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

// redisStorageReadCmdPattern selects read-side redis_exporter cmd labels for storageIo.readRate.
func redisStorageReadCmdPattern(template string) string {
	switch template {
	case "social-feed":
		return "zrevrange"
	default:
		return "get|mget|hget|hmget"
	}
}

// redisStorageWriteCmdPattern selects write-side redis_exporter cmd labels for storageIo.writeRate.
func redisStorageWriteCmdPattern(template string) string {
	switch template {
	case "social-feed":
		return "zadd|zremrangebyrank"
	default:
		return "set|del|mset|hset|hdel"
	}
}

func redisStorageReadLatencyCmdPattern(template string) string {
	switch template {
	case "social-feed":
		return "zrevrange"
	default:
		return "get|mget"
	}
}

func redisStorageWriteLatencyCmdPattern(template string) string {
	switch template {
	case "social-feed":
		return "zadd|zremrangebyrank"
	default:
		return "set|del"
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

func (c *MetricsCollector) noteActiveChaosPartition(ctx context.Context, namespace string) {
	if c == nil || c.consistency == nil {
		return
	}
	if c.clientset == nil {
		c.consistency.NoteActivePartition(namespace, nil)
		return
	}
	partitions, err := chaos.ListActivePartitions(ctx, c.clientset, namespace)
	if err != nil || len(partitions) == 0 {
		c.consistency.NoteActivePartition(namespace, nil)
		return
	}
	part := partitions[0]
	c.consistency.NoteActivePartition(namespace, &PartitionContext{
		Scenario:        part.Scenario,
		TargetComponent: part.TargetComponent,
	})
}

// Collect queries all metrics for a deployment namespace and returns a snapshot.
// template is the deployment template id (e.g. social-feed); empty skips template-specific PromQL.
// topo is derived from the saved deployment design (may be zero); used for narratives and cache-strategy overlay.
func (c *MetricsCollector) Collect(ctx context.Context, namespace, template string, topo TopologyFlags) (*DeploymentMetrics, error) {
	ctx = withCollectNamespace(ctx, namespace)
	ctx = WithPromMeta(ctx, template, "collect", "direct")
	ctx = kubeobs.WithCaller(ctx, "metrics")
	c.ensureClusterSample(ctx)

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

	var sessionHPAs []autoscalingv2.HorizontalPodAutoscaler
	sessionHPAsOK := false
	if c.clientset != nil {
		if hpaList, err := c.clientset.AutoscalingV2().HorizontalPodAutoscalers(ns).List(ctx, metav1.ListOptions{}); err == nil {
			sessionHPAs = hpaList.Items
			sessionHPAsOK = true
		}
	}

	// DB (postgres) — CPU/memory: container cAdvisor → Metrics API → kube requests.
	// Include postgres-replica containers so a promoted writer still contributes
	// after FencePrimary scales the postgres Deployment to zero.
	cmDB := ComponentMetrics{Component: "db"}
	dbCPURes := c.queryPostgresClusterCPU(ctx, ns)
	dbMemRes := c.queryPostgresClusterMemory(ctx, ns)
	cmDB.CPUUsage = dbCPURes.Usage
	cmDB.CPUIsEstimated = dbCPURes.Estimated
	cmDB.CPUSource = dbCPURes.Source
	cmDB.MemoryBytes = int64(dbMemRes.Usage)
	cmDB.MemIsEstimated = dbMemRes.Estimated
	cmDB.MemSource = dbMemRes.Source
	cmDB.CPULimit = c.kubeLimitPostgresClusterCPU(ctx, ns)
	cmDB.MemoryLimit = c.kubeLimitPostgresClusterMem(ctx, ns)

	dbQPS, dbQPSPresent, _ := c.queryScalar(ctx, fmt.Sprintf(
		`sum(rate(db_query_duration_seconds_count{%s}%s))`,
		appServerSel, promRateRange(),
	))
	if dbQPSPresent {
		cmDB.DbQueryRate = dbQPS
		cmDB.DbQuerySource = DbQuerySourceApp
	} else {
		cmDB.DbQueryRate = 0
		cmDB.DbQuerySource = DbQuerySourceNone
	}
	dbLatSum, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(db_query_duration_seconds_sum{%s}%s))`,
		appServerSel, promRateRange(),
	))
	dbLatCount, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(db_query_duration_seconds_count{%s}%s))`,
		appServerSel, promRateRange(),
	))
	if dbLatCount > 0 {
		cmDB.DbQueryLatency = dbLatSum / dbLatCount
	}
	cmDB.AvgLatency = cmDB.DbQueryLatency
	// DbTotalQueries is filled after app-server by-pod SQL counters are scraped.

	cmDB.DbActiveConnections = c.postgresClusterBackends(ctx, ns)

	maxConn, _ := c.query(ctx, fmt.Sprintf(
		`max(pg_settings{namespace=%q,name="max_connections"})`,
		ns,
	))
	cmDB.MaxConnections = maxConn
	// PostgreSQL-native throughput from postgres_exporter; fallback to app-side DB query rate.
	pgTxnRate, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(pg_stat_database_xact_commit{namespace=%q,datname!=""}%s))`,
		ns, promRateRange(),
	))
	pgRollbackRate, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(pg_stat_database_xact_rollback{namespace=%q,datname!=""}%s))`,
		ns, promRateRange(),
	))
	if pgTxnRate+pgRollbackRate > 0 {
		cmDB.RequestRate = pgTxnRate + pgRollbackRate
	} else if dbQPSPresent {
		cmDB.RequestRate = dbQPS
	}
	// DbQueryRate is app SQL throughput only (dbQuerySource=app). Do not backfill from
	// xact_commit — that includes postgres_exporter sidecar traffic (~5/s at idle).
	c.debugCompare("db", "rps", pgTxnRate+pgRollbackRate, dbQPS, "pg_stat_database_xact_commit+rollback", "app db_query_duration_count")
	c.debugCompare("db", "cpu", dbCPURes.Usage, 0, dbCPURes.Source, "")

	if templateHasTimelineJob(templates.GetTemplate(template)) {
		c.collectPostgresTimelineTableStats(ctx, ns, &cmDB)
	}

	dm.Components["db"] = cmDB

	// Cache tier: redis + cache-proxy (separate queries so exporter sidecars are not summed).
	cmCache := ComponentMetrics{Component: "cache"}
	redisCPUUser, redisUserPresent, _ := c.queryScalar(ctx, fmt.Sprintf(
		`sum(rate(redis_cpu_user_seconds_total{namespace=%q}%s))`,
		ns, promRateRange(),
	))
	redisCPUSys, redisSysPresent, _ := c.queryScalar(ctx, fmt.Sprintf(
		`sum(rate(redis_cpu_sys_seconds_total{namespace=%q}%s))`,
		ns, promRateRange(),
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
		`sum(rate(process_cpu_seconds_total{namespace=%q,pod=~%q}%s))`,
		ns, "cache-proxy.*", promRateRange(),
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
		`sum(rate(http_requests_total{%s}%s))`,
		nsSel, promRateRange(),
	))
	cmApp.RequestRate = rps

	// UNDERSTAND: histogram_quantile needs the _bucket metric, not _total.
	latBucketRate := fmt.Sprintf(`sum(rate(http_request_duration_seconds_bucket{%s}%s)) by (le)`, nsSel, promRateRange())
	p50, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.50, %s)`, latBucketRate))
	p95, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.95, %s)`, latBucketRate))
	p99, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.99, %s)`, latBucketRate))
	cmApp.P50Latency = p50
	cmApp.P95Latency = p95
	cmApp.P99Latency = p99

	hits, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(cache_hits_total{%s}%s))`,
		nsSel, promRateRange(),
	))
	cmApp.CacheHits = hits

	misses, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(cache_misses_total{%s}%s))`,
		nsSel, promRateRange(),
	))
	cmApp.CacheMisses = misses

	cacheErrs, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(cache_errors_total{%s}%s))`,
		nsSel, promRateRange(),
	))
	cmApp.CacheErrors = cacheErrs

	errRate, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(http_requests_total{%s,status=~"5.."}%s))`,
		nsSel, promRateRange(),
	))
	cmApp.ErrorRate = errRate

	// Session p50/p95/p99: rate() over elapsed deployment time (clamped 1m–30m), not a fixed 9m window.
	// After an app-server pod kill, the quantile reflects only pods still live in Prometheus.
	sessionP99Range := sessionP99PromRange(c.sessionAcc.Elapsed(ns, time.Now()))
	latBucketRate9m := fmt.Sprintf(`sum(rate(http_request_duration_seconds_bucket{%s}%s)) by (le)`, nsSel, sessionP99Range)
	p50_9m, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.50, %s)`, latBucketRate9m))
	p95_9m, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.95, %s)`, latBucketRate9m))
	p99_9m, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.99, %s)`, latBucketRate9m))
	cmApp.P50Latency9m = p50_9m
	cmApp.P95Latency9m = p95_9m
	cmApp.P99Latency9m = p99_9m

	cmApp.CPULimit = c.kubeLimitCPU(ctx, ns, "app-server.*", "app-server")
	cmApp.MemoryLimit = c.kubeLimitMem(ctx, ns, "app-server.*", "app-server")

	rpsByPod, p50ByPod, p95ByPod, p99ByPod, p50SessionByPod, p95SessionByPod, p99SessionByPod, cpuByPod, errByPod, totalReqsByPod, totalErrsByPod, memByPod := c.appServerReplicaSeries(ctx, ns)
	httpErrByPod := c.queryMultiResult(ctx, fmt.Sprintf(
		`sum by (pod) (http_requests_total{%s,status=~"5.."})`,
		appServerSel,
	), "pod")
	sqlByPod := c.queryMultiResult(ctx, fmt.Sprintf(
		`sum by (pod) (db_query_duration_seconds_count{%s})`,
		appServerSel,
	), "pod")
	hitsByPod := c.queryMultiResult(ctx, fmt.Sprintf(
		`sum by (pod) (cache_hits_total{%s})`,
		appServerSel,
	), "pod")
	missesByPod := c.queryMultiResult(ctx, fmt.Sprintf(
		`sum by (pod) (cache_misses_total{%s})`,
		appServerSel,
	), "pod")
	errorsByPod := c.queryMultiResult(ctx, fmt.Sprintf(
		`sum by (pod) (cache_errors_total{%s})`,
		appServerSel,
	), "pod")
	cmApp.TotalRequests = c.sessionAcc.Observe(ns, seriesHTTP, totalReqsByPod)
	cmApp.TotalErrors9m = c.sessionAcc.Observe(ns, seriesHTTP5xx, httpErrByPod)
	cmApp.CacheHitsLast9m = c.sessionAcc.Observe(ns, seriesCacheHits, hitsByPod)
	cmApp.CacheMissesLast9m = c.sessionAcc.Observe(ns, seriesCacheMisses, missesByPod)
	cmApp.CacheErrorsLast9m = c.sessionAcc.Observe(ns, seriesCacheErrors, errorsByPod)
	if db, ok := dm.Components["db"]; ok {
		db.DbTotalQueries = c.sessionAcc.Observe(ns, seriesSQL, sqlByPod)
		dm.Components["db"] = db
	}

	podNames, readyByPod, backendReplicas := c.listAppServerPods(ctx, ns)
	replicaRows := mergePodMetricMaps(
		podNames, readyByPod,
		rpsByPod, p50ByPod, p95ByPod, p99ByPod,
		p50SessionByPod, p95SessionByPod, p99SessionByPod,
		cpuByPod, errByPod, totalReqsByPod, totalErrsByPod, memByPod,
	)
	if len(replicaRows) > 0 {
		cmApp.Replicas = replicaRows
	}
	if sessionHPAsOK {
		if st := scaleStatusForApp(sessionHPAs, "app-server", backendReplicas.Ready); st != nil {
			cmApp.Scale = st
		}
	}

	dm.Components["app-server"] = cmApp

	// Copy cache metrics from app-server onto the cache component so the CachePanel can display them.
	// UNDERSTAND: cache_hits_total and cache_misses_total are emitted by the app-server (not Redis).
	// We mirror them onto the cache component so the frontend doesn't need to cross-reference.
	cmCache.CacheHits = cmApp.CacheHits
	cmCache.CacheMisses = cmApp.CacheMisses
	cmCache.CacheErrors = cmApp.CacheErrors
	cmCache.CacheHitsLast9m = cmApp.CacheHitsLast9m
	cmCache.CacheMissesLast9m = cmApp.CacheMissesLast9m
	cmCache.CacheErrorsLast9m = cmApp.CacheErrorsLast9m
	if template == "social-feed" {
		timelineKeys, _ := c.query(ctx, fmt.Sprintf(`max(social_timeline_keys{%s})`, nsSel))
		cmCache.TimelineKeyCount = timelineKeys
		pushFeed, _ := c.query(ctx, fmt.Sprintf(
			`sum(rate(social_feed_request_total{%s,mode="push-redis"}%s))`,
			nsSel, promRateRange(),
		))
		pullFeed, _ := c.query(ctx, fmt.Sprintf(
			`sum(rate(social_feed_request_total{%s,mode="pull"}%s))`,
			nsSel, promRateRange(),
		))
		cmCache.FeedRequestRatePush = pushFeed
		cmCache.FeedRequestRatePull = pullFeed
	}
	redisCmdPattern := redisCommandPattern(template)
	redisCmdRates := c.queryMultiResult(ctx, fmt.Sprintf(
		`sum by (cmd) (rate(redis_commands_total{namespace=%q,cmd=~%q}%s))`,
		ns, redisCmdPattern, promRateRange(),
	), "cmd")
	if len(redisCmdRates) > 0 {
		cmCache.RedisCmdRates = redisCmdRates
	}
	redisOps := RequestPathRedisOpsPerSec(cmCache.RedisCmdRates)
	if redisOps <= 0 {
		redisOps, _ = c.query(ctx, fmt.Sprintf(
			`sum(rate(redis_commands_total{namespace=%q,cmd=~%q}%s))`,
			ns, redisCmdPattern, promRateRange(),
		))
	}
	cmCache.RedisOpsPerSec = redisOps
	if redisOps > 0 {
		cmCache.RequestRate = redisOps
	} else {
		cmCache.RequestRate = cmApp.CacheHits + cmApp.CacheMisses + cmApp.CacheErrors
	}
	c.debugCompare("cache", "rps", redisOps, cmApp.CacheHits+cmApp.CacheMisses+cmApp.CacheErrors, "redis_commands_total", "app cache_hits+misses+errors")

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
	rh, _ := c.query(ctx, fmt.Sprintf(`sum(rate(redis_keyspace_hits_total{namespace=%q}%s))`, ns, promRateRange()))
	rm, _ := c.query(ctx, fmt.Sprintf(`sum(rate(redis_keyspace_misses_total{namespace=%q}%s))`, ns, promRateRange()))
	cmCache.RedisKeyspaceHits = rh
	cmCache.RedisKeyspaceMisses = rm
	rejected, _ := c.query(ctx, fmt.Sprintf(`sum(rate(redis_rejected_connections_total{namespace=%q}%s))`, ns, promRateRange()))
	cmCache.RedisRejectedRate = rejected
	applyCacheStoreTelemetry(&cmCache, c.fillRedisCacheStoreTelemetry(ctx, ns))
	// Redis latency percentiles from histogram buckets (microseconds -> seconds).
	redisLatCmdPattern := redisLatencyCommandPattern(template)
	redisLatBucket := fmt.Sprintf(
		`sum(rate(redis_commands_latencies_usec_bucket{namespace=%q,cmd=~%q}%s)) by (le)`,
		ns, redisLatCmdPattern, promRateRange(),
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
		`sum(rate(redis_commands_duration_seconds_total{namespace=%q,cmd=~%q}%s))`,
		ns, redisLatCmdPattern, promRateRange(),
	))
	redisCmdCount, _ := c.query(ctx, fmt.Sprintf(
		`sum(rate(redis_commands_total{namespace=%q,cmd=~%q}%s))`,
		ns, redisLatCmdPattern, promRateRange(),
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

	if strings.EqualFold(topo.CacheEngine, "memcached") {
		c.applyMemcachedCacheMetrics(ctx, ns, &cmCache)
	}
	annotateCacheForTemplate(&cmCache, template)
	if topo.CacheStrategy != "" {
		cmCache.CacheStrategy = topo.CacheStrategy
	}
	layerSplit := c.collectCacheLayerSplit(ctx, ns)
	applyCacheLayers(&cmCache, layerSplit)
	dm.Components["cache"] = cmCache

	cmMQ := ComponentMetrics{Component: "mq"}
	mqCPURes := c.queryContainerResourceCPU(ctx, ns, "rabbitmq.*", "rabbitmq")
	mqMemRes := c.queryContainerResourceMemory(ctx, ns, "rabbitmq.*", "rabbitmq")
	cmMQ.CPUUsage = mqCPURes.Usage
	cmMQ.CPUIsEstimated = mqCPURes.Estimated
	cmMQ.CPUSource = mqCPURes.Source
	cmMQ.MemoryBytes = int64(mqMemRes.Usage)
	cmMQ.MemIsEstimated = mqMemRes.Estimated
	cmMQ.MemSource = mqMemRes.Source
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
	mqPubRate, _ := c.query(ctx, fmt.Sprintf(`sum(rate({__name__=~".*_mq_publish_total",namespace=%q}%s))`, ns, promRateRange()))
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
	workerRPS, _ := c.query(ctx, fmt.Sprintf(`sum(rate({__name__=~".*_worker_processed_total",namespace=%q}%s))`, ns, promRateRange()))
	if workerRPS > 0 {
		cmWorker.RequestRate = workerRPS
	} else {
		cmWorker.RequestRate = cmMQ.ConsumeRate
	}
	workerLiveBucketRate := fmt.Sprintf(
		`sum(rate({__name__=~".*_worker_processing_duration_seconds_bucket",namespace=%q}%s)) by (le)`,
		ns, promRateRange(),
	)
	workerP50, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.50, %s)`, workerLiveBucketRate))
	workerP95, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.95, %s)`, workerLiveBucketRate))
	workerP99, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.99, %s)`, workerLiveBucketRate))
	cmWorker.P50Latency = workerP50
	cmWorker.P95Latency = workerP95
	cmWorker.P99Latency = workerP99
	workerErrors, _ := c.query(ctx, fmt.Sprintf(`sum(rate({__name__=~".*_worker_errors_total",namespace=%q}%s))`, ns, promRateRange()))
	cmWorker.ErrorRate = workerErrors
	workerSessionRange := sessionP99PromRange(c.sessionAcc.Elapsed(ns, time.Now()))
	workerSessionBucketRate := fmt.Sprintf(
		`sum(rate({__name__=~".*_worker_processing_duration_seconds_bucket",namespace=%q}%s)) by (le)`,
		ns, workerSessionRange,
	)
	wP50Session, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.50, %s)`, workerSessionBucketRate))
	wP95Session, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.95, %s)`, workerSessionBucketRate))
	wP99Session, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.99, %s)`, workerSessionBucketRate))
	cmWorker.P50Latency9m = wP50Session
	cmWorker.P95Latency9m = wP95Session
	cmWorker.P99Latency9m = wP99Session
	cmWorker.QueueDepth = cmMQ.QueueDepth
	if cmMQ.QueueDepth > 0 {
		sat := cmMQ.QueueDepth / 100.0
		if sat > 1.0 {
			sat = 1.0
		}
		cmWorker.WorkerSaturation = sat
	}
	wRpsByPod, wP50ByPod, wP95ByPod, wP99ByPod, wP50SessionByPod, wP95SessionByPod, wP99SessionByPod, wCPUByPod, wErrByPod, wTotalProcessedByPod, wTotalErrorsByPod, wMemByPod := c.workerReplicaSeries(ctx, ns)
	cmWorker.TotalRequests = c.sessionAcc.Observe(ns, seriesWorkerProcessed, wTotalProcessedByPod)
	cmWorker.TotalErrors9m = c.sessionAcc.Observe(ns, seriesWorkerErrors, wTotalErrorsByPod)
	workerPodNames, workerReadyByPod, workerReplicaCounts := c.listWorkerPods(ctx, ns)
	workerReplicaRows := mergePodMetricMaps(
		workerPodNames, workerReadyByPod,
		wRpsByPod, wP50ByPod, wP95ByPod, wP99ByPod,
		wP50SessionByPod, wP95SessionByPod, wP99SessionByPod,
		wCPUByPod, wErrByPod, wTotalProcessedByPod, wTotalErrorsByPod, wMemByPod,
	)
	if len(workerReplicaRows) > 0 {
		cmWorker.Replicas = workerReplicaRows
	}
	if sessionHPAsOK {
		if st := scaleStatusForApp(sessionHPAs, "worker", workerReplicaCounts.Ready); st != nil {
			cmWorker.Scale = st
		}
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
	lbCPURes := c.queryContainerResourceCPU(ctx, ns, "lb.*", "nginx")
	lbMemRes := c.queryContainerResourceMemory(ctx, ns, "lb.*", "nginx")
	cmLB.CPUUsage = lbCPURes.Usage
	cmLB.CPUIsEstimated = lbCPURes.Estimated
	cmLB.CPUSource = lbCPURes.Source
	cmLB.MemoryBytes = int64(lbMemRes.Usage)
	cmLB.MemIsEstimated = lbMemRes.Estimated
	cmLB.MemSource = lbMemRes.Source
	cmLB.CPULimit = c.kubeLimitCPU(ctx, ns, "lb.*", "nginx")
	cmLB.MemoryLimit = c.kubeLimitMem(ctx, ns, "lb.*", "nginx")
	cmLB.ActiveConnections = c.scrapeNginxStubActiveConnections(ctx, ns)
	cmLB.LBConnectionLimit = nginxWorkerConnections
	algo, backends := c.lbMetadataFromCluster(ctx, ns)
	cmLB.LBAlgorithm = algo
	cmLB.LBBackends = backends
	if c.prometheusURL != "" {
		cmLB.PerBackendRPS = c.queryMultiResult(ctx,
			fmt.Sprintf(`sum by (pod) (rate(http_requests_total{namespace=%q,pod=~"app-server.*"}%s))`, ns, promRateRange()),
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
	cmLB.TotalRequests = cmApp.TotalRequests
	cmLB.TotalErrors9m = cmApp.TotalErrors9m
	cmLB.P50Latency9m = cmApp.P50Latency9m
	cmLB.P95Latency9m = cmApp.P95Latency9m
	cmLB.P99Latency9m = cmApp.P99Latency9m
	c.debugCompare("lb", "rps", cmLB.RequestRate, cmApp.RequestRate, "sum(perBackendRPS)", "mirrored app requestRate")
	dm.Components["lb"] = cmLB

	applyStorageProfiles(dm, topo)
	if topo.HasDB {
		if cm, ok := dm.Components["db"]; ok {
			engine := StorageEngineID(topo.DBEngine)
			if engine == "" {
				engine = StorageEnginePostgreSQL
			}
			live := c.collectStorageIO(ctx, ns, nsSel, engine, template)
			cm.StorageIO = mergeStorageIO(cm.StorageIO, live)
			finalizeStorageIO(cm.StorageIO, engine, topo, template)
			if cm.StorageIO != nil {
				c.debugCompare("db", "storage_read_rows", cm.StorageIO.ReadRate, cm.DbQueryRate,
					"pg_stat_user_tables seq+idx read", "app db_query_duration_count")
				c.debugCompare("db", "storage_write_rows", cm.StorageIO.WriteRate, 0,
					"pg_stat_user_tables ins+upd+del", "none")
			}
			dbLayerSplit := c.collectDBReplicaLayers(ctx, ns, cm.StorageIO)
			applyDBReplicaLayers(&cm, dbLayerSplit)
			if c.consistency != nil {
				c.consistency.NoteDBReplicaPrimaryConnectivity(namespace, dbLayerSplit.ready, dbLayerSplit.connectedOnPrimary)
			}
			needReplica := len(dbLayerSplit.replicaPods) > 0
			needCache := topo.HasCache
			if c.observer != nil && (needReplica || needCache) {
				cacheOpts := probes.DefaultCacheDialOptions(topo.HasProxyCache)
				if topo.CacheEngine != "" {
					cacheOpts.Engine = topo.CacheEngine
				}
				c.observer.Start(namespace, probes.ObserverConfig{
					NeedReplica: needReplica,
					NeedCache:   needCache,
					Cache:       cacheOpts,
				})
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
			live := c.collectStorageIO(ctx, ns, nsSel, engine, template)
			cm.StorageIO = mergeStorageIO(cm.StorageIO, live)
			finalizeStorageIO(cm.StorageIO, engine, topo, template)
			dm.Components["cache"] = cm
		}
	}

	dm.TemplateMetrics = c.collectTemplateMetrics(ctx, nsSel, template)
	c.augmentTemplateMetrics(dm, template)
	dm.Timeline = c.collectTimelineOps(ctx, nsSel, template)

	tmpl := templates.GetTemplate(template)
	runtimeEvents := c.attachRuntimeHealth(ctx, namespace, dm)
	if cm, ok := dm.Components["cache"]; ok {
		syncCacheLayerRestarts(&cm)
		dm.Components["cache"] = cm
	}
	if cm, ok := dm.Components["db"]; ok {
		syncDBReplicaLayerRestarts(&cm)
		dm.Components["db"] = cm
	}
	// All components and layers are final here; tag per-signal-group freshness.
	annotateFreshness(dm)

	// Attach load generator stats to the metrics snapshot.
	// UNDERSTAND: load stats are embedded in the existing metrics stream
	// so the frontend gets everything on one WebSocket connection.
	if c.loadgen != nil {
		stats := c.loadgen.GetStats(namespace)
		lagging := appServerScaleLagging(dm.Components["app-server"])
		stats = c.applyScaleLagToLoadStats(namespace, lagging, stats)
		dm.LoadGen = &stats
		if stats.Active && len(stats.RecentIntervals) > 0 {
			c.mergeClientLatencyHistory(namespace, stats.RecentIntervals)
		}
	}

	var ev []Event
	if c.eventBus != nil {
		ev = append(ev, c.eventBus.Drain(namespace)...)
	}
	ev = append(ev, runtimeEvents...)
	if c.thresholds != nil {
		ev = append(ev, c.thresholds.Eval(namespace, dm, topo, tmpl)...)
	}
	if c.cascade != nil {
		ev = append(ev, c.cascade.Eval(namespace, dm, topo, tmpl)...)
	}
	if c.invariants != nil {
		ev = append(ev, c.invariants.Eval(namespace, dm, topo, tmpl)...)
	}
	if c.consistency != nil {
		if topo.HasCache && topo.HasDB {
			staleTotal, _, _ := c.queryScalar(ctx, fmt.Sprintf(`sum(consistency_read_stale_total{%s})`, nsSel))
			cacheSample, replicaSamples := c.scrapeConsistencySamples(ctx, namespace)
			c.consistency.NoteAppCacheStale(namespace, staleTotal, cacheSample, replicaSamples)
		}
		var threadTraffic appTrafficConsistency
		if topo.HasDB {
			c.noteActiveChaosPartition(ctx, namespace)
			threadTraffic = c.scrapeAppTrafficConsistency(ctx, nsSel)
			c.consistency.NoteAppTrafficConsistency(namespace, threadTraffic)
			if topologyRequiresPostgresSyncGUCs(topo) {
				c.consistency.NotePostgresSyncActive(namespace, c.postgresSyncGUCsActive(ctx, ns, topo))
			}
		}
		ev = append(ev, c.consistency.Eval(namespace, dm, topo, tmpl)...)
		appendReplyBeforeParentPhenomenon(dm, template, topo, threadTraffic)
	}
	if sessionHPAsOK {
		nextScale := make(map[string]int32)
		if app, ok := dm.Components["app-server"]; ok && app.Scale != nil {
			nextScale["app-server"] = app.Scale.Desired
		}
		if w, ok := dm.Components["worker"]; ok && w.Scale != nil {
			nextScale["worker"] = w.Scale.Desired
		}
		ev = append(ev, c.appendScaleEvents(ns, nextScale, dm.Timestamp)...)
	}
	dm.Events = ev

	c.maybeScrapeTraceSamples(ctx, namespace)

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

	c.copyRunInfoOnto(dm)

	return dm, nil
}
