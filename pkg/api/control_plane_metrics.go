package api

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var controlPlaneDurationBuckets = []float64{1, 5, 15, 30, 60, 120, 300, 600, 1200}

var k8sRequestDurationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

var collectDurationBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30}

var wsFrameBytesBuckets = []float64{1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288}

// controlPlaneMetrics holds Prometheus collectors for cloud9-api provision timing.
// It uses a private registry so this process does not register app-side metrics.
type controlPlaneMetrics struct {
	registry              *prometheus.Registry
	admitTotal            *prometheus.CounterVec
	deployQueueDepth      prometheus.Gauge
	doorWaitSeconds       prometheus.Histogram
	provisionSeconds      prometheus.Histogram
	orchestrationSeconds  prometheus.Histogram
	teardownLagSeconds    prometheus.Histogram
	websocketLive         prometheus.Gauge
	informerQueueDepth    *prometheus.GaugeVec
	informerEventAge      *prometheus.GaugeVec
	informerEventMu       sync.Mutex
	informerLastEvent     map[string]time.Time
	metricsQueriesTotal   *prometheus.CounterVec
	k8sRequestsTotal      *prometheus.CounterVec
	k8sRequestSeconds     *prometheus.HistogramVec
	k8sThrottleWaitsTotal prometheus.Counter
	promQueriesTotal      *prometheus.CounterVec
	collectSeconds        *prometheus.HistogramVec
	wsFrameBytes          prometheus.Histogram
	storeRequestsTotal    *prometheus.CounterVec
}

func newControlPlaneMetrics() *controlPlaneMetrics {
	reg := prometheus.NewRegistry()
	m := &controlPlaneMetrics{registry: reg}

	m.admitTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cloud9_admit_total",
		Help: "Create admission decisions by result.",
	}, []string{"result"})
	m.deployQueueDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cloud9_deploy_queue_depth",
		Help: "Deployments waiting in the room deploy queue.",
	})
	m.doorWaitSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "cloud9_door_wait_seconds",
		Help:    "Seconds from provision start to doorway acquire (0 when no wait).",
		Buckets: controlPlaneDurationBuckets,
	})
	m.provisionSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "cloud9_provision_seconds",
		Help:    "Seconds from deployment created_at to running or failed after doorway held.",
		Buckets: controlPlaneDurationBuckets,
	})
	m.orchestrationSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "cloud9_orchestration_seconds",
		Help:    "Seconds from doorway acquire to running or failed (excludes door wait).",
		Buckets: controlPlaneDurationBuckets,
	})
	m.teardownLagSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "cloud9_teardown_lag_seconds",
		Help:    "Seconds from expires_at to namespace delete start.",
		Buckets: controlPlaneDurationBuckets,
	})
	m.websocketLive = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cloud9_websocket_live",
		Help: "Active per-session metrics publisher loops.",
	})
	m.informerQueueDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cloud9_informer_queue_depth",
		Help: "Shared informer workqueue depth (0 until informer path is enabled).",
	}, []string{"informer"})
	m.informerEventAge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cloud9_informer_event_age_seconds",
		Help: "Seconds since the last shared informer Add, Update, or Delete event.",
	}, []string{"informer"})
	m.metricsQueriesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cloud9_metrics_queries_total",
		Help: "Cluster-wide metrics poll queries per refresh (not per session).",
	}, []string{"source"})
	m.k8sRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cloud9_k8s_requests_total",
		Help: "Kubernetes API requests by verb, resource, and logical caller.",
	}, []string{"verb", "resource", "caller"})
	m.k8sRequestSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cloud9_k8s_request_seconds",
		Help:    "Kubernetes API request latency by verb, resource, and logical caller.",
		Buckets: k8sRequestDurationBuckets,
	}, []string{"verb", "resource", "caller"})
	m.k8sThrottleWaitsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cloud9_k8s_throttle_waits_total",
		Help: "Client-side rate limiter waits longer than 1ms.",
	})
	m.promQueriesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cloud9_prom_queries_total",
		Help: "Prometheus instant queries by path, logical caller, and deployment template.",
	}, []string{"path", "caller", "template"})
	m.collectSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cloud9_collect_seconds",
		Help:    "Per-session metrics Collect duration by deployment template.",
		Buckets: collectDurationBuckets,
	}, []string{"template"})
	m.wsFrameBytes = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "cloud9_ws_frame_bytes",
		Help:    "Serialized metrics WebSocket frame size in bytes.",
		Buckets: wsFrameBytesBuckets,
	})
	m.storeRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cloud9_store_requests_total",
		Help: "Supabase PostgREST requests by store operation.",
	}, []string{"op"})

	reg.MustRegister(
		m.admitTotal,
		m.deployQueueDepth,
		m.doorWaitSeconds,
		m.provisionSeconds,
		m.orchestrationSeconds,
		m.teardownLagSeconds,
		m.websocketLive,
		m.informerQueueDepth,
		m.informerEventAge,
		m.metricsQueriesTotal,
		m.k8sRequestsTotal,
		m.k8sRequestSeconds,
		m.k8sThrottleWaitsTotal,
		m.promQueriesTotal,
		m.collectSeconds,
		m.wsFrameBytes,
		m.storeRequestsTotal,
	)
	for _, name := range []string{"pod", "deployment", "statefulset", "hpa", "endpoints", "endpointslice"} {
		m.informerQueueDepth.WithLabelValues(name).Set(1)
	}
	// CounterVec omits a series until the label exists. Publish the known results at 0
	// so GET /metrics lists cloud9_admit_total before the first create.
	for _, result := range []string{"admitted", "rejected", "queued"} {
		m.admitTotal.WithLabelValues(result)
	}
	// CounterVec and HistogramVec omit HELP until a label set exists.
	m.k8sRequestsTotal.WithLabelValues("get", "pods", "unknown")
	m.k8sRequestSeconds.WithLabelValues("get", "pods", "unknown")
	return m
}

func (m *controlPlaneMetrics) metricsHandler() http.Handler {
	if m == nil {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *controlPlaneMetrics) gather() (string, error) {
	if m == nil {
		return "", nil
	}
	metricFamilies, err := m.registry.Gather()
	if err != nil {
		return "", err
	}
	var out string
	for _, mf := range metricFamilies {
		out += mf.String()
	}
	return out, nil
}

func (m *controlPlaneMetrics) observeMetricsQueries(source string, count int) {
	if m == nil || count <= 0 {
		return
	}
	m.metricsQueriesTotal.WithLabelValues(source).Add(float64(count))
}

// RegisterRingGauge registers cloud9_metrics_ring_bytes on the private registry.
// reader is evaluated on each /metrics scrape; call after the metrics collector exists.
func (m *controlPlaneMetrics) RegisterRingGauge(reader func() float64) {
	if m == nil || reader == nil {
		return
	}
	g := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cloud9_metrics_ring_bytes",
		Help: "Bytes of JSON-marshaled snapshot rings across all live sessions.",
	}, reader)
	m.registry.MustRegister(g)
}

func (m *controlPlaneMetrics) observeCollectSeconds(template string, seconds float64) {
	if m == nil {
		return
	}
	if template == "" {
		template = "none"
	}
	m.collectSeconds.WithLabelValues(template).Observe(seconds)
}

func (m *controlPlaneMetrics) observeFrameBytes(n int) {
	if m == nil || n < 0 {
		return
	}
	m.wsFrameBytes.Observe(float64(n))
}

func (m *controlPlaneMetrics) observeStoreRequest(op string) {
	if m == nil || op == "" {
		return
	}
	m.storeRequestsTotal.WithLabelValues(op).Inc()
}

func (m *controlPlaneMetrics) observePromQuery(path, caller, template string) {
	if m == nil || path == "" {
		return
	}
	if template == "" {
		template = "none"
	}
	if caller == "" {
		caller = "collect"
	}
	m.promQueriesTotal.WithLabelValues(path, caller, template).Inc()
}

func (m *controlPlaneMetrics) ObserveRequest(verb, resource, caller string, seconds float64) {
	if m == nil {
		return
	}
	m.k8sRequestsTotal.WithLabelValues(verb, resource, caller).Inc()
	m.k8sRequestSeconds.WithLabelValues(verb, resource, caller).Observe(seconds)
}

func (m *controlPlaneMetrics) ObserveThrottle(waitSeconds float64) {
	if m == nil || waitSeconds <= 0 {
		return
	}
	m.k8sThrottleWaitsTotal.Add(1)
}

func (m *controlPlaneMetrics) incAdmit(result string) {
	if m == nil {
		return
	}
	m.admitTotal.WithLabelValues(result).Inc()
}

func (m *controlPlaneMetrics) setDeployQueueDepth(n float64) {
	if m == nil {
		return
	}
	m.deployQueueDepth.Set(n)
}

func (m *controlPlaneMetrics) observeDoorWait(seconds float64) {
	if m == nil {
		return
	}
	m.doorWaitSeconds.Observe(seconds)
}

func (m *controlPlaneMetrics) observeProvision(seconds float64) {
	if m == nil {
		return
	}
	m.provisionSeconds.Observe(seconds)
}

func (m *controlPlaneMetrics) observeOrchestration(seconds float64) {
	if m == nil {
		return
	}
	m.orchestrationSeconds.Observe(seconds)
}

func (m *controlPlaneMetrics) observeTeardownLag(seconds float64) {
	if m == nil {
		return
	}
	m.teardownLagSeconds.Observe(seconds)
}

func (m *controlPlaneMetrics) setWebsocketLive(n float64) {
	if m == nil {
		return
	}
	m.websocketLive.Set(n)
}

// SetInformerQueueDepth sets cloud9_informer_queue_depth. When FIFO depth is unavailable,
// depth is 0 after HasSynced and 1 before sync.
func (m *controlPlaneMetrics) SetInformerQueueDepth(informer string, depth float64) {
	if m == nil || informer == "" {
		return
	}
	m.informerQueueDepth.WithLabelValues(informer).Set(depth)
}

// NoteInformerEvent records the time of the latest informer mutation for event-age metrics.
func (m *controlPlaneMetrics) NoteInformerEvent(informer string) {
	if m == nil || informer == "" {
		return
	}
	now := time.Now()
	m.informerEventMu.Lock()
	if m.informerLastEvent == nil {
		m.informerLastEvent = make(map[string]time.Time)
	}
	m.informerLastEvent[informer] = now
	m.informerEventMu.Unlock()
	m.informerEventAge.WithLabelValues(informer).Set(0)
}

// RefreshInformerEventAges updates cloud9_informer_event_age_seconds from last event times.
func (m *controlPlaneMetrics) RefreshInformerEventAges() {
	if m == nil {
		return
	}
	m.informerEventMu.Lock()
	defer m.informerEventMu.Unlock()
	for informer, at := range m.informerLastEvent {
		m.informerEventAge.WithLabelValues(informer).Set(time.Since(at).Seconds())
	}
}

func (h *Handler) refreshDeployQueueDepth(ctx context.Context) {
	if h == nil || h.cpMetrics == nil || h.store == nil {
		return
	}
	n, err := h.store.CountQueuedDeployments(ctx)
	if err != nil {
		return
	}
	h.cpMetrics.setDeployQueueDepth(float64(n))
}

func (h *Handler) refreshWebsocketLiveGauge() {
	if h == nil || h.cpMetrics == nil || h.metricsCollector == nil {
		return
	}
	h.cpMetrics.setWebsocketLive(float64(h.metricsCollector.PublisherCount()))
}

// RefreshWebsocketLiveGauge updates cloud9_websocket_live from the metrics publisher map.
func (h *Handler) RefreshWebsocketLiveGauge() {
	h.refreshWebsocketLiveGauge()
}

// ObserveTeardownLag records lag from session expiry to delete start.
func (h *Handler) ObserveTeardownLag(expiresAt time.Time) {
	if h == nil || h.cpMetrics == nil || expiresAt.IsZero() {
		return
	}
	lag := time.Since(expiresAt)
	if lag < 0 {
		lag = 0
	}
	h.cpMetrics.observeTeardownLag(lag.Seconds())
}

func (h *Handler) observeAdmitResult(ctx context.Context, status string, err error) {
	if h == nil || h.cpMetrics == nil {
		return
	}
	if err != nil {
		h.cpMetrics.incAdmit("rejected")
		return
	}
	switch status {
	case statusQueued:
		h.cpMetrics.incAdmit("queued")
	default:
		h.cpMetrics.incAdmit("admitted")
	}
	h.refreshDeployQueueDepth(ctx)
}

// recordProvisionTimings updates histograms and metrics_summary after doorway work finishes.
// holdStart must be non-zero when the doorway slot was held through orchestration.
// Door wait must already be observed at doorway acquire.
func (h *Handler) recordProvisionTimings(
	ctx context.Context,
	namespace string,
	holdStart time.Time,
	doorWait time.Duration,
) {
	if namespace == "" || h == nil || holdStart.IsZero() {
		return
	}
	doorWaitSec := doorWait.Seconds()
	orchSec := time.Since(holdStart).Seconds()
	var provisionSec float64
	if h.store != nil {
		row, err := h.store.GetDeployment(ctx, namespace)
		if err == nil && !row.CreatedAt.IsZero() {
			provisionSec = time.Since(row.CreatedAt).Seconds()
		}
	}
	if h.cpMetrics != nil {
		h.cpMetrics.observeOrchestration(orchSec)
		if provisionSec > 0 {
			h.cpMetrics.observeProvision(provisionSec)
		}
	}
	if h.store != nil && provisionSec > 0 {
		h.stampMetrics(ctx, namespace, map[string]any{
			"provision_seconds":     provisionSec,
			"orchestration_seconds": orchSec,
			"door_wait_seconds":     doorWaitSec,
		})
	}
}
