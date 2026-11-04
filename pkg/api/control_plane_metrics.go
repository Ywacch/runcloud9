package api

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var controlPlaneDurationBuckets = []float64{1, 5, 15, 30, 60, 120, 300, 600, 1200}

// controlPlaneMetrics holds Prometheus collectors for cloud9-api provision timing.
// It uses a private registry so this process does not register app-side metrics.
type controlPlaneMetrics struct {
	registry             *prometheus.Registry
	admitTotal           *prometheus.CounterVec
	deployQueueDepth     prometheus.Gauge
	doorWaitSeconds      prometheus.Histogram
	provisionSeconds     prometheus.Histogram
	orchestrationSeconds prometheus.Histogram
	teardownLagSeconds   prometheus.Histogram
	websocketLive        prometheus.Gauge
	informerQueueDepth   *prometheus.GaugeVec
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

	reg.MustRegister(
		m.admitTotal,
		m.deployQueueDepth,
		m.doorWaitSeconds,
		m.provisionSeconds,
		m.orchestrationSeconds,
		m.teardownLagSeconds,
		m.websocketLive,
		m.informerQueueDepth,
	)
	m.informerQueueDepth.WithLabelValues("pod").Set(0)
	// CounterVec omits a series until the label exists. Publish the known results at 0
	// so GET /metrics lists cloud9_admit_total before the first create.
	for _, result := range []string{"admitted", "rejected", "queued"} {
		m.admitTotal.WithLabelValues(result)
	}
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
