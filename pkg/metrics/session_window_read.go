package metrics

import (
	"context"
	"fmt"
	"log"
	"math"
	"time"

	metricshub "github.com/ywacch/cloud9/pkg/metrics/hub"
)

func (c *MetricsCollector) appSessionLatency9m(ctx context.Context, ns, nsSel string) (p50, p95, p99 float64) {
	sessionRange := sessionP99PromRange(c.sessionAcc.Elapsed(ns, time.Now()))
	latBucketRate := fmt.Sprintf(`sum(rate(http_request_duration_seconds_bucket{%s}%s)) by (le)`, nsSel, sessionRange)
	p50Old, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.50, %s)`, latBucketRate))
	p95Old, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.95, %s)`, latBucketRate))
	p99Old, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.99, %s)`, latBucketRate))

	p50, p95, p99 = p50Old, p95Old, p99Old
	h := c.MetricsHub()
	if h == nil {
		return p50, p95, p99
	}
	if v, ok := h.SessionQuantile(ns, metricshub.StreamAppHTTPLatency, 0.50); ok {
		p50 = v
		if signalCompareEnabled() {
			c.compareSessionQuantile("session_app_latency_p50", v, true, p50Old, true)
		}
	}
	if v, ok := h.SessionQuantile(ns, metricshub.StreamAppHTTPLatency, 0.95); ok {
		p95 = v
		if signalCompareEnabled() {
			c.compareSessionQuantile("session_app_latency_p95", v, true, p95Old, true)
		}
	}
	if v, ok := h.SessionQuantile(ns, metricshub.StreamAppHTTPLatency, 0.99); ok {
		p99 = v
		if signalCompareEnabled() {
			c.compareSessionQuantile("session_app_latency_p99", v, true, p99Old, true)
		}
	}
	return p50, p95, p99
}

func (c *MetricsCollector) workerSessionLatency9m(ctx context.Context, ns string) (p50, p95, p99 float64) {
	sessionRange := sessionP99PromRange(c.sessionAcc.Elapsed(ns, time.Now()))
	workerSessionBucketRate := fmt.Sprintf(
		`sum(rate({__name__=~".*_worker_processing_duration_seconds_bucket",namespace=%q}%s)) by (le)`,
		ns, sessionRange,
	)
	p50Old, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.50, %s)`, workerSessionBucketRate))
	p95Old, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.95, %s)`, workerSessionBucketRate))
	p99Old, _ := c.query(ctx, fmt.Sprintf(`histogram_quantile(0.99, %s)`, workerSessionBucketRate))

	p50, p95, p99 = p50Old, p95Old, p99Old
	h := c.MetricsHub()
	if h == nil {
		return p50, p95, p99
	}
	if v, ok := h.SessionQuantile(ns, metricshub.StreamWorkerProcLatency, 0.50); ok {
		p50 = v
		if signalCompareEnabled() {
			c.compareSessionQuantile("session_worker_latency_p50", v, true, p50Old, true)
		}
	}
	if v, ok := h.SessionQuantile(ns, metricshub.StreamWorkerProcLatency, 0.95); ok {
		p95 = v
		if signalCompareEnabled() {
			c.compareSessionQuantile("session_worker_latency_p95", v, true, p95Old, true)
		}
	}
	if v, ok := h.SessionQuantile(ns, metricshub.StreamWorkerProcLatency, 0.99); ok {
		p99 = v
		if signalCompareEnabled() {
			c.compareSessionQuantile("session_worker_latency_p99", v, true, p99Old, true)
		}
	}
	return p50, p95, p99
}

func (c *MetricsCollector) sessionTotalHTTP(ctx context.Context, ns string, byPod map[string]float64) float64 {
	old := c.sessionAcc.Observe(ns, seriesHTTP, byPod)
	h := c.MetricsHub()
	if h == nil {
		return old
	}
	if v, ok := h.SessionTotal(ns, metricshub.StreamHTTPRequests); ok {
		if signalCompareEnabled() {
			c.compareSessionTotal("session_http_requests_total", v, true, old, true)
		}
		return v
	}
	return old
}

func (c *MetricsCollector) sessionTotalHTTP5xx(ctx context.Context, ns string, byPod map[string]float64) float64 {
	old := c.sessionAcc.Observe(ns, seriesHTTP5xx, byPod)
	h := c.MetricsHub()
	if h == nil {
		return old
	}
	if v, ok := h.SessionTotal(ns, metricshub.StreamHTTP5xx); ok {
		if signalCompareEnabled() {
			c.compareSessionTotal("session_http_5xx_total", v, true, old, true)
		}
		return v
	}
	return old
}

func (c *MetricsCollector) sessionTotalCacheHits(ns string, byPod map[string]float64) float64 {
	old := c.sessionAcc.Observe(ns, seriesCacheHits, byPod)
	h := c.MetricsHub()
	if h == nil {
		return old
	}
	if v, ok := h.SessionTotal(ns, metricshub.StreamCacheHits); ok {
		if signalCompareEnabled() {
			c.compareSessionTotal("session_cache_hits_total", v, true, old, true)
		}
		return v
	}
	return old
}

func (c *MetricsCollector) sessionTotalCacheMisses(ns string, byPod map[string]float64) float64 {
	old := c.sessionAcc.Observe(ns, seriesCacheMisses, byPod)
	h := c.MetricsHub()
	if h == nil {
		return old
	}
	if v, ok := h.SessionTotal(ns, metricshub.StreamCacheMisses); ok {
		if signalCompareEnabled() {
			c.compareSessionTotal("session_cache_misses_total", v, true, old, true)
		}
		return v
	}
	return old
}

func (c *MetricsCollector) sessionTotalCacheErrors(ns string, byPod map[string]float64) float64 {
	old := c.sessionAcc.Observe(ns, seriesCacheErrors, byPod)
	h := c.MetricsHub()
	if h == nil {
		return old
	}
	if v, ok := h.SessionTotal(ns, metricshub.StreamCacheErrors); ok {
		if signalCompareEnabled() {
			c.compareSessionTotal("session_cache_errors_total", v, true, old, true)
		}
		return v
	}
	return old
}

func (c *MetricsCollector) sessionTotalSQL(ns string, byPod map[string]float64) float64 {
	old := c.sessionAcc.Observe(ns, seriesSQL, byPod)
	h := c.MetricsHub()
	if h == nil {
		return old
	}
	if v, ok := h.SessionTotal(ns, metricshub.StreamSQLQueries); ok {
		if signalCompareEnabled() {
			c.compareSessionTotal("session_sql_queries_total", v, true, old, true)
		}
		return v
	}
	return old
}

func (c *MetricsCollector) sessionTotalWorkerProcessed(ns string, byPod map[string]float64) float64 {
	old := c.sessionAcc.Observe(ns, seriesWorkerProcessed, byPod)
	h := c.MetricsHub()
	if h == nil {
		return old
	}
	if v, ok := h.SessionTotal(ns, metricshub.StreamWorkerProcessed); ok {
		if signalCompareEnabled() {
			c.compareSessionTotal("session_worker_processed_total", v, true, old, true)
		}
		return v
	}
	return old
}

func (c *MetricsCollector) sessionTotalWorkerErrors(ns string, byPod map[string]float64) float64 {
	old := c.sessionAcc.Observe(ns, seriesWorkerErrors, byPod)
	h := c.MetricsHub()
	if h == nil {
		return old
	}
	if v, ok := h.SessionTotal(ns, metricshub.StreamWorkerErrors); ok {
		if signalCompareEnabled() {
			c.compareSessionTotal("session_worker_errors_total", v, true, old, true)
		}
		return v
	}
	return old
}

func (c *MetricsCollector) compareSessionTotal(signal string, hubVal float64, hubOK bool, oldVal float64, oldOK bool) {
	match := promValuesMatch(hubVal, oldVal, hubOK, oldOK)
	result := "match"
	if !match {
		result = "mismatch"
		log.Printf("metrics: session compare mismatch signal=%s hub=%v old=%v ok=(%v,%v)", signal, hubVal, oldVal, hubOK, oldOK)
	}
	if c != nil && c.signalCompareObserver != nil {
		c.signalCompareObserver(signal, result)
	}
}

func (c *MetricsCollector) compareSessionQuantile(signal string, hubVal float64, hubOK bool, oldVal float64, oldOK bool) {
	match := promSessionQuantileMatch(hubVal, oldVal, hubOK, oldOK)
	result := "match"
	if !match {
		result = "mismatch"
		log.Printf("metrics: session compare mismatch signal=%s hub=%v old=%v ok=(%v,%v)", signal, hubVal, oldVal, hubOK, oldOK)
	}
	if c != nil && c.signalCompareObserver != nil {
		c.signalCompareObserver(signal, result)
	}
}

func promSessionQuantileMatch(signalVal, oldVal float64, signalOK, oldOK bool) bool {
	if !signalOK && !oldOK {
		return true
	}
	if signalOK != oldOK {
		return false
	}
	tol := mathMaxSessionTol(signalVal, oldVal, 0.10)
	return math.Abs(signalVal-oldVal) <= tol
}

func mathMaxSessionTol(a, b, rel float64) float64 {
	m := a
	if b > m {
		m = b
	}
	if m < 0 {
		m = -m
	}
	t := m * rel
	if t < 0.01 {
		t = 0.01
	}
	return t
}
