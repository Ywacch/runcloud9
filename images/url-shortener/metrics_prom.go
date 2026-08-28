package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Promauto registers metrics with the default Prometheus registry automatically.
// We define them as package-level vars so handlers and db.go can access them directly.
// counter = monotonically increasing (total requests, total errors)
// histogram = records distribution of values (request latency)
// gauge = can go up and down (active connections, current memory usage)

var (
	httpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "path", "status"},
	)

	httpRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "http_request_duration_seconds",
			Help: "HTTP request latency in seconds",
			// Buckets define the histogram resolution.
			// These cover sub-millisecond to multi-second latency ranges.
			// histogram_quantile() uses these buckets to estimate p99 etc.
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5},
		},
		[]string{"method", "path"},
	)

	cacheHitsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_hits_total",
		Help: "Total cache hits",
	})

	cacheMissesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_misses_total",
		Help: "Total cache misses",
	})

	shortenerCodeCollisionsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "shortener_code_collisions_total",
		Help: "Short code collisions detected while allocating a new code",
	})
)
