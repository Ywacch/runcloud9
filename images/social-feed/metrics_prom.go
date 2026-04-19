package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

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
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency in seconds",
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

	dbQueryDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "db_query_duration_seconds",
			Help:    "Database query latency in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation"},
	)

	postTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "social_post_total",
			Help: "Total posts created",
		},
		[]string{"mode"},
	)

	feedRequestTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "social_feed_request_total",
			Help: "Total feed requests",
		},
		[]string{"mode"},
	)

	feedLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "social_feed_latency_seconds",
			Help:    "Feed generation/fetch latency",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5},
		},
		[]string{"mode"},
	)

	mqPublishTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "social_mq_publish_total",
		Help: "Total messages published to fanout queue",
	})

	mqPublishErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "social_mq_publish_errors_total",
		Help: "Failed MQ publishes",
	})
)
