package consistency

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	readsCheckedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "consistency_reads_checked_total",
			Help: "Sampled cache hits verified against the source of truth",
		},
		[]string{"source", "template"},
	)
	readStaleTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "consistency_read_stale_total",
			Help: "Sampled cache hits that served a different value than the source of truth",
		},
		[]string{"source", "template"},
	)
	verifyDroppedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "consistency_verify_dropped_total",
			Help: "Cache consistency observations dropped because the verify queue was full",
		},
		[]string{"source", "template"},
	)
	verifyErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "consistency_verify_errors_total",
			Help: "Out-of-band truth reads that failed during cache consistency verification",
		},
		[]string{"source", "template"},
	)
)
