package readrouter

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	dbReadTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "db_read_total",
			Help: "Database reads routed to leader or replica",
		},
		[]string{"route"},
	)
	rywViolationsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ryw_violations_total",
			Help: "Read-your-writes violations observed on replica reads",
		},
	)
	monotonicViolationsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "monotonic_violations_total",
			Help: "Monotonic read violations observed on replica reads",
		},
	)
	rywLeaderRedirectsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "ryw_leader_redirects_total",
			Help: "Reads redirected to leader to satisfy read-your-writes",
		},
	)
)
