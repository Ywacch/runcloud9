package queue

import (
	"github.com/prometheus/client_golang/prometheus"
)

var poisonTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "queue_poison_total",
		Help: "Messages dropped after repeated handler failures",
	},
	[]string{"engine", "queue"},
)

func registerPoisonMetrics(reg prometheus.Registerer) {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	_ = reg.Register(poisonTotal)
}

func incPoison(engine, queueName string) {
	poisonTotal.WithLabelValues(engine, queueName).Inc()
}
