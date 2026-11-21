package queue

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	poisonTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "queue_poison_total",
			Help: "Messages dropped after repeated handler failures",
		},
		[]string{"engine", "queue"},
	)
	publishTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "queue_publish_total",
			Help: "Messages published to the queue",
		},
		[]string{"engine", "queue"},
	)
	ackTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "queue_ack_total",
			Help: "Messages acknowledged by consumers",
		},
		[]string{"engine", "queue"},
	)
	redeliverTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "queue_redeliver_total",
			Help: "Messages redelivered before acknowledgment",
		},
		[]string{"engine", "queue"},
	)
)

func registerQueueMetrics(reg prometheus.Registerer) {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	_ = reg.Register(poisonTotal)
	_ = reg.Register(publishTotal)
	_ = reg.Register(ackTotal)
	_ = reg.Register(redeliverTotal)
}

func registerPoisonMetrics(reg prometheus.Registerer) {
	registerQueueMetrics(reg)
}

func incPoison(engine, queueName string) {
	poisonTotal.WithLabelValues(engine, queueName).Inc()
}

func incPublish(engine, queueName string) {
	publishTotal.WithLabelValues(engine, queueName).Inc()
}

func incAck(engine, queueName string) {
	ackTotal.WithLabelValues(engine, queueName).Inc()
}

func incRedeliver(engine, queueName string) {
	redeliverTotal.WithLabelValues(engine, queueName).Inc()
}
