package metrics

import (
	"context"
	"fmt"

	"github.com/ywacch/cloud9/pkg/metrics/signals"
)

func rabbitMQPodSelector(ns string) string {
	return fmt.Sprintf(`namespace=%q,pod=~"rabbitmq.*"`, ns)
}

// applyRabbitMQBrokerMetrics fills mq queue and broker memory fields from Prometheus signals.
// Missing series stay unset on cm (not zero).
func (c *MetricsCollector) applyRabbitMQBrokerMetrics(ctx context.Context, ns string, cm *ComponentMetrics) {
	if c == nil || cm == nil || ns == "" {
		return
	}
	sel := rabbitMQPodSelector(ns)
	rr := promRateRange()

	if v, ok := c.sigCompat(ctx, ns, signals.RabbitMQQueueMessages,
		fmt.Sprintf(`sum(rabbitmq_queue_messages{%s})`, sel)); ok {
		cm.QueueDepth = v
	}
	if v, ok := c.sigCompat(ctx, ns, signals.RabbitMQQueueMessagesReady,
		fmt.Sprintf(`sum(rabbitmq_queue_messages_ready{%s})`, sel)); ok {
		cm.MessagesReady = v
	}
	if v, ok := c.sigCompat(ctx, ns, signals.RabbitMQQueueMessagesUnacked,
		fmt.Sprintf(`sum(rabbitmq_queue_messages_unacked{%s})`, sel)); ok {
		cm.MessagesUnacked = v
	}
	if v, ok := c.sigCompat(ctx, ns, signals.RabbitMQGlobalMessagesReceivedRate,
		fmt.Sprintf(`sum(rate(rabbitmq_global_messages_received_total{%s}%s))`, sel, rr)); ok {
		cm.ProduceRate = v
	}
	if v, ok := c.sigCompat(ctx, ns, signals.RabbitMQGlobalMessagesDeliveredRate,
		fmt.Sprintf(`sum(rate(rabbitmq_global_messages_delivered_consume_auto_ack_total{%s}%s))`, sel, rr)); ok {
		cm.ConsumeRate = v
	}
	if v, ok := c.sigCompat(ctx, ns, signals.RabbitMQProcessResidentMemoryBytes,
		fmt.Sprintf(`sum(rabbitmq_process_resident_memory_bytes{%s})`, sel)); ok {
		cm.MQMemoryUsed = v
	}
	if v, ok := c.sigCompat(ctx, ns, signals.RabbitMQResidentMemoryLimitBytes,
		fmt.Sprintf(`sum(rabbitmq_resident_memory_limit_bytes{%s})`, sel)); ok {
		cm.MQMemoryLimit = v
	}
}
