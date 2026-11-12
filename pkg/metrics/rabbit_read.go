package metrics

import (
	"context"

	"github.com/ywacch/cloud9/pkg/metrics/signals"
)

// applyRabbitMQBrokerMetrics fills mq queue and broker memory fields from Prometheus signals.
// Missing series stay unset on cm (not zero).
func (c *MetricsCollector) applyRabbitMQBrokerMetrics(ctx context.Context, ns string, cm *ComponentMetrics) {
	if c == nil || cm == nil || ns == "" {
		return
	}

	if v, ok := c.sig(ctx, ns, signals.RabbitMQQueueMessages); ok {
		cm.QueueDepth = v
	}
	if v, ok := c.sig(ctx, ns, signals.RabbitMQQueueMessagesReady); ok {
		cm.MessagesReady = v
	}
	if v, ok := c.sig(ctx, ns, signals.RabbitMQQueueMessagesUnacked); ok {
		cm.MessagesUnacked = v
	}
	if v, ok := c.sig(ctx, ns, signals.RabbitMQGlobalMessagesReceivedRate); ok {
		cm.ProduceRate = v
	}
	if v, ok := c.sig(ctx, ns, signals.RabbitMQGlobalMessagesDeliveredRate); ok {
		cm.ConsumeRate = v
	}
	if v, ok := c.sig(ctx, ns, signals.RabbitMQProcessResidentMemoryBytes); ok {
		cm.MQMemoryUsed = v
	}
	if v, ok := c.sig(ctx, ns, signals.RabbitMQResidentMemoryLimitBytes); ok {
		cm.MQMemoryLimit = v
	}
}
