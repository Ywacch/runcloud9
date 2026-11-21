package metrics

import (
	"testing"
)

func TestRabbitMQ_ParseFixture_MatchesLegacyDepth(t *testing.T) {
	t.Parallel()
	raw := `# HELP rabbitmq_queue_messages queue depth
rabbitmq_queue_messages{queue="fanout"} 42
rabbitmq_queue_messages_ready{queue="fanout"} 30
rabbitmq_queue_messages_unacked{queue="fanout"} 12
`
	depth, ready, unacked, _, _, _, _ := parseRabbitMQMetricsText(raw)
	if depth != 42 || ready != 30 || unacked != 12 {
		t.Fatalf("depth=%v ready=%v unacked=%v", depth, ready, unacked)
	}
}
