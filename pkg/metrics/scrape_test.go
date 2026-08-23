package metrics

import (
	"math"
	"testing"
	"time"
)

func TestParseRabbitMQMetricsText(t *testing.T) {
	t.Parallel()
	raw := `
# TYPE rabbitmq_queue_messages gauge
rabbitmq_queue_messages{queue="q1"} 4
rabbitmq_queue_messages{queue="q2"} 6
rabbitmq_queue_messages_ready{queue="q1"} 3
rabbitmq_queue_messages_unacked{queue="q1"} 7
rabbitmq_global_messages_received_total 120
rabbitmq_global_messages_delivered_consume_auto_ack_total 96
rabbitmq_process_resident_memory_bytes 2048
rabbitmq_resident_memory_limit_bytes 4096
`
	depth, ready, unacked, received, delivered, memUsed, memLimit := parseRabbitMQMetricsText(raw)
	if depth != 10 || ready != 3 || unacked != 7 {
		t.Fatalf("queue parse mismatch: depth=%v ready=%v unacked=%v", depth, ready, unacked)
	}
	if received != 120 || delivered != 96 {
		t.Fatalf("counter parse mismatch: received=%v delivered=%v", received, delivered)
	}
	if memUsed != 2048 || memLimit != 4096 {
		t.Fatalf("memory parse mismatch: memUsed=%v memLimit=%v", memUsed, memLimit)
	}
}

func TestRabbitCounterRates(t *testing.T) {
	t.Parallel()
	c := &MetricsCollector{mqRates: make(map[string]rabbitCounterSnapshot)}
	ns := "cloud9-test"
	t0 := time.Unix(1000, 0)

	p, q := c.rabbitCounterRates(ns, 100, 80, t0)
	if p != 0 || q != 0 {
		t.Fatalf("first scrape rates must be zero, got produce=%v consume=%v", p, q)
	}

	p, q = c.rabbitCounterRates(ns, 130, 95, t0.Add(5*time.Second))
	if math.Abs(p-6) > 0.001 || math.Abs(q-3) > 0.001 {
		t.Fatalf("delta rates mismatch: produce=%v consume=%v", p, q)
	}
}

func TestParseConsistencySamplesJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		body    string
		wantKey string
	}{
		{
			name:    "newest first row",
			body:    `{"samples":[{"key":"product:7","served":"5","truth":"2","requestId":"rid-1","source":"cache"}]}`,
			wantKey: "product:7",
		},
		{
			name: "empty samples",
			body: `{"samples":[]}`,
		},
		{
			name: "invalid json",
			body: `{`,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := parseConsistencySamplesJSON([]byte(tc.body))
			if got.Key != tc.wantKey {
				t.Fatalf("key=%q want %q full=%+v", got.Key, tc.wantKey, got)
			}
			if tc.wantKey != "" {
				if got.Served != "5" || got.Truth != "2" || got.RequestID != "rid-1" {
					t.Fatalf("sample=%+v", got)
				}
			}
		})
	}
}
