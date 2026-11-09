package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCollect_RabbitMQPrometheusSignals(t *testing.T) {
	t.Parallel()
	const ns = "ns-rabbit-mq"
	rr := promRateRange()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "rabbitmq_queue_messages_ready"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("3"))
		case strings.Contains(q, "rabbitmq_queue_messages_unacked"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("7"))
		case strings.Contains(q, "rabbitmq_queue_messages{") && !strings.Contains(q, "_ready") && !strings.Contains(q, "_unacked"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("10"))
		case strings.Contains(q, "rabbitmq_global_messages_received_total"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("2.5"))
		case strings.Contains(q, "rabbitmq_global_messages_delivered_consume_auto_ack_total"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("1.5"))
		case strings.Contains(q, "rabbitmq_process_resident_memory_bytes"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("2048"))
		case strings.Contains(q, "rabbitmq_resident_memory_limit_bytes"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("4096"))
		case strings.Contains(q, rr):
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	dm, err := c.Collect(context.Background(), ns, "ecommerce", TopologyFlags{HasDB: true, HasCache: true})
	if err != nil {
		t.Fatal(err)
	}
	mq := dm.Components["mq"]
	if mq.QueueDepth != 10 || mq.MessagesReady != 3 || mq.MessagesUnacked != 7 {
		t.Fatalf("queue: depth=%v ready=%v unacked=%v", mq.QueueDepth, mq.MessagesReady, mq.MessagesUnacked)
	}
	if mq.ProduceRate != 2.5 || mq.ConsumeRate != 1.5 {
		t.Fatalf("rates: produce=%v consume=%v", mq.ProduceRate, mq.ConsumeRate)
	}
	if mq.MQMemoryUsed != 2048 || mq.MQMemoryLimit != 4096 {
		t.Fatalf("mem: used=%v limit=%v", mq.MQMemoryUsed, mq.MQMemoryLimit)
	}
	if mq.MemoryBytes != 2048 || mq.MemIsEstimated {
		t.Fatalf("mq memory override: bytes=%d estimated=%v", mq.MemoryBytes, mq.MemIsEstimated)
	}
}
