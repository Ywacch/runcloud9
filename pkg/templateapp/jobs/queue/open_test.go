package queue_test

import (
	"testing"

	"github.com/ywacch/cloud9/pkg/templateapp/jobs/queue"
)

func TestOpen_rabbitmqByDefault(t *testing.T) {
	t.Parallel()
	_, err := queue.Open(queue.Config{
		EngineEnv: "rabbitmq",
		HostEnv:   "",
		PortEnv:   "5672",
		QueueName: "fanout",
	})
	if err == nil {
		t.Fatal("expected error for empty host")
	}
}

func TestOpen_unsupportedEngine(t *testing.T) {
	t.Parallel()
	_, err := queue.Open(queue.Config{
		EngineEnv: "nats",
		HostEnv:   "localhost",
		PortEnv:   "4222",
		QueueName: "fanout",
	})
	if err == nil {
		t.Fatal("expected error")
	}
}
