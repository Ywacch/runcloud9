//go:build integration

package queue_test

import (
	"os"
	"testing"

	"github.com/ywacch/cloud9/pkg/templateapp/jobs/queue"
	"github.com/ywacch/cloud9/pkg/templateapp/jobs/queue/queuetest"
)

func TestQueueConformance_RabbitMQ_Redeliver(t *testing.T) {
	host := os.Getenv("RABBITMQ_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port := os.Getenv("RABBITMQ_PORT")
	if port == "" {
		port = "5672"
	}
	queuetest.RunRedeliverConformance(t, func(t *testing.T) (queue.Queue, func()) {
		t.Helper()
		q, err := queue.NewRabbitMQ(queue.RabbitMQOptions{
			Host:      host,
			Port:      port,
			QueueName: "conformance-redeliver",
			Prefetch:  1,
		})
		if err != nil {
			t.Skipf("rabbitmq unavailable: %v", err)
		}
		return q, func() { _ = q.Close() }
	})
}
