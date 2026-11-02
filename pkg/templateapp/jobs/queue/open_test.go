package queue_test

import (
	"testing"

	"github.com/redis/go-redis/v9"
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

func TestOpen_redisNilClientDoesNotPanic(t *testing.T) {
	t.Parallel()
	var typedNil *redis.Client
	tests := []struct {
		name   string
		client redis.UniversalClient
	}{
		{name: "nil interface", client: nil},
		{name: "nil pointer in interface", client: typedNil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := queue.Open(queue.Config{
				EngineEnv:   "redis",
				HostEnv:     "127.0.0.1",
				PortEnv:     "1",
				QueueName:   "fanout",
				RedisClient: tt.client,
			})
			if err == nil {
				t.Fatal("expected error")
			}
		})
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
