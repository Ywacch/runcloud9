package queue

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

// Config selects a queue implementation from deployer job env vars.
type Config struct {
	EngineEnv   string
	HostEnv     string
	PortEnv     string
	QueueName   string
	Prefetch    int
	Registerer  prometheus.Registerer
	RedisClient redis.UniversalClient
}

// Open returns a Queue from WORK_QUEUE_* (or legacy MQ_HOST) configuration.
func Open(cfg Config) (Queue, error) {
	engine := strings.ToLower(strings.TrimSpace(cfg.EngineEnv))
	host := strings.TrimSpace(cfg.HostEnv)
	port := strings.TrimSpace(cfg.PortEnv)
	queueName := strings.TrimSpace(cfg.QueueName)
	if queueName == "" {
		return nil, fmt.Errorf("queue: queue name is required")
	}
	if host == "" {
		return nil, fmt.Errorf("queue: host is required")
	}
	if port == "" {
		port = defaultPort(engine)
	}
	switch engine {
	case "", "rabbitmq":
		return NewRabbitMQ(RabbitMQOptions{
			Host:       host,
			Port:       port,
			QueueName:  queueName,
			Prefetch:   cfg.Prefetch,
			Registerer: cfg.Registerer,
		})
	case "redis":
		if cfg.RedisClient == nil {
			client := redis.NewClient(&redis.Options{Addr: fmt.Sprintf("%s:%s", host, port)})
			cfg.RedisClient = client
		}
		return NewStreams(StreamsOptions{
			Client:     cfg.RedisClient,
			StreamKey:  queueName,
			Registerer: cfg.Registerer,
		})
	default:
		return nil, fmt.Errorf("queue: unsupported WORK_QUEUE_ENGINE %q", engine)
	}
}

func defaultPort(engine string) string {
	switch engine {
	case "redis":
		return "6379"
	default:
		return "5672"
	}
}

// EnvConfig builds Config from standard process env.
func EnvConfig(queueName string, reg prometheus.Registerer, rdb redis.UniversalClient) Config {
	engine := os.Getenv("WORK_QUEUE_ENGINE")
	host := os.Getenv("WORK_QUEUE_HOST")
	port := os.Getenv("WORK_QUEUE_PORT")
	if strings.TrimSpace(host) == "" {
		host = os.Getenv("MQ_HOST")
	}
	if strings.TrimSpace(port) == "" {
		port = os.Getenv("MQ_PORT")
	}
	if strings.TrimSpace(engine) == "" {
		engine = "rabbitmq"
	}
	prefetch := 10
	if v := os.Getenv("WORKER_PREFETCH"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			prefetch = n
		}
	}
	return Config{
		EngineEnv:   engine,
		HostEnv:     host,
		PortEnv:     port,
		QueueName:   queueName,
		Prefetch:    prefetch,
		Registerer:  reg,
		RedisClient: rdb,
	}
}
