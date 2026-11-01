package queue

import (
	"context"
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/ywacch/cloud9/pkg/templateapp/tracemq"
)

// RabbitMQOptions configures an AMQP queue adapter.
type RabbitMQOptions struct {
	Host       string
	Port       string
	QueueName  string
	Prefetch   int
	Registerer prometheus.Registerer
}

// RabbitMQ implements Queue over a durable AMQP queue.
type RabbitMQ struct {
	conn  *amqp.Connection
	ch    *amqp.Channel
	queue string
	opts  RabbitMQOptions
}

// NewRabbitMQ dials RabbitMQ and declares the queue.
func NewRabbitMQ(opts RabbitMQOptions) (*RabbitMQ, error) {
	host := strings.TrimSpace(opts.Host)
	port := strings.TrimSpace(opts.Port)
	queueName := strings.TrimSpace(opts.QueueName)
	if host == "" || port == "" || queueName == "" {
		return nil, fmt.Errorf("queue: rabbitmq host, port, and queue name are required")
	}
	registerPoisonMetrics(opts.Registerer)
	conn, err := amqp.Dial(fmt.Sprintf("amqp://cloud9:cloud9@%s:%s/", host, port))
	if err != nil {
		return nil, fmt.Errorf("queue: rabbitmq dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("queue: rabbitmq channel: %w", err)
	}
	if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("queue: rabbitmq declare: %w", err)
	}
	prefetch := opts.Prefetch
	if prefetch <= 0 {
		prefetch = 10
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("queue: rabbitmq qos: %w", err)
	}
	return &RabbitMQ{conn: conn, ch: ch, queue: queueName, opts: opts}, nil
}

// Engine returns "rabbitmq".
func (r *RabbitMQ) Engine() string {
	return "rabbitmq"
}

// Publish sends body to the queue with trace headers from ctx.
func (r *RabbitMQ) Publish(ctx context.Context, body []byte) error {
	return r.ch.PublishWithContext(ctx, "", r.queue, false, false, amqp.Publishing{
		ContentType: "application/json",
		Headers:     tracemq.Headers(ctx),
		Body:        body,
	})
}

// Consume reads deliveries until ctx is cancelled.
func (r *RabbitMQ) Consume(ctx context.Context, h func(ctx context.Context, body []byte) error) error {
	msgs, err := r.ch.Consume(r.queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("queue: rabbitmq consume: %w", err)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-msgs:
			if !ok {
				return nil
			}
			r.handleDelivery(ctx, d, h)
		}
	}
}

func (r *RabbitMQ) handleDelivery(parent context.Context, d amqp.Delivery, h func(ctx context.Context, body []byte) error) {
	msgCtx := tracemq.ContextFromDelivery(parent, d)
	if err := h(msgCtx, d.Body); err != nil {
		attempts := rabbitDeliveryAttempts(d)
		if attempts >= maxDeliveryAttempts {
			incPoison(r.Engine(), r.queue)
			_ = d.Ack(false)
			return
		}
		_ = d.Nack(false, true)
		return
	}
	_ = d.Ack(false)
}

func rabbitDeliveryAttempts(d amqp.Delivery) int {
	if death, ok := d.Headers["x-death"].([]interface{}); ok && len(death) > 0 {
		if table, ok := death[0].(amqp.Table); ok {
			if count, ok := table["count"].(int64); ok && count > 0 {
				return int(count) + 1
			}
		}
	}
	if d.Redelivered {
		return 2
	}
	return 1
}

// Close shuts down the AMQP channel and connection.
func (r *RabbitMQ) Close() error {
	if r.ch != nil {
		_ = r.ch.Close()
	}
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

// Ping reports whether the AMQP connection is open.
func (r *RabbitMQ) Ping() error {
	if r.conn == nil || r.conn.IsClosed() {
		return fmt.Errorf("queue: rabbitmq connection closed")
	}
	return nil
}

// Connection exposes the underlying AMQP connection for health checks.
func (r *RabbitMQ) Connection() *amqp.Connection {
	return r.conn
}
