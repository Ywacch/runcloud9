package introspect

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	rabbitUser = "cloud9"
	rabbitPass = "cloud9"
	rabbitPort = 5672
)

// RabbitMQDriver implements QueueDriver over AMQP (no management plugin).
type RabbitMQDriver struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

// NewRabbitMQDriver dials AMQP at host:5672 with session credentials.
func NewRabbitMQDriver(_ context.Context, host string) (*RabbitMQDriver, error) {
	url := fmt.Sprintf("amqp://%s:%s@%s:%d/", rabbitUser, rabbitPass, host, rabbitPort)
	conn, err := amqp.DialConfig(url, amqp.Config{
		Dial: amqp.DefaultDial(5 * time.Second),
	})
	if err != nil {
		return nil, fmt.Errorf("rabbitmq dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("rabbitmq channel: %w", err)
	}
	return &RabbitMQDriver{conn: conn, ch: ch}, nil
}

func (d *RabbitMQDriver) Capabilities() []Capability {
	return []Capability{CapQueues, CapPeek}
}

func (d *RabbitMQDriver) Close() error {
	var first error
	if d.ch != nil {
		if err := d.ch.Close(); err != nil && first == nil {
			first = err
		}
	}
	if d.conn != nil {
		if err := d.conn.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// ListQueues inspects each whitelisted name via passive declare.
func (d *RabbitMQDriver) ListQueues(_ context.Context, names []string) ([]QueueInfo, error) {
	out := make([]QueueInfo, 0, len(names))
	for _, name := range names {
		info := QueueInfo{Name: name}
		q, err := d.ch.QueueDeclarePassive(name, true, false, false, false, nil)
		if err != nil {
			info.Available = false
			out = append(out, info)
			continue
		}
		info.Available = true
		info.Messages = q.Messages
		info.Consumers = q.Consumers
		out = append(out, info)
	}
	return out, nil
}

// PeekQueue performs basic.get then nack+requeue so workers are not starved.
func (d *RabbitMQDriver) PeekQueue(_ context.Context, name string) (PeekResult, error) {
	msg, ok, err := d.ch.Get(name, false)
	if err != nil {
		return PeekResult{}, fmt.Errorf("peek %q: %w", name, err)
	}
	if !ok {
		return PeekResult{Queue: name, Empty: true}, nil
	}
	requeued := true
	if err := d.ch.Nack(msg.DeliveryTag, false, true); err != nil {
		requeued = false
		_ = d.ch.Ack(msg.DeliveryTag, false)
	}
	ct := msg.ContentType
	if ct == "" {
		ct = "application/octet-stream"
	}
	return PeekResult{
		Queue:       name,
		Empty:       false,
		Body:        string(msg.Body),
		ContentType: ct,
		Requeued:    requeued,
	}, nil
}
