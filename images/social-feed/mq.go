package main

import (
	"context"
	"fmt"

	"github.com/ywacch/cloud9/pkg/templateapp/jobs/queue"
)

type MQPublisher struct {
	q   queue.Queue
	rmq *queue.RabbitMQ
}

func NewMQPublisher(host, port, queueName string) (*MQPublisher, error) {
	rmq, err := queue.NewRabbitMQ(queue.RabbitMQOptions{
		Host:      host,
		Port:      port,
		QueueName: queueName,
	})
	if err != nil {
		return nil, err
	}
	return &MQPublisher{q: rmq, rmq: rmq}, nil
}

func (mq *MQPublisher) Publish(ctx context.Context, body []byte) error {
	return mq.q.Publish(ctx, body)
}

func (mq *MQPublisher) Close() {
	if mq.rmq != nil {
		_ = mq.rmq.Close()
	}
}

func (mq *MQPublisher) Ping() error {
	if mq.rmq == nil {
		return fmt.Errorf("mq publisher closed")
	}
	return mq.rmq.Ping()
}
