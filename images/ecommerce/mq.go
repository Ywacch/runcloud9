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

func newMQPublisher(q queue.Queue) *MQPublisher {
	p := &MQPublisher{q: q}
	if rmq, ok := q.(*queue.RabbitMQ); ok {
		p.rmq = rmq
	}
	return p
}

func (mq *MQPublisher) Publish(ctx context.Context, body []byte) error {
	return mq.q.Publish(ctx, body)
}

func (mq *MQPublisher) Close() {
	if mq.rmq != nil {
		_ = mq.rmq.Close()
	}
}

func (mq *MQPublisher) Ping(ctx context.Context) error {
	if mq.q == nil {
		return fmt.Errorf("mq publisher closed")
	}
	switch q := mq.q.(type) {
	case *queue.RabbitMQ:
		return q.Ping()
	case *queue.Streams:
		return q.Ping(ctx)
	default:
		return fmt.Errorf("mq publisher unsupported engine %q", mq.q.Engine())
	}
}
