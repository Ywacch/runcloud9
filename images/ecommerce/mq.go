package main

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type MQPublisher struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   string
}

func NewMQPublisher(host, port, queueName string) (*MQPublisher, error) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://cloud9:cloud9@%s:%s/", host, port))
	if err != nil {
		return nil, fmt.Errorf("mq dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("mq channel: %w", err)
	}
	_, err = ch.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("mq queue declare: %w", err)
	}
	return &MQPublisher{conn: conn, channel: ch, queue: queueName}, nil
}

func (mq *MQPublisher) Publish(ctx context.Context, body []byte) error {
	return mq.channel.PublishWithContext(ctx, "", mq.queue, false, false, amqp.Publishing{
		ContentType: "application/json",
		Headers:     amqpTraceHeaders(ctx),
		Body:        body,
	})
}

func amqpTraceHeaders(ctx context.Context) amqp.Table {
	h := amqp.Table{}
	if rid := requestIDFrom(ctx); rid != "" {
		h["x-request-id"] = rid
	}
	if sampledFrom(ctx) {
		h["x-trace-sample"] = "1"
	}
	if len(h) == 0 {
		return nil
	}
	return h
}

func (mq *MQPublisher) Close() {
	if mq.channel != nil {
		_ = mq.channel.Close()
	}
	if mq.conn != nil {
		_ = mq.conn.Close()
	}
}

func (mq *MQPublisher) Ping() error {
	if mq.conn == nil || mq.conn.IsClosed() {
		return fmt.Errorf("mq connection closed")
	}
	return nil
}
