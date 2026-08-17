package main

import (
	"context"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestCtxFromDelivery_roundTrip(t *testing.T) {
	d := amqp.Delivery{Headers: amqp.Table{
		"x-request-id":   "rid-order",
		"x-trace-sample": "1",
	}}
	ctx := ctxFromDelivery(context.Background(), d)
	if requestIDFrom(ctx) != "rid-order" {
		t.Fatalf("request id: %q", requestIDFrom(ctx))
	}
	if !sampledFrom(ctx) {
		t.Fatal("expected sampled")
	}
}

func TestShouldTrace_sampleWithoutVerbose(t *testing.T) {
	t.Setenv("LOG_VERBOSE", "")
	d := amqp.Delivery{Headers: amqp.Table{"x-request-id": "rid", "x-trace-sample": "1"}}
	ctx := ctxFromDelivery(context.Background(), d)
	if !shouldTrace(ctx) {
		t.Fatal("expected shouldTrace true")
	}
}

func TestShouldTrace_neither(t *testing.T) {
	t.Setenv("LOG_VERBOSE", "")
	d := amqp.Delivery{Headers: amqp.Table{"x-request-id": "rid"}}
	ctx := ctxFromDelivery(context.Background(), d)
	if shouldTrace(ctx) {
		t.Fatal("expected shouldTrace false")
	}
}
