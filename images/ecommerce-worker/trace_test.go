package main

import (
	"context"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/ywacch/cloud9/pkg/templateapp/trace"
	"github.com/ywacch/cloud9/pkg/templateapp/tracemq"
)

func TestCtxFromDelivery_roundTrip(t *testing.T) {
	d := amqp.Delivery{Headers: amqp.Table{
		"x-request-id":   "rid-order",
		"x-trace-sample": "1",
	}}
	ctx := tracemq.ContextFromDelivery(context.Background(), d)
	if trace.RequestID(ctx) != "rid-order" {
		t.Fatalf("request id: %q", trace.RequestID(ctx))
	}
	if !trace.Sampled(ctx) {
		t.Fatal("expected sampled")
	}
}

func TestShouldTrace_sampleWithoutVerbose(t *testing.T) {
	t.Setenv("LOG_VERBOSE", "")
	d := amqp.Delivery{Headers: amqp.Table{"x-request-id": "rid", "x-trace-sample": "1"}}
	ctx := tracemq.ContextFromDelivery(context.Background(), d)
	if !trace.ShouldTrace(ctx) {
		t.Fatal("expected shouldTrace true")
	}
}

func TestShouldTrace_neither(t *testing.T) {
	t.Setenv("LOG_VERBOSE", "")
	d := amqp.Delivery{Headers: amqp.Table{"x-request-id": "rid"}}
	ctx := tracemq.ContextFromDelivery(context.Background(), d)
	if trace.ShouldTrace(ctx) {
		t.Fatal("expected shouldTrace false")
	}
}
