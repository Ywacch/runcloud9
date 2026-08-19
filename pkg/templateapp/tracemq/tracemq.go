// Package tracemq provides AMQP trace header helpers for template app images.
package tracemq

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/ywacch/cloud9/pkg/templateapp/trace"
)

func headerString(h amqp.Table, key string) string {
	if h == nil {
		return ""
	}
	v, ok := h[key]
	if !ok || v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	default:
		return fmt.Sprint(t)
	}
}

// ContextFromDelivery extracts trace context from an AMQP delivery.
func ContextFromDelivery(base context.Context, d amqp.Delivery) context.Context {
	rid := headerString(d.Headers, "x-request-id")
	sampled := headerString(d.Headers, "x-trace-sample") == "1"
	return trace.WithIDs(base, rid, sampled)
}

// Headers builds AMQP headers from ctx for publishing.
func Headers(ctx context.Context) amqp.Table {
	h := amqp.Table{}
	if rid := trace.RequestID(ctx); rid != "" {
		h["x-request-id"] = rid
	}
	if trace.Sampled(ctx) {
		h["x-trace-sample"] = "1"
	}
	if len(h) == 0 {
		return nil
	}
	return h
}
