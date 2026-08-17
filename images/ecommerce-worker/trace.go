// Duplicate of template-app requestid helpers. Wave 2: collapse into pkg/templateapp/trace.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type requestIDKey struct{}
type traceSampleKey struct{}

func verboseLogsEnabled() bool {
	return os.Getenv("LOG_VERBOSE") == "1"
}

func requestIDFrom(ctx context.Context) string {
	v, _ := ctx.Value(requestIDKey{}).(string)
	return v
}

func sampledFrom(ctx context.Context) bool {
	v, _ := ctx.Value(traceSampleKey{}).(bool)
	return v
}

func shouldTrace(ctx context.Context) bool {
	return verboseLogsEnabled() || sampledFrom(ctx)
}

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

func ctxFromDelivery(base context.Context, d amqp.Delivery) context.Context {
	ctx := base
	if rid := headerString(d.Headers, "x-request-id"); rid != "" {
		ctx = context.WithValue(ctx, requestIDKey{}, rid)
	}
	if headerString(d.Headers, "x-trace-sample") == "1" {
		ctx = context.WithValue(ctx, traceSampleKey{}, true)
	}
	return ctx
}

func logWorkerOp(ctx context.Context, op string, start time.Time) {
	if !shouldTrace(ctx) {
		return
	}
	rid := requestIDFrom(ctx)
	if rid == "" {
		return
	}
	slog.Info("worker op", "request_id", rid, "op", op, "duration_ms", time.Since(start).Milliseconds())
}
