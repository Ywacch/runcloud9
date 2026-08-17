// Duplicate of template-app requestid helpers. Wave 2: collapse into pkg/templateapp/trace.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"
)

type requestIDKey struct{}
type traceSampleKey struct{}

func verboseLogsEnabled() bool {
	return os.Getenv("LOG_VERBOSE") == "1"
}

func withRequestContext(r *http.Request) *http.Request {
	ctx := r.Context()
	if rid := r.Header.Get("X-Request-ID"); rid != "" {
		ctx = context.WithValue(ctx, requestIDKey{}, rid)
	}
	if r.Header.Get("X-Trace-Sample") == "1" {
		ctx = context.WithValue(ctx, traceSampleKey{}, true)
	}
	return r.WithContext(ctx)
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

func logProxyOp(ctx context.Context, op, key string, start time.Time) {
	if !shouldTrace(ctx) {
		return
	}
	rid := requestIDFrom(ctx)
	if rid == "" {
		return
	}
	slog.Info("proxy op", "request_id", rid, "op", op, "key", key, "duration_ms", time.Since(start).Milliseconds())
}
