// Duplicate across url-shortener/social-feed/ecommerce. Collapse into pkg/templateapp/trace in PR3.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

type requestIDKey struct{}
type traceSampleKey struct{}

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

func setRequestIDHeader(req *http.Request, ctx context.Context) {
	if rid := requestIDFrom(ctx); rid != "" {
		req.Header.Set("X-Request-ID", rid)
	}
	if sampledFrom(ctx) {
		req.Header.Set("X-Trace-Sample", "1")
	}
}

func logDBOp(ctx context.Context, operation string, start time.Time) {
	if !shouldTrace(ctx) {
		return
	}
	rid := requestIDFrom(ctx)
	if rid == "" {
		return
	}
	slog.Info("db op", "request_id", rid, "op", "db_query", "operation", operation, "duration_ms", time.Since(start).Milliseconds())
}

func logCacheGet(ctx context.Context, key string, hit bool, start time.Time) {
	if !shouldTrace(ctx) {
		return
	}
	rid := requestIDFrom(ctx)
	if rid == "" {
		return
	}
	slog.Info("cache op", "request_id", rid, "op", "cache_get", "key", key, "hit", hit, "duration_ms", time.Since(start).Milliseconds())
}

func logCacheSet(ctx context.Context, key string, start time.Time) {
	if !shouldTrace(ctx) {
		return
	}
	rid := requestIDFrom(ctx)
	if rid == "" {
		return
	}
	slog.Info("cache op", "request_id", rid, "op", "cache_set", "key", key, "duration_ms", time.Since(start).Milliseconds())
}
