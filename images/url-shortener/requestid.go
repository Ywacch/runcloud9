package main

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

type requestIDKey struct{}

func withRequestContext(r *http.Request) *http.Request {
	if rid := r.Header.Get("X-Request-ID"); rid == "" {
		return r
	}
	return r.WithContext(context.WithValue(r.Context(), requestIDKey{}, r.Header.Get("X-Request-ID")))
}

func requestIDFrom(ctx context.Context) string {
	v, _ := ctx.Value(requestIDKey{}).(string)
	return v
}

func setRequestIDHeader(req *http.Request, ctx context.Context) {
	if rid := requestIDFrom(ctx); rid != "" {
		req.Header.Set("X-Request-ID", rid)
	}
}

func logDBOp(ctx context.Context, operation string, start time.Time) {
	rid := requestIDFrom(ctx)
	if rid == "" {
		return
	}
	slog.Info("db op", "request_id", rid, "op", "db_query", "operation", operation, "duration_ms", time.Since(start).Milliseconds())
}

func logCacheGet(ctx context.Context, key string, hit bool, start time.Time) {
	rid := requestIDFrom(ctx)
	if rid == "" {
		return
	}
	slog.Info("cache op", "request_id", rid, "op", "cache_get", "key", key, "hit", hit, "duration_ms", time.Since(start).Milliseconds())
}

func logCacheSet(ctx context.Context, key string, start time.Time) {
	rid := requestIDFrom(ctx)
	if rid == "" {
		return
	}
	slog.Info("cache op", "request_id", rid, "op", "cache_set", "key", key, "duration_ms", time.Since(start).Milliseconds())
}
