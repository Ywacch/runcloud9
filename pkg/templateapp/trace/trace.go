// Package trace provides request tracing helpers shared by template app images.
package trace

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

type requestIDKey struct{}
type traceSampleKey struct{}
type clientIDKey struct{}

// VerboseEnabled reports whether LOG_VERBOSE=1.
func VerboseEnabled() bool {
	return os.Getenv("LOG_VERBOSE") == "1"
}

// ContextFromRequest reads X-Request-ID, X-Trace-Sample, and client identity from r into the request context.
func ContextFromRequest(r *http.Request) *http.Request {
	ctx := r.Context()
	if rid := r.Header.Get("X-Request-ID"); rid != "" {
		ctx = context.WithValue(ctx, requestIDKey{}, rid)
	}
	if r.Header.Get("X-Trace-Sample") == "1" {
		ctx = context.WithValue(ctx, traceSampleKey{}, true)
	}
	if clientID := clientIDFromRequest(r); clientID != "" {
		ctx = context.WithValue(ctx, clientIDKey{}, clientID)
	}
	return r.WithContext(ctx)
}

func clientIDFromRequest(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		if ip := strings.TrimSpace(parts[0]); ip != "" {
			return ip
		}
	}
	if xri := strings.TrimSpace(r.Header.Get("X-Real-IP")); xri != "" {
		return xri
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// WithIDs attaches request ID and sampling flag to ctx. Used by tracemq when consuming AMQP deliveries.
func WithIDs(ctx context.Context, requestID string, sampled bool) context.Context {
	if requestID != "" {
		ctx = context.WithValue(ctx, requestIDKey{}, requestID)
	}
	if sampled {
		ctx = context.WithValue(ctx, traceSampleKey{}, true)
	}
	return ctx
}

// RequestID returns the request ID from ctx, if any.
func RequestID(ctx context.Context) string {
	v, _ := ctx.Value(requestIDKey{}).(string)
	return v
}

// ClientID returns the sticky client identity from ctx, if any.
// Loadgen sets X-Forwarded-For per virtual user; nginx ip_hash pins that user to one app pod.
func ClientID(ctx context.Context) string {
	v, _ := ctx.Value(clientIDKey{}).(string)
	return v
}

// Sampled reports whether the request was marked for trace sampling.
func Sampled(ctx context.Context) bool {
	v, _ := ctx.Value(traceSampleKey{}).(bool)
	return v
}

// ShouldTrace reports whether verbose tracing logs should be emitted for ctx.
func ShouldTrace(ctx context.Context) bool {
	return VerboseEnabled() || Sampled(ctx)
}

// SetOutboundHeaders sets X-Request-ID and X-Trace-Sample on outbound HTTP requests from ctx.
func SetOutboundHeaders(req *http.Request, ctx context.Context) {
	if rid := RequestID(ctx); rid != "" {
		req.Header.Set("X-Request-ID", rid)
	}
	if Sampled(ctx) {
		req.Header.Set("X-Trace-Sample", "1")
	}
}

// LogDB logs a database operation when tracing is enabled.
func LogDB(ctx context.Context, operation string, start time.Time) {
	if !ShouldTrace(ctx) {
		return
	}
	rid := RequestID(ctx)
	if rid == "" {
		return
	}
	slog.Info("db op", "request_id", rid, "op", "db_query", "operation", operation, "duration_ms", time.Since(start).Milliseconds())
}

// LogCacheGet logs a cache get when tracing is enabled.
func LogCacheGet(ctx context.Context, key string, hit bool, start time.Time) {
	if !ShouldTrace(ctx) {
		return
	}
	rid := RequestID(ctx)
	if rid == "" {
		return
	}
	slog.Info("cache op", "request_id", rid, "op", "cache_get", "key", key, "hit", hit, "duration_ms", time.Since(start).Milliseconds())
}

// LogCacheSet logs a cache set when tracing is enabled.
func LogCacheSet(ctx context.Context, key string, start time.Time) {
	if !ShouldTrace(ctx) {
		return
	}
	rid := RequestID(ctx)
	if rid == "" {
		return
	}
	slog.Info("cache op", "request_id", rid, "op", "cache_set", "key", key, "duration_ms", time.Since(start).Milliseconds())
}

// LogCacheDelete logs a cache delete when tracing is enabled.
func LogCacheDelete(ctx context.Context, start time.Time) {
	if !ShouldTrace(ctx) {
		return
	}
	rid := RequestID(ctx)
	if rid == "" {
		return
	}
	slog.Info("cache op", "request_id", rid, "op", "cache_del", "duration_ms", time.Since(start).Milliseconds())
}

// LogProxy logs a caching-proxy operation when tracing is enabled.
func LogProxy(ctx context.Context, op, key string, start time.Time) {
	if !ShouldTrace(ctx) {
		return
	}
	rid := RequestID(ctx)
	if rid == "" {
		return
	}
	slog.Info("proxy op", "request_id", rid, "op", op, "key", key, "duration_ms", time.Since(start).Milliseconds())
}

// LogWorker logs a worker operation when tracing is enabled.
func LogWorker(ctx context.Context, op string, start time.Time) {
	if !ShouldTrace(ctx) {
		return
	}
	rid := RequestID(ctx)
	if rid == "" {
		return
	}
	slog.Info("worker op", "request_id", rid, "op", op, "duration_ms", time.Since(start).Milliseconds())
}
