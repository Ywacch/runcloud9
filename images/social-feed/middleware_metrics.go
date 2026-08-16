package main

import (
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

func withMetrics(pathPattern string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = withRequestContext(r)
		start := time.Now()
		rw := &statusResponseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		duration := time.Since(start).Seconds()

		httpRequestsTotal.WithLabelValues(
			r.Method,
			pathPattern,
			strconv.Itoa(rw.status),
		).Inc()

		httpRequestDuration.WithLabelValues(r.Method, pathPattern).Observe(duration)
		attrs := []any{
			"method", r.Method,
			"path", r.URL.Path,
			"route", pathPattern,
			"status", rw.status,
			"duration_ms", time.Since(start).Milliseconds(),
		}
		if rid := r.Header.Get("X-Request-ID"); rid != "" {
			attrs = append(attrs, "request_id", rid)
		}
		if verboseLogsEnabled() || r.Header.Get("X-Trace-Sample") == "1" {
			slog.Info("request handled", attrs...)
		}
	})
}

type statusResponseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *statusResponseWriter) WriteHeader(status int) {
	rw.status = status
	rw.ResponseWriter.WriteHeader(status)
}
