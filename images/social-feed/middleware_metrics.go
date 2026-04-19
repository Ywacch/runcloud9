package main

import (
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

func withMetrics(pathPattern string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		slog.Info("request handled",
			"method", r.Method,
			"path", r.URL.Path,
			"route", pathPattern,
			"status", rw.status,
			"duration_ms", time.Since(start).Milliseconds(),
		)
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
