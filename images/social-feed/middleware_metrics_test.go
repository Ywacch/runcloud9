package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestWithMetrics_SkipsObserver(t *testing.T) {
	h := withMetrics("/threads", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	before := testutil.ToFloat64(httpRequestsTotal.WithLabelValues(http.MethodGet, "/threads", "200"))

	req := httptest.NewRequest(http.MethodGet, "/threads/1", nil)
	req.Header.Set("X-C9-Observer", "1")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	after := testutil.ToFloat64(httpRequestsTotal.WithLabelValues(http.MethodGet, "/threads", "200"))
	if after != before {
		t.Fatalf("counter moved from %v to %v", before, after)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
}
