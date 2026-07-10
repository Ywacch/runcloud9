package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestHealthLivenessReadiness verifies the split probe endpoints are dependency-free:
// they return 200 on GET without touching Redis, Postgres, or the caching-proxy, and
// reject non-GET methods. A zero-value Server is sufficient precisely because these
// handlers must not dereference any downstream client.
func TestHealthLivenessReadiness(t *testing.T) {
	t.Parallel()
	s := &Server{}

	cases := []struct {
		name       string
		handler    http.HandlerFunc
		method     string
		wantStatus int
	}{
		{"live GET ok", s.handleLive, http.MethodGet, http.StatusOK},
		{"ready GET ok", s.handleReady, http.MethodGet, http.StatusOK},
		{"live POST rejected", s.handleLive, http.MethodPost, http.StatusMethodNotAllowed},
		{"ready POST rejected", s.handleReady, http.MethodPost, http.StatusMethodNotAllowed},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(tc.method, "/health/x", nil)
			rec := httptest.NewRecorder()
			tc.handler(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status: got %d want %d", rec.Code, tc.wantStatus)
			}
		})
	}
}
