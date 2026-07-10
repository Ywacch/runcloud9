package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestHealthLiveness verifies /health/live is dependency-free: 200 on GET without touching
// Redis or Postgres, and 405 on non-GET. handleReady's GET path pings Redis, so only its
// method guard is exercised here (a live Redis client is an integration concern).
func TestHealthLiveness(t *testing.T) {
	t.Parallel()
	s := &Server{}

	cases := []struct {
		name       string
		handler    http.HandlerFunc
		method     string
		wantStatus int
	}{
		{"live GET ok", s.handleLive, http.MethodGet, http.StatusOK},
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
