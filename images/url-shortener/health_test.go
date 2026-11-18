package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMissingDBIsFatal(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		listenOnly bool
		cacheMode  string
		dbHost     string
		want       bool
	}{
		{name: "redis without host", cacheMode: "redis", want: true},
		{name: "proxy without host", cacheMode: "proxy", want: false},
		{name: "listen only without host", listenOnly: true, cacheMode: "redis", want: false},
		{name: "redis with host", cacheMode: "redis", dbHost: "db", want: false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := missingDBIsFatal(tc.listenOnly, tc.cacheMode, tc.dbHost)
			if got != tc.want {
				t.Fatalf("missingDBIsFatal: got %v want %v", got, tc.want)
			}
		})
	}
}

func TestRoutes_ListenOnlyProbes(t *testing.T) {
	t.Parallel()
	s := &Server{cfg: Config{ListenOnly: true}}
	mux := s.routes()

	for _, path := range []string{"/health/live", "/health/ready"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: got %d want 200", path, rec.Code)
		}
	}

	req := httptest.NewRequest(http.MethodPost, "/shorten", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("/shorten: got %d want 404", rec.Code)
	}
}

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
