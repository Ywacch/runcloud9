package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestProxyRequestID_preservesClientHeader(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Request-ID", "trace-abc-123")
	if got := proxyRequestID(req); got != "trace-abc-123" {
		t.Fatalf("got %q, want trace-abc-123", got)
	}
}

func TestProxyRequestID_generatesWhenMissing(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	got := proxyRequestID(req)
	if _, err := uuid.Parse(got); err != nil {
		t.Fatalf("expected UUID, got %q: %v", got, err)
	}
}

func TestProxyRESTHeaders_includesRequestID(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest(http.MethodPost, "/shorten", nil)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Request-ID", "persona-trace-1")

	h := proxyRESTHeaders(req, proxyRequestID(req))
	if h["X-Request-ID"] != "persona-trace-1" {
		t.Fatalf("X-Request-ID: got %q", h["X-Request-ID"])
	}
	if h["Content-Type"] != "application/json" {
		t.Fatalf("Content-Type: got %q", h["Content-Type"])
	}
	if h["Accept"] != "application/json" {
		t.Fatalf("Accept: got %q", h["Accept"])
	}
}

func TestSetProxyCORS_exposesRequestID(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	setProxyCORS(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	expose := rec.Header().Get("Access-Control-Expose-Headers")
	if expose == "" || !containsHeaderToken(expose, "X-Request-ID") {
		t.Fatalf("Access-Control-Expose-Headers: got %q", expose)
	}
	allow := rec.Header().Get("Access-Control-Allow-Headers")
	if !containsHeaderToken(allow, "X-Request-ID") {
		t.Fatalf("Access-Control-Allow-Headers: got %q", allow)
	}
}

func containsHeaderToken(list, token string) bool {
	for _, part := range strings.Split(list, ",") {
		if strings.TrimSpace(part) == token {
			return true
		}
	}
	return false
}

func TestHandleProxy_OPTIONS_echoesClientRequestID(t *testing.T) {
	t.Setenv("SUPABASE_JWT_SECRET", "")
	t.Setenv("SUPABASE_URL", "")
	t.Setenv("ENV", "development")

	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodOptions, "/deployments/ns-1/proxy/app-server", nil)
	req.Header.Set("X-Request-ID", "opts-trace-99")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status: want 204, got %d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("X-Request-ID"); got != "opts-trace-99" {
		t.Fatalf("X-Request-ID: got %q, want opts-trace-99", got)
	}
}

func TestHandleProxy_OPTIONS_generatesRequestIDWhenMissing(t *testing.T) {
	t.Setenv("SUPABASE_JWT_SECRET", "")
	t.Setenv("SUPABASE_URL", "")
	t.Setenv("ENV", "development")

	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodOptions, "/deployments/ns-1/proxy/app-server", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status: want 204, got %d body=%s", rec.Code, rec.Body.String())
	}
	got := rec.Header().Get("X-Request-ID")
	if got == "" {
		t.Fatal("expected X-Request-ID response header")
	}
	if _, err := uuid.Parse(got); err != nil {
		t.Fatalf("expected UUID X-Request-ID, got %q: %v", got, err)
	}
}
