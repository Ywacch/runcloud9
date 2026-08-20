package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/ywacch/cloud9/pkg/loadgen"
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

	h := proxyRESTHeaders(req, proxyRequestID(req), "owner-u1")
	if h["X-Request-ID"] != "persona-trace-1" {
		t.Fatalf("X-Request-ID: got %q", h["X-Request-ID"])
	}
	if h["X-Trace-Sample"] != "1" {
		t.Fatalf("X-Trace-Sample: got %q, want 1", h["X-Trace-Sample"])
	}
	wantXFF := make(http.Header)
	loadgen.ApplyClientIdentityHeaders(wantXFF, "owner-u1")
	if h["X-Forwarded-For"] != wantXFF.Get("X-Forwarded-For") {
		t.Fatalf("X-Forwarded-For: got %q want %q", h["X-Forwarded-For"], wantXFF.Get("X-Forwarded-For"))
	}
	if h["Content-Type"] != "application/json" {
		t.Fatalf("Content-Type: got %q", h["Content-Type"])
	}
	if h["Accept"] != "application/json" {
		t.Fatalf("Accept: got %q", h["Accept"])
	}
}

func TestSetProxyCORS_exposesRequestID(t *testing.T) {
	t.Setenv("ALLOWED_ORIGINS", "https://app.example")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://app.example")
	setProxyCORS(rec, req)
	expose := rec.Header().Get("Access-Control-Expose-Headers")
	if expose == "" || !containsHeaderToken(expose, "X-Request-ID") {
		t.Fatalf("Access-Control-Expose-Headers: got %q", expose)
	}
	allow := rec.Header().Get("Access-Control-Allow-Headers")
	if !containsHeaderToken(allow, "X-Request-ID") {
		t.Fatalf("Access-Control-Allow-Headers: got %q", allow)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example" {
		t.Fatalf("Allow-Origin: got %q", got)
	}
}

func TestSetProxyCORS_disallowedOriginOmitsAllowOrigin(t *testing.T) {
	t.Setenv("ALLOWED_ORIGINS", "https://app.example")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://evil.example")
	setProxyCORS(rec, req)
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("Allow-Origin: got %q, want empty", got)
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

func TestResolveProxyTarget_appServerViaLB(t *testing.T) {
	t.Parallel()
	designWithLB := map[string]any{
		"nodes": []any{
			map[string]any{"type": "lb", "config": map[string]any{}},
			map[string]any{"type": "app-server", "config": map[string]any{}},
		},
	}
	host, port, err := resolveProxyTarget("app-server", designWithLB)
	if err != nil {
		t.Fatal(err)
	}
	if host != "lb" || port != 80 {
		t.Fatalf("got %q:%d want lb:80", host, port)
	}

	host, port, err = resolveProxyTarget("app-server", map[string]any{
		"nodes": []any{map[string]any{"type": "app-server"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if host != "app-server" || port != 8080 {
		t.Fatalf("got %q:%d want app-server:8080", host, port)
	}

	host, port, err = resolveProxyTarget("cache", designWithLB)
	if err != nil {
		t.Fatal(err)
	}
	if host != "cache" {
		t.Fatalf("cache should not route via lb, got %q", host)
	}
}

func TestPlaygroundClientSeed(t *testing.T) {
	t.Parallel()
	if got := playgroundClientSeed("rid", "u1", "feed/42"); got != "42" {
		t.Fatalf("feed path: got %q want 42", got)
	}
	if got := playgroundClientSeed("rid", "u1", "users/7"); got != "7" {
		t.Fatalf("users path: got %q want 7", got)
	}
	if got := playgroundClientSeed("rid", "u1", "shorten"); got != "u1" {
		t.Fatalf("owner fallback: got %q want u1", got)
	}
	if got := playgroundClientSeed("rid-only", "", "shorten"); got != "rid-only" {
		t.Fatalf("requestID fallback: got %q want rid-only", got)
	}
	if got := playgroundPathUserID("user-pool"); got != "" {
		t.Fatalf("user-pool should not match, got %q", got)
	}
}

func TestResolveIntrospectRedisHost(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		design map[string]any
		want   string
	}{
		{
			name: "cache-aside uses cache service",
			design: map[string]any{
				"nodes": []any{
					map[string]any{"type": "cache", "config": map[string]any{"strategy": "cache-aside"}},
				},
			},
			want: "cache",
		},
		{
			name: "write-behind uses redis-internal",
			design: map[string]any{
				"nodes": []any{
					map[string]any{"type": "cache", "config": map[string]any{"strategy": "write-behind"}},
				},
			},
			want: "redis-internal",
		},
		{
			name: "write-through uses redis-internal",
			design: map[string]any{
				"nodes": []any{
					map[string]any{"type": "cache", "config": map[string]any{"strategy": "write-through"}},
				},
			},
			want: "redis-internal",
		},
		{
			name:   "nil design defaults to cache",
			design: nil,
			want:   "cache",
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := resolveIntrospectRedisHost(tc.design); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}
