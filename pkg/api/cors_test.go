package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAllowedOriginsFromEnv_Default(t *testing.T) {
	t.Setenv("ALLOWED_ORIGINS", "")
	got := AllowedOriginsFromEnv()
	if !got[defaultAllowedOrigin] {
		t.Fatalf("want default %q in set, got %#v", defaultAllowedOrigin, got)
	}
}

func TestAllowedOriginsFromEnv_ParsesList(t *testing.T) {
	t.Setenv("ALLOWED_ORIGINS", "https://a.example, https://b.example")
	got := AllowedOriginsFromEnv()
	if !got["https://a.example"] || !got["https://b.example"] {
		t.Fatalf("got %#v", got)
	}
	if got["https://evil.example"] {
		t.Fatal("unexpected origin allowed")
	}
}

func TestApplyCORSAllowlist_OnlyAllowlistedOrigin(t *testing.T) {
	t.Parallel()
	allowed := map[string]bool{"https://app.example": true}

	t.Run("allowed", func(t *testing.T) {
		t.Parallel()
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		req.Header.Set("Origin", "https://app.example")
		applyCORSAllowlist(rec, req, allowed, "GET, OPTIONS")
		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example" {
			t.Fatalf("Allow-Origin = %q", got)
		}
	})

	t.Run("disallowed", func(t *testing.T) {
		t.Parallel()
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		req.Header.Set("Origin", "https://evil.example")
		applyCORSAllowlist(rec, req, allowed, "GET, OPTIONS")
		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
			t.Fatalf("Allow-Origin = %q, want empty", got)
		}
	})
}

func TestWithSecurityHeaders(t *testing.T) {
	t.Parallel()
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	h := WithSecurityHeaders(inner)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("X-Content-Type-Options = %q", got)
	}
	if got := rec.Header().Get("Referrer-Policy"); got != "strict-origin-when-cross-origin" {
		t.Fatalf("Referrer-Policy = %q", got)
	}
	if got := rec.Header().Get("X-Frame-Options"); got != "DENY" {
		t.Fatalf("X-Frame-Options = %q", got)
	}
	if got := rec.Header().Get("Content-Security-Policy"); got != "default-src 'none'; frame-ancestors 'none'" {
		t.Fatalf("CSP = %q", got)
	}
}

func TestWithCORS_OPTIONS_NoContent(t *testing.T) {
	t.Setenv("ALLOWED_ORIGINS", "https://app.example")
	innerCalled := false
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		innerCalled = true
	})
	h := WithCORS(inner)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/deployments", nil)
	req.Header.Set("Origin", "https://app.example")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d", rec.Code)
	}
	if innerCalled {
		t.Fatal("OPTIONS should not reach inner handler")
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example" {
		t.Fatalf("Allow-Origin = %q", got)
	}
	if got := rec.Header().Get("Access-Control-Allow-Methods"); got != "GET, POST, PATCH, DELETE, OPTIONS" {
		t.Fatalf("Allow-Methods = %q", got)
	}
}
