package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/time/rate"
)

func TestNoopRequestLimiter_AlwaysAllows(t *testing.T) {
	t.Parallel()
	var lim noopRequestLimiter
	allowed, retry := lim.Allow(context.Background(), "user:abc", LimitClassStandard)
	if !allowed || retry != 0 {
		t.Fatalf("want allow with no retry, got allowed=%v retry=%v", allowed, retry)
	}
}

func TestMemoryRequestLimiter_StandardBurst(t *testing.T) {
	t.Parallel()
	lim := newMemoryRequestLimiter(rateLimitConfig{
		standardRPS:    rate.Limit(1000),
		standardBurst:  2,
		expensiveRPS:   rate.Limit(1000),
		expensiveBurst: 10,
		publicRPS:      rate.Limit(1000),
		publicBurst:    10,
	})

	key := "user:test"
	for i := 0; i < 2; i++ {
		allowed, _ := lim.Allow(context.Background(), key, LimitClassStandard)
		if !allowed {
			t.Fatalf("request %d: expected allowed within burst", i+1)
		}
	}
	allowed, retry := lim.Allow(context.Background(), key, LimitClassStandard)
	if allowed {
		t.Fatal("expected third request to be rate limited")
	}
	if retry <= 0 {
		t.Fatalf("expected positive retry-after, got %v", retry)
	}
}

func TestMemoryRequestLimiter_ReleaseSeparateFromStandard(t *testing.T) {
	t.Parallel()
	lim := newMemoryRequestLimiter(rateLimitConfig{
		standardRPS:   rate.Limit(0.001),
		standardBurst: 1,
		releaseRPS:    rate.Limit(100),
		releaseBurst:  4,
	})
	key := "user:release"
	if allowed, _ := lim.Allow(context.Background(), key, LimitClassStandard); !allowed {
		t.Fatal("standard burst should allow the first request")
	}
	if allowed, _ := lim.Allow(context.Background(), key, LimitClassStandard); allowed {
		t.Fatal("standard bucket should be exhausted")
	}
	for i := 0; i < 4; i++ {
		allowed, _ := lim.Allow(context.Background(), key, LimitClassRelease)
		if !allowed {
			t.Fatalf("release request %d: expected allowed", i+1)
		}
	}
	if allowed, _ := lim.Allow(context.Background(), key, LimitClassRelease); allowed {
		t.Fatal("release burst should be exhausted on the fifth request")
	}
}

func TestMemoryRequestLimiter_ExpensiveSeparateFromStandard(t *testing.T) {
	t.Parallel()
	lim := newMemoryRequestLimiter(rateLimitConfig{
		standardRPS:    rate.Limit(1000),
		standardBurst:  1,
		expensiveRPS:   rate.Limit(1000),
		expensiveBurst: 1,
		publicRPS:      rate.Limit(1000),
		publicBurst:    10,
	})

	key := "user:test"
	if allowed, _ := lim.Allow(context.Background(), key, LimitClassStandard); !allowed {
		t.Fatal("expected first standard request allowed")
	}
	if allowed, _ := lim.Allow(context.Background(), key, LimitClassExpensive); !allowed {
		t.Fatal("expected expensive bucket independent of exhausted standard bucket")
	}
}

func TestRateLimitMiddleware_Returns429(t *testing.T) {
	t.Parallel()
	lim := newMemoryRequestLimiter(rateLimitConfig{
		standardRPS:    rate.Limit(1),
		standardBurst:  1,
		expensiveRPS:   rate.Limit(1),
		expensiveBurst: 1,
		publicRPS:      rate.Limit(1),
		publicBurst:    1,
	})
	rl := newRateLimitMiddleware(lim)

	var hits int
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.WriteHeader(http.StatusOK)
	})
	handler := rl.wrap(LimitClassStandard, next)

	req := httptest.NewRequest(http.MethodGet, "/deployments", nil)
	req = req.WithContext(context.WithValue(req.Context(), userIDKey, "user-1"))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("first request: want 200, got %d", rec.Code)
	}

	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req)
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("second request: want 429, got %d body=%s", rec2.Code, rec2.Body.String())
	}
	if hits != 1 {
		t.Fatalf("handler should run once, got %d", hits)
	}
	if got := rec2.Header().Get("Retry-After"); got == "" {
		t.Fatal("expected Retry-After header on 429")
	}
}

func TestClientIP_XForwardedFor(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("X-Forwarded-For", "203.0.113.1, 10.0.0.1")
	req.RemoteAddr = "10.0.0.5:1234"
	if got := clientIP(req); got != "203.0.113.1" {
		t.Fatalf("clientIP: want 203.0.113.1, got %q", got)
	}
}

func TestRateLimitEnabled_DefaultByEnv(t *testing.T) {
	tests := []struct {
		name    string
		env     string
		enabled string
		want    bool
	}{
		{name: "production default on", env: "production", enabled: "", want: true},
		{name: "dev default off", env: "development", enabled: "", want: false},
		{name: "explicit true", env: "development", enabled: "true", want: true},
		{name: "explicit false", env: "production", enabled: "false", want: false},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("ENV", tt.env)
			t.Setenv("API_RATE_LIMIT_ENABLED", tt.enabled)
			if got := rateLimitEnabled(); got != tt.want {
				t.Fatalf("rateLimitEnabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewRequestLimiterFromEnv_DisabledUsesNoop(t *testing.T) {
	t.Setenv("API_RATE_LIMIT_ENABLED", "false")
	lim, err := NewRequestLimiterFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := lim.(noopRequestLimiter); !ok {
		t.Fatalf("want noopRequestLimiter, got %T", lim)
	}
}

func TestNewRequestLimiterFromEnv_RedisNotImplemented(t *testing.T) {
	t.Setenv("API_RATE_LIMIT_ENABLED", "true")
	t.Setenv("API_RATE_LIMIT_BACKEND", "redis")
	_, err := NewRequestLimiterFromEnv()
	if err == nil {
		t.Fatal("expected error for redis backend")
	}
}

func TestRouteAuth_OrderAuthBeforeRateLimit(t *testing.T) {
	lim := newMemoryRequestLimiter(rateLimitConfig{
		standardRPS:    rate.Limit(1),
		standardBurst:  1,
		expensiveRPS:   rate.Limit(1),
		expensiveBurst: 1,
		publicRPS:      rate.Limit(1),
		publicBurst:    1,
	})
	rl := newRateLimitMiddleware(lim)

	var called bool
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		if userIDFromContext(r.Context()) != "user-42" {
			t.Fatalf("user id not set before handler")
		}
		w.WriteHeader(http.StatusOK)
	})

	t.Setenv("SUPABASE_JWT_SECRET", "secret")
	t.Setenv("SUPABASE_URL", "https://example.supabase.co")
	t.Setenv("ENV", "production")

	// Missing auth should 401 without consuming rate limit budget on a keyed user.
	handler := routeAuth(requireAuth, rl, next)
	req := httptest.NewRequest(http.MethodGet, "/deployments", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", rec.Code)
	}
	if called {
		t.Fatal("handler should not run without auth")
	}

	// Authenticated request should use user key and succeed once.
	handler2 := routeAuth(requireAuth, rl, next)
	req2 := httptest.NewRequest(http.MethodGet, "/deployments", nil)
	req2.Header.Set("Authorization", "Bearer "+testHS256Token(t, "user-42"))
	rec2 := httptest.NewRecorder()
	handler2.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Fatalf("want 200, got %d body=%s", rec2.Code, rec2.Body.String())
	}
}

func testHS256Token(t *testing.T, sub string) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": sub,
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	signed, err := token.SignedString([]byte("secret"))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return signed
}
