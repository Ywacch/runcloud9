package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestRequireAuth_MissingBearer_Unauthorized(t *testing.T) {
	t.Setenv("SUPABASE_JWT_SECRET", "test-secret")
	t.Setenv("ENV", "production")

	var called bool
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/deployments", nil)
	rec := httptest.NewRecorder()
	requireAuth(next).ServeHTTP(rec, req)

	if called {
		t.Fatal("next handler should not be called")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status: want 401, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestRequireAuth_ValidToken_SetsUserID(t *testing.T) {
	t.Setenv("SUPABASE_JWT_SECRET", "test-secret")
	t.Setenv("ENV", "production")

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": "user-123",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	signed, err := token.SignedString([]byte("test-secret"))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := userIDFromContext(r.Context()); got != "user-123" {
			t.Fatalf("user id: got %q", got)
		}
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/deployments", nil)
	req.Header.Set("Authorization", "Bearer "+signed)
	rec := httptest.NewRecorder()
	requireAuth(next).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestRequireAuth_LocalDevFallback_InjectsDevUser(t *testing.T) {
	t.Setenv("SUPABASE_JWT_SECRET", "")
	t.Setenv("ENV", "development")

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := userIDFromContext(r.Context()); got != "dev-user" {
			t.Fatalf("user id: got %q", got)
		}
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/deployments", nil)
	rec := httptest.NewRecorder()
	requireAuth(next).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
}
