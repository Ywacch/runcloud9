package api

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
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
	t.Setenv("SUPABASE_URL", "")
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

func TestRequireAuth_ValidES256TokenWithJWKS_SetsUserID(t *testing.T) {
	t.Setenv("SUPABASE_JWT_SECRET", "")
	t.Setenv("ENV", "production")

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate ecdsa key: %v", err)
	}

	kid := "kid-1"
	jwks := map[string]any{
		"keys": []map[string]any{
			{
				"kid": kid,
				"kty": "EC",
				"crv": "P-256",
				"x":   base64.RawURLEncoding.EncodeToString(priv.PublicKey.X.Bytes()),
				"y":   base64.RawURLEncoding.EncodeToString(priv.PublicKey.Y.Bytes()),
			},
		},
	}
	jwksServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != supabaseJWKSPath {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(jwks); err != nil {
			t.Fatalf("encode jwks response: %v", err)
		}
	}))
	defer jwksServer.Close()
	t.Setenv("SUPABASE_URL", jwksServer.URL)

	// Reset cache to avoid cross-test contamination.
	supabaseJWKSCache.mu.Lock()
	supabaseJWKSCache.keys = nil
	supabaseJWKSCache.fetchedAt = time.Time{}
	supabaseJWKSCache.mu.Unlock()

	token := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.MapClaims{
		"sub": "cd9fbd72-fb5b-4ef5-affe-a591d5e73c29",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	token.Header["kid"] = kid
	signed, err := token.SignedString(priv)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := userIDFromContext(r.Context()); got != "cd9fbd72-fb5b-4ef5-affe-a591d5e73c29" {
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
