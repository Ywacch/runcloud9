package api

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestRequireAuth_MissingBearer_Unauthorized(t *testing.T) {
	t.Setenv("SUPABASE_JWT_SECRET", "test-secret")
	t.Setenv("SUPABASE_URL", "https://example.supabase.co")
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
	t.Setenv("SUPABASE_URL", "https://example.supabase.co")
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

func TestRequireAuth_QueryTokenFallback_SetsUserID(t *testing.T) {
	t.Setenv("SUPABASE_JWT_SECRET", "test-secret")
	t.Setenv("SUPABASE_URL", "https://example.supabase.co")
	t.Setenv("ENV", "production")

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": "user-456",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	signed, err := token.SignedString([]byte("test-secret"))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := userIDFromContext(r.Context()); got != "user-456" {
			t.Fatalf("user id: got %q", got)
		}
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/deployments?token="+signed, nil)
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

func TestRequireAuth_JWKSOnlyRejectsHS256(t *testing.T) {
	t.Setenv("SUPABASE_JWT_SECRET", "test-secret")
	t.Setenv("SUPABASE_URL", "https://example.supabase.co")
	t.Setenv("ENV", "production")
	t.Setenv("JWT_VERIFY_MODE", "jwks_only")

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": "user-123",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	signed, err := token.SignedString([]byte("test-secret"))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/deployments", nil)
	req.Header.Set("Authorization", "Bearer "+signed)
	rec := httptest.NewRecorder()
	requireAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status: want 401, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestRequireAuth_HS256OnlyRejectsES256(t *testing.T) {
	t.Setenv("SUPABASE_JWT_SECRET", "test-secret")
	t.Setenv("ENV", "production")
	t.Setenv("JWT_VERIFY_MODE", "hs256_only")

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate ecdsa key: %v", err)
	}
	kid := "kid-hs-only"

	// Even with a valid JWKS endpoint, ES256 should be rejected by mode.
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
		_ = json.NewEncoder(w).Encode(jwks)
	}))
	defer jwksServer.Close()
	t.Setenv("SUPABASE_URL", jwksServer.URL)

	token := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.MapClaims{
		"sub": "user-123",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	token.Header["kid"] = kid
	signed, err := token.SignedString(priv)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/deployments", nil)
	req.Header.Set("Authorization", "Bearer "+signed)
	rec := httptest.NewRecorder()
	requireAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status: want 401, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestGetSupabaseJWK_RefreshesOnUnknownKid(t *testing.T) {
	privOld, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate old key: %v", err)
	}
	privNew, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate new key: %v", err)
	}

	oldKid := "kid-old"
	newKid := "kid-new"
	requestCount := 0
	jwksServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.Header().Set("Content-Type", "application/json")
		var body map[string]any
		if requestCount == 1 {
			body = map[string]any{
				"keys": []map[string]any{
					{
						"kid": oldKid,
						"kty": "EC",
						"crv": "P-256",
						"x":   base64.RawURLEncoding.EncodeToString(privOld.PublicKey.X.Bytes()),
						"y":   base64.RawURLEncoding.EncodeToString(privOld.PublicKey.Y.Bytes()),
					},
				},
			}
		} else {
			body = map[string]any{
				"keys": []map[string]any{
					{
						"kid": newKid,
						"kty": "EC",
						"crv": "P-256",
						"x":   base64.RawURLEncoding.EncodeToString(privNew.PublicKey.X.Bytes()),
						"y":   base64.RawURLEncoding.EncodeToString(privNew.PublicKey.Y.Bytes()),
					},
				},
			}
		}
		if err := json.NewEncoder(w).Encode(body); err != nil {
			t.Fatalf("encode jwks: %v", err)
		}
	}))
	defer jwksServer.Close()

	// Prime cache with first response containing old kid only.
	supabaseJWKSCache.mu.Lock()
	supabaseJWKSCache.keys = nil
	supabaseJWKSCache.fetchedAt = time.Time{}
	supabaseJWKSCache.mu.Unlock()
	_, err = supabaseJWKSCache.getKeys(context.Background(), jwksServer.URL)
	if err != nil {
		t.Fatalf("prime getKeys: %v", err)
	}
	if requestCount != 1 {
		t.Fatalf("expected first jwks fetch count=1, got %d", requestCount)
	}

	key, err := getSupabaseJWK(context.Background(), jwksServer.URL, newKid)
	if err != nil {
		t.Fatalf("getSupabaseJWK with refresh: %v", err)
	}
	if key == nil {
		t.Fatal("expected non-nil key")
	}
	if requestCount < 2 {
		t.Fatalf("expected refresh fetch for unknown kid, fetch count=%d", requestCount)
	}
}

func TestJWTVerifyMode_InvalidFallsBackHybrid(t *testing.T) {
	t.Setenv("JWT_VERIFY_MODE", "unknown-mode")
	if got := jwtVerifyMode(); got != jwtVerifyModeHybrid {
		t.Fatalf("jwtVerifyMode fallback: got %q", got)
	}
}

func TestValidJWTAlgs_ByMode(t *testing.T) {
	tests := []struct {
		mode string
		want []string
	}{
		{mode: jwtVerifyModeHybrid, want: []string{jwt.SigningMethodHS256.Alg(), jwt.SigningMethodES256.Alg()}},
		{mode: jwtVerifyModeJWKSOnly, want: []string{jwt.SigningMethodES256.Alg()}},
		{mode: jwtVerifyModeHS256, want: []string{jwt.SigningMethodHS256.Alg()}},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("mode=%s", tt.mode), func(t *testing.T) {
			got := validJWTAlgs(tt.mode)
			if len(got) != len(tt.want) {
				t.Fatalf("len: got %d want %d", len(got), len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("alg[%d]: got %q want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}
