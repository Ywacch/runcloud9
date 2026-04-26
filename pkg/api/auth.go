package api

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/ywacch/cloud9/pkg/store"
)

type ctxKey string

const userIDKey ctxKey = "userID"

const (
	jwksCacheTTL     = 10 * time.Minute
	supabaseJWKSPath = "/auth/v1/.well-known/jwks.json"
)

type jwksResponse struct {
	Keys []jwkEntry `json:"keys"`
}

type jwkEntry struct {
	Kid string `json:"kid"`
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

type jwksCache struct {
	mu        sync.RWMutex
	keys      map[string]any
	fetchedAt time.Time
}

var supabaseJWKSCache = &jwksCache{}

// requireAuth validates Supabase JWTs and stores the user id in request context.
// Supports legacy HS256 (SUPABASE_JWT_SECRET) and ES256 (Supabase JWKS).
// Local development can run without auth configuration unless ENV=production.
func requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secret := strings.TrimSpace(os.Getenv("SUPABASE_JWT_SECRET"))
		supabaseURL := strings.TrimSpace(os.Getenv("SUPABASE_URL"))
		if secret == "" {
			if os.Getenv("ENV") != "production" {
				// Local dev fallback keeps existing workflows unblocked.
				r = r.WithContext(context.WithValue(r.Context(), userIDKey, "dev-user"))
				next.ServeHTTP(w, r)
				return
			}
			if supabaseURL == "" {
				writeError(w, http.StatusInternalServerError, "server auth misconfiguration")
				return
			}
		}

		header := r.Header.Get("Authorization")
		// Browser WebSocket API cannot set custom headers. For WS upgrades,
		// accept JWT from ?token= query parameter when Authorization is absent.
		if strings.TrimSpace(header) == "" {
			if qToken := r.URL.Query().Get("token"); qToken != "" {
				header = "Bearer " + qToken
			}
		}
		if os.Getenv("ENV") != "production" && strings.TrimSpace(header) == "" {
			// In non-production, missing auth headers should not block local workflows/tests.
			r = r.WithContext(context.WithValue(r.Context(), userIDKey, "dev-user"))
			next.ServeHTTP(w, r)
			return
		}
		if !strings.HasPrefix(header, "Bearer ") {
			writeError(w, http.StatusUnauthorized, "missing or invalid Authorization header")
			return
		}

		tokenStr := strings.TrimPrefix(header, "Bearer ")
		token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (any, error) {
			switch t.Method.Alg() {
			case jwt.SigningMethodHS256.Alg():
				if secret == "" {
					return nil, fmt.Errorf("hs256 secret is not configured")
				}
				return []byte(secret), nil
			case jwt.SigningMethodES256.Alg():
				return getSupabaseJWK(r.Context(), supabaseURL, t.Header["kid"])
			default:
				return nil, fmt.Errorf("unexpected signing method")
			}
		}, jwt.WithValidMethods([]string{
			jwt.SigningMethodHS256.Alg(),
			jwt.SigningMethodES256.Alg(),
		}))
		if err != nil || !token.Valid {
			writeError(w, http.StatusUnauthorized, "invalid token")
			return
		}

		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			writeError(w, http.StatusUnauthorized, "invalid claims")
			return
		}
		userID, _ := claims["sub"].(string)
		if strings.TrimSpace(userID) == "" {
			writeError(w, http.StatusUnauthorized, "invalid token claims")
			return
		}

		r = r.WithContext(context.WithValue(r.Context(), userIDKey, userID))
		next.ServeHTTP(w, r)
	})
}

func getSupabaseJWK(ctx context.Context, supabaseURL string, kidVal any) (any, error) {
	if supabaseURL == "" {
		return nil, fmt.Errorf("supabase url is not configured")
	}
	kid, _ := kidVal.(string)
	if strings.TrimSpace(kid) == "" {
		return nil, fmt.Errorf("missing kid in token header")
	}
	keys, err := supabaseJWKSCache.getKeys(ctx, supabaseURL)
	if err != nil {
		return nil, err
	}
	key, ok := keys[kid]
	if !ok {
		return nil, fmt.Errorf("jwk for kid %q not found", kid)
	}
	return key, nil
}

func (c *jwksCache) getKeys(ctx context.Context, supabaseURL string) (map[string]any, error) {
	now := time.Now()

	c.mu.RLock()
	if len(c.keys) > 0 && now.Sub(c.fetchedAt) < jwksCacheTTL {
		keys := c.keys
		c.mu.RUnlock()
		return keys, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	// Re-check after acquiring the write lock to avoid duplicate refreshes.
	if len(c.keys) > 0 && now.Sub(c.fetchedAt) < jwksCacheTTL {
		return c.keys, nil
	}

	keys, err := fetchSupabaseJWKS(ctx, supabaseURL)
	if err != nil {
		return nil, err
	}
	c.keys = keys
	c.fetchedAt = time.Now()
	return c.keys, nil
}

func fetchSupabaseJWKS(ctx context.Context, supabaseURL string) (map[string]any, error) {
	jwksURL := strings.TrimRight(supabaseURL, "/") + supabaseJWKSPath
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, jwksURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build jwks request: %w", err)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch jwks: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch jwks: unexpected status %d", resp.StatusCode)
	}

	var doc jwksResponse
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return nil, fmt.Errorf("decode jwks: %w", err)
	}

	keys := make(map[string]any, len(doc.Keys))
	for _, k := range doc.Keys {
		pub, err := parseP256PublicKey(k)
		if err != nil {
			continue
		}
		keys[k.Kid] = pub
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("jwks contains no valid keys")
	}
	return keys, nil
}

func parseP256PublicKey(k jwkEntry) (*ecdsa.PublicKey, error) {
	if strings.TrimSpace(k.Kid) == "" {
		return nil, fmt.Errorf("jwk kid is required")
	}
	if k.Kty != "EC" || k.Crv != "P-256" {
		return nil, fmt.Errorf("unsupported jwk type")
	}
	xBytes, err := base64.RawURLEncoding.DecodeString(k.X)
	if err != nil {
		return nil, fmt.Errorf("decode jwk x: %w", err)
	}
	yBytes, err := base64.RawURLEncoding.DecodeString(k.Y)
	if err != nil {
		return nil, fmt.Errorf("decode jwk y: %w", err)
	}
	pub := &ecdsa.PublicKey{
		Curve: elliptic.P256(),
		X:     new(big.Int).SetBytes(xBytes),
		Y:     new(big.Int).SetBytes(yBytes),
	}
	if !pub.Curve.IsOnCurve(pub.X, pub.Y) {
		return nil, fmt.Errorf("jwk point is not on curve")
	}
	return pub, nil
}

func userIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(userIDKey).(string)
	return id
}

// requireOwnership ensures the authenticated caller owns the target deployment.
// It writes an HTTP error and returns nil when access should be denied.
func (h *Handler) requireOwnership(w http.ResponseWriter, r *http.Request, namespace string) *store.DeploymentRow {
	if h.store == nil {
		writeError(w, http.StatusServiceUnavailable, "deployment store unavailable")
		return nil
	}
	row, err := h.store.GetDeployment(r.Context(), namespace)
	if err != nil {
		writeError(w, http.StatusNotFound, "deployment not found")
		return nil
	}
	callerID := userIDFromContext(r.Context())
	if row.UserID != "" && row.UserID != callerID {
		writeError(w, http.StatusForbidden, "you do not own this deployment")
		return nil
	}
	return &row
}
