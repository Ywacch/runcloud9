package api

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"github.com/ywacch/cloud9/pkg/store"
)

type ctxKey string

const userIDKey ctxKey = "userID"

// requireAuth validates Supabase JWTs (HS256) and stores the user id in request context.
// Local development can run without SUPABASE_JWT_SECRET unless ENV=production.
func requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secret := os.Getenv("SUPABASE_JWT_SECRET")
		if secret == "" {
			if os.Getenv("ENV") == "production" {
				writeError(w, http.StatusInternalServerError, "server auth misconfiguration")
				return
			}
			// Local dev fallback keeps existing workflows unblocked.
			r = r.WithContext(context.WithValue(r.Context(), userIDKey, "dev-user"))
			next.ServeHTTP(w, r)
			return
		}

		header := r.Header.Get("Authorization")
		if !strings.HasPrefix(header, "Bearer ") {
			writeError(w, http.StatusUnauthorized, "missing or invalid Authorization header")
			return
		}

		tokenStr := strings.TrimPrefix(header, "Bearer ")
		token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (any, error) {
			if t.Method.Alg() != jwt.SigningMethodHS256.Alg() {
				return nil, fmt.Errorf("unexpected signing method")
			}
			return []byte(secret), nil
		})
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
