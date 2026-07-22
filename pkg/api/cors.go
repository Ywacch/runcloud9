package api

import (
	"net/http"
	"os"
	"strings"
)

const defaultAllowedOrigin = "http://localhost:3000"

// AllowedOriginsFromEnv parses ALLOWED_ORIGINS (comma-separated).
// Empty env defaults to http://localhost:3000.
func AllowedOriginsFromEnv() map[string]bool {
	raw := strings.TrimSpace(os.Getenv("ALLOWED_ORIGINS"))
	if raw == "" {
		raw = defaultAllowedOrigin
	}
	allowed := make(map[string]bool)
	for _, origin := range strings.Split(raw, ",") {
		o := strings.TrimSpace(origin)
		if o != "" {
			allowed[o] = true
		}
	}
	return allowed
}

// applyCORSAllowlist sets CORS response headers. Access-Control-Allow-Origin is
// only set when the request Origin is in allowed (never "*").
func applyCORSAllowlist(w http.ResponseWriter, r *http.Request, allowed map[string]bool, methods string) {
	origin := r.Header.Get("Origin")
	if origin != "" && allowed[origin] {
		w.Header().Set("Access-Control-Allow-Origin", origin)
	}
	w.Header().Set("Access-Control-Allow-Methods", methods)
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Request-ID")
	w.Header().Set("Access-Control-Expose-Headers", "X-Request-ID, X-Cloud9-Proxied")
}

// WithCORS allows browser calls from ALLOWED_ORIGINS and handles OPTIONS preflight.
func WithCORS(next http.Handler) http.Handler {
	allowed := AllowedOriginsFromEnv()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		applyCORSAllowlist(w, r, allowed, "GET, POST, DELETE, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
