package api

import (
	"context"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// LimitClass selects which token bucket applies to a request.
type LimitClass string

const (
	LimitClassStandard  LimitClass = "standard"
	LimitClassExpensive LimitClass = "expensive"
	LimitClassPublic    LimitClass = "public"
)

// RequestLimiter decides whether a request is within rate limits.
// Keys must be stable across API replicas (for example user:<sub> or ip:<addr>).
// A Redis-backed implementation can satisfy the same interface when replicas > 1.
type RequestLimiter interface {
	Allow(ctx context.Context, key string, class LimitClass) (allowed bool, retryAfter time.Duration)
}

// noopRequestLimiter allows every request. Used when rate limiting is disabled.
type noopRequestLimiter struct{}

// Allow implements RequestLimiter.
func (noopRequestLimiter) Allow(context.Context, string, LimitClass) (bool, time.Duration) {
	return true, 0
}

type rateLimitConfig struct {
	standardRPS   rate.Limit
	standardBurst int
	expensiveRPS  rate.Limit
	expensiveBurst int
	publicRPS     rate.Limit
	publicBurst   int
}

type memoryRequestLimiter struct {
	mu       sync.Mutex
	limiters map[string]*rate.Limiter
	cfg      rateLimitConfig
}

// newMemoryRequestLimiter returns an in-process limiter (correct at replicas=1).
func newMemoryRequestLimiter(cfg rateLimitConfig) *memoryRequestLimiter {
	return &memoryRequestLimiter{
		limiters: make(map[string]*rate.Limiter),
		cfg:      cfg,
	}
}

// Allow implements RequestLimiter.
func (m *memoryRequestLimiter) Allow(_ context.Context, key string, class LimitClass) (bool, time.Duration) {
	if strings.TrimSpace(key) == "" {
		return true, 0
	}
	lim := m.limiter(key, class)
	if lim.Allow() {
		return true, 0
	}
	reservation := lim.Reserve()
	delay := reservation.Delay()
	reservation.Cancel()
	if delay <= 0 {
		delay = time.Second
	}
	return false, delay
}

func (m *memoryRequestLimiter) limiter(key string, class LimitClass) *rate.Limiter {
	mapKey := string(class) + ":" + key
	rps, burst := m.limitsFor(class)

	m.mu.Lock()
	defer m.mu.Unlock()
	lim, ok := m.limiters[mapKey]
	if !ok {
		lim = rate.NewLimiter(rps, burst)
		m.limiters[mapKey] = lim
	}
	return lim
}

func (m *memoryRequestLimiter) limitsFor(class LimitClass) (rate.Limit, int) {
	switch class {
	case LimitClassExpensive:
		return m.cfg.expensiveRPS, m.cfg.expensiveBurst
	case LimitClassPublic:
		return m.cfg.publicRPS, m.cfg.publicBurst
	default:
		return m.cfg.standardRPS, m.cfg.standardBurst
	}
}

// NewRequestLimiterFromEnv builds the configured RequestLimiter.
// API_RATE_LIMIT_BACKEND=redis is reserved for a future shared store.
func NewRequestLimiterFromEnv() (RequestLimiter, error) {
	if !rateLimitEnabled() {
		return noopRequestLimiter{}, nil
	}
	backend := strings.ToLower(strings.TrimSpace(os.Getenv("API_RATE_LIMIT_BACKEND")))
	switch backend {
	case "", "memory":
		cfg := rateLimitConfigFromEnv()
		log.Printf("API rate limiting enabled (backend=memory, per-pod buckets)")
		return newMemoryRequestLimiter(cfg), nil
	case "redis":
		return nil, fmt.Errorf("API_RATE_LIMIT_BACKEND=redis is not implemented yet; use memory or disable with API_RATE_LIMIT_ENABLED=false")
	default:
		return nil, fmt.Errorf("unknown API_RATE_LIMIT_BACKEND %q", backend)
	}
}

func rateLimitEnabled() bool {
	raw := strings.TrimSpace(os.Getenv("API_RATE_LIMIT_ENABLED"))
	if raw != "" {
		enabled, err := strconv.ParseBool(raw)
		if err != nil {
			log.Printf("invalid API_RATE_LIMIT_ENABLED %q, using default", raw)
		} else {
			return enabled
		}
	}
	return strings.EqualFold(strings.TrimSpace(os.Getenv("ENV")), "production")
}

func rateLimitConfigFromEnv() rateLimitConfig {
	return rateLimitConfig{
		standardRPS:    envRateLimit("API_RATE_LIMIT_RPS", 30),
		standardBurst:  envPositiveInt("API_RATE_LIMIT_BURST", 60),
		expensiveRPS:   envRPM("API_RATE_LIMIT_EXPENSIVE_RPM", 6),
		expensiveBurst: envPositiveInt("API_RATE_LIMIT_EXPENSIVE_BURST", 3),
		publicRPS:      envRateLimit("API_RATE_LIMIT_PUBLIC_RPS", 60),
		publicBurst:    envPositiveInt("API_RATE_LIMIT_PUBLIC_BURST", 120),
	}
}

func envRateLimit(name string, defaultRPS float64) rate.Limit {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return rate.Limit(defaultRPS)
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil || v <= 0 {
		log.Printf("invalid %s %q, using default %.0f", name, raw, defaultRPS)
		return rate.Limit(defaultRPS)
	}
	return rate.Limit(v)
}

func envRPM(name string, defaultRPM float64) rate.Limit {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return rate.Limit(defaultRPM / 60)
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil || v <= 0 {
		log.Printf("invalid %s %q, using default %.0f rpm", name, raw, defaultRPM)
		return rate.Limit(defaultRPM / 60)
	}
	return rate.Limit(v / 60)
}

func envPositiveInt(name string, defaultVal int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return defaultVal
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 1 {
		log.Printf("invalid %s %q, using default %d", name, raw, defaultVal)
		return defaultVal
	}
	return v
}

type rateLimitMiddleware struct {
	limiter RequestLimiter
}

func newRateLimitMiddleware(limiter RequestLimiter) *rateLimitMiddleware {
	return &rateLimitMiddleware{limiter: limiter}
}

func (m *rateLimitMiddleware) wrap(class LimitClass, next http.Handler) http.Handler {
	if m == nil || m.limiter == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := rateLimitKey(r, class)
		allowed, retryAfter := m.limiter.Allow(r.Context(), key, class)
		if allowed {
			next.ServeHTTP(w, r)
			return
		}
		if retryAfter > 0 {
			secs := int(math.Ceil(retryAfter.Seconds()))
			if secs < 1 {
				secs = 1
			}
			w.Header().Set("Retry-After", strconv.Itoa(secs))
		}
		writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
	})
}

func rateLimitKey(r *http.Request, class LimitClass) string {
	if class == LimitClassPublic {
		return "ip:" + clientIP(r)
	}
	userID := userIDFromContext(r.Context())
	if userID == "" {
		userID = "anonymous"
	}
	return "user:" + userID
}

// clientIP returns the best-effort client address behind a reverse proxy.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		if ip := strings.TrimSpace(parts[0]); ip != "" {
			return ip
		}
	}
	if xri := strings.TrimSpace(r.Header.Get("X-Real-IP")); xri != "" {
		return xri
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// routeAuth applies JWT auth then per-user rate limits (standard class).
func routeAuth(auth func(http.Handler) http.Handler, rl *rateLimitMiddleware, next http.Handler) http.Handler {
	return auth(rl.wrap(LimitClassStandard, next))
}

// routeAuthExpensive applies JWT auth then stricter per-user limits.
func routeAuthExpensive(auth func(http.Handler) http.Handler, rl *rateLimitMiddleware, next http.Handler) http.Handler {
	return auth(rl.wrap(LimitClassExpensive, next))
}

// routePublic applies per-IP limits without auth.
func routePublic(rl *rateLimitMiddleware, next http.Handler) http.Handler {
	return rl.wrap(LimitClassPublic, next)
}
