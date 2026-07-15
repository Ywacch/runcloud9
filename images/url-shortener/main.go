package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

type Config struct {
	DBHost        string
	DBPort        string
	DBName        string
	DBUser        string
	DBPassword    string
	CacheHost     string
	CachePort     string
	CacheStrategy string
	CacheMode     string
	ProxyPort     string
	CacheTTL      time.Duration
	Stampede      bool
	Port          string
}

type Server struct {
	db    *DB
	cache *Cache // nil if CACHE_HOST empty
	proxy *ProxyClient
	cfg   Config

	cacheHits   atomic.Int64
	cacheMisses atomic.Int64
}

func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func loadConfig() Config {
	cacheMode := os.Getenv("CACHE_MODE")
	if cacheMode == "" {
		cacheMode = "redis"
	}

	ttlSecs, _ := strconv.Atoi(os.Getenv("CACHE_TTL"))
	if ttlSecs == 0 {
		ttlSecs = 300
	}

	strategy := os.Getenv("CACHE_STRATEGY")
	if strategy == "" {
		strategy = "cache-aside"
	}

	dbHost := os.Getenv("DB_HOST")
	if cacheMode != "proxy" && dbHost == "" {
		// UNDERSTAND: DB_HOST is required — every URL shortener deployment has Postgres in redis mode.
		// In proxy mode the caching-proxy owns DB access.
		slog.Error("DB_HOST missing")
		os.Exit(1)
	}

	return Config{
		DBHost:        dbHost,
		DBPort:        envOrDefault("DB_PORT", "5432"),
		DBName:        envOrDefault("DB_NAME", "syscrack"),
		DBUser:        envOrDefault("DB_USER", "syscrack"),
		DBPassword:    envOrDefault("DB_PASSWORD", "syscrack"),
		CacheHost:     os.Getenv("CACHE_HOST"),
		CachePort:     envOrDefault("CACHE_PORT", "6379"),
		CacheStrategy: strategy,
		CacheMode:     cacheMode,
		ProxyPort:     envOrDefault("CACHE_PROXY_PORT", "8081"),
		CacheTTL:      time.Duration(ttlSecs) * time.Second,
		Stampede:      os.Getenv("STAMPEDE_PROTECTION") == "true",
		Port:          envOrDefault("PORT", "8080"),
	}
}

func jsonError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func validateShortenURL(raw string) error {
	if raw == "" {
		return errors.New("url is required")
	}
	if len(raw) > 2048 {
		return errors.New("url exceeds maximum length of 2048 characters")
	}
	u, err := url.ParseRequestURI(raw)
	if err != nil {
		return errors.New("url is not a valid absolute URI")
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
	default:
		return errors.New("url scheme must be http or https")
	}
	if u.Host == "" {
		return errors.New("url must include a host")
	}
	return nil
}

// Never panic — we recover and return JSON error.
func withRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				slog.Error("panic recovered", "error", rec)
				jsonError(w, http.StatusInternalServerError, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) routes() *http.ServeMux {
	mux := http.NewServeMux()

	mux.Handle("/shorten", withRecovery(withMetrics("/shorten", http.HandlerFunc(s.handleShorten))))
	mux.Handle("/r/", withRecovery(withMetrics("/r", http.HandlerFunc(s.handleRedirect))))
	mux.Handle("/stats/", withRecovery(withMetrics("/stats", http.HandlerFunc(s.handleStats))))
	mux.Handle("/health", withRecovery(http.HandlerFunc(s.handleHealth)))
	mux.Handle("/health/live", withRecovery(http.HandlerFunc(s.handleLive)))
	mux.Handle("/health/ready", withRecovery(http.HandlerFunc(s.handleReady)))
	mux.Handle("/cache-stats", withRecovery(http.HandlerFunc(s.handleCacheStats)))
	mux.Handle("/code-pool", withRecovery(http.HandlerFunc(s.handleCodePool)))

	// promhttp.Handler() serves the Prometheus exposition format
	// that Prometheus scrapes. This is separate from /cache-stats (our JSON endpoint).
	mux.Handle("/metrics", promhttp.Handler())

	return mux
}

func (s *Server) handleShorten(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	req.URL = strings.TrimSpace(req.URL)
	if err := validateShortenURL(req.URL); err != nil {
		jsonError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx := r.Context()
	var code string
	var err error
	if s.cfg.CacheMode == "proxy" {
		code, err = s.allocateCodeProxy(ctx, req.URL)
	} else {
		code, err = s.allocateCodeDB(ctx, req.URL)
	}
	if err != nil {
		slog.Error("allocate short code failed", "error", err)
		jsonError(w, http.StatusInternalServerError, "failed to generate code")
		return
	}

	if s.cfg.CacheMode == "proxy" {
		writeJSON(w, http.StatusCreated, map[string]string{
			"code":      code,
			"short_url": "/r/" + code,
		})
		return
	}

	// Cache-aside (redis mode): durable row already inserted in allocateCodeDB.
	// Redis warms on GET miss. Write-through / write-behind only apply in proxy mode.
	writeJSON(w, http.StatusCreated, map[string]string{
		"code":      code,
		"short_url": "/r/" + code,
	})
}

func (s *Server) handleRedirect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	code := strings.TrimPrefix(r.URL.Path, "/r/")
	code = strings.Trim(code, "/")
	if code == "" {
		jsonError(w, http.StatusNotFound, "not found")
		return
	}

	ctx := r.Context()

	if s.cfg.CacheMode == "proxy" {
		val, source, ok, err := s.proxy.Get(ctx, code)
		if err != nil {
			slog.Error("proxy GET failed", "error", err)
			jsonError(w, http.StatusInternalServerError, "failed to fetch url")
			return
		}
		if !ok {
			jsonError(w, http.StatusNotFound, "not found")
			return
		}

		if source == "cache" {
			s.cacheHits.Add(1)
			cacheHitsTotal.Inc()
		} else {
			s.cacheMisses.Add(1)
			cacheMissesTotal.Inc()
		}

		writeJSON(w, http.StatusOK, map[string]string{
			"code":         code,
			"original_url": val,
		})
		return
	}

	// Cache enabled?
	if s.cache != nil {
		// Fast-path: plain Get to differentiate hit vs miss.
		if val, ok := s.cache.Get(ctx, code); ok {
			s.cacheHits.Add(1)
			cacheHitsTotal.Inc()
			writeJSON(w, http.StatusOK, map[string]string{
				"code":         code,
				"original_url": val,
			})
			return
		}

		s.cacheMisses.Add(1)
		cacheMissesTotal.Inc()
		fetchFn := func() (string, error) {
			rec, err := s.db.GetURL(ctx, code)
			if err != nil {
				return "", err
			}
			return rec.Original, nil
		}

		val, err := s.cache.GetProtected(ctx, code, fetchFn)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				jsonError(w, http.StatusNotFound, "not found")
				return
			}
			slog.Error("redirect fetch failed", "error", err)
			jsonError(w, http.StatusInternalServerError, "failed to fetch url")
			return
		}

		// Populate cache with original only.
		if err := s.cache.Set(ctx, code, val, s.cfg.CacheTTL); err != nil {
			slog.Warn("cache set after miss failed", "error", err)
		}

		writeJSON(w, http.StatusOK, map[string]string{
			"code":         code,
			"original_url": val,
		})
		return
	}

	// Cache disabled: read from DB.
	rec, err := s.db.GetURL(ctx, code)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			jsonError(w, http.StatusNotFound, "not found")
			return
		}
		slog.Error("redirect db read failed", "error", err)
		jsonError(w, http.StatusInternalServerError, "failed to fetch url")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"code":         code,
		"original_url": rec.Original,
	})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	code := strings.TrimPrefix(r.URL.Path, "/stats/")
	code = strings.Trim(code, "/")
	if code == "" {
		jsonError(w, http.StatusNotFound, "not found")
		return
	}

	ctx := r.Context()

	if s.cfg.CacheMode == "proxy" {
		rec, ok, err := s.proxy.Stats(ctx, code)
		if err != nil {
			slog.Error("proxy STATS failed", "error", err)
			jsonError(w, http.StatusInternalServerError, "failed to fetch stats")
			return
		}
		if !ok {
			jsonError(w, http.StatusNotFound, "not found")
			return
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"code":         rec.Code,
			"original_url": rec.Original,
			"created_at":   rec.CreatedAt,
		})
		return
	}

	// UNDERSTAND: /stats bypasses cache and reads authoritative row data from Postgres.
	rec, err := s.db.GetURL(ctx, code)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			jsonError(w, http.StatusNotFound, "not found")
			return
		}
		slog.Error("stats db read failed", "error", err)
		jsonError(w, http.StatusInternalServerError, "failed to fetch stats")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"code":         rec.Code,
		"original_url": rec.Original,
		"created_at":   rec.CreatedAt,
	})
}

// handleCodePool returns a sample of existing short codes for the load generator.
// UNDERSTAND: the load generator calls this once on startup to get real codes
// from the seeded database. These codes will produce cache hits/misses and
// DB lookups — much more realistic than random codes that always 404.
func (s *Server) handleCodePool(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.db == nil || s.db.pool == nil {
		if s.proxy != nil {
			codes, err := s.proxy.Codes(r.Context())
			if err != nil {
				slog.Warn("proxy CODES failed", "error", err)
				writeJSON(w, http.StatusOK, map[string]any{"codes": []string{}})
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"codes": codes})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"codes": []string{}})
		return
	}
	const limit = 200
	rows, err := s.db.pool.Query(r.Context(),
		`SELECT code FROM urls ORDER BY code LIMIT $1`, limit)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "failed to fetch codes")
		return
	}
	defer rows.Close()

	var codes []string
	for rows.Next() {
		var code string
		if err := rows.Scan(&code); err != nil {
			continue
		}
		codes = append(codes, code)
	}
	writeJSON(w, http.StatusOK, map[string]any{"codes": codes})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	ctx := r.Context()

	if s.cfg.CacheMode == "proxy" {
		_, redis, db, err := s.proxy.Health(ctx)
		if err != nil {
			slog.Error("proxy health failed", "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{
				"status": "error",
				"db":     "unreachable",
			})
			return
		}
		_ = redis
		_ = db
		writeJSON(w, http.StatusOK, map[string]string{
			"status": "ok",
			"db":     "ok",
			"cache":  "proxy",
		})
		return
	}

	// DB health.
	var n int
	if err := s.db.pool.QueryRow(ctx, "SELECT 1").Scan(&n); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"status": "error",
			"db":     "unreachable",
		})
		return
	}

	cacheHealth := "disabled"
	if s.cache != nil {
		if err := s.cache.client.Ping(ctx).Err(); err != nil {
			cacheHealth = "error"
		} else {
			cacheHealth = "ok"
		}
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
		"db":     "ok",
		"cache":  cacheHealth,
	})
}

// handleLive is the Kubernetes liveness endpoint. It reports only that the
// process is up and serving HTTP; it never touches Redis, Postgres, or the
// caching-proxy. Keeping it dependency-free means a slow or briefly-unavailable
// downstream can never trigger a container restart storm.
func (s *Server) handleLive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReady is the Kubernetes readiness endpoint. It is intentionally
// dependency-free: the app-server can accept traffic as soon as it is listening.
// Chaining downstream checks here (DB SELECT, proxy /health) is what previously
// flipped every pod NotReady under load and left the load balancer with no
// backends, amplifying failures. Deep diagnostics remain at GET /health.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) handleCacheStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	hits := s.cacheHits.Load()
	misses := s.cacheMisses.Load()
	total := hits + misses

	hitRate := float64(0)
	if total > 0 {
		hitRate = float64(hits) / float64(total)
	}

	strategy := s.cfg.CacheStrategy
	if s.cfg.CacheMode == "proxy" {
		strategy = "proxy"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"cache_hits":          hits,
		"cache_misses":        misses,
		"hit_rate":            hitRate,
		"strategy":            strategy,
		"stampede_protection": s.cfg.Stampede,
	})
}

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func generateCode() string {
	// generate 6 chars using crypto-rand so we don't depend on math/rand seeding.
	b := make([]byte, 6)
	for i := range b {
		n, _ := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		b[i] = charset[n.Int64()]
	}
	return string(b)
}

// allocateCodeProxy reserves a code via the caching-proxy (Exists + Set), retrying on collisions.
func (s *Server) allocateCodeProxy(ctx context.Context, longURL string) (string, error) {
	for attempt := 1; attempt <= 5; attempt++ {
		code := generateCode()
		exists, err := s.proxy.Exists(ctx, code)
		if err != nil {
			return "", err
		}
		if exists {
			shortenerCodeCollisionsTotal.Inc()
			slog.Warn("short code collision", "mode", "proxy", "attempt", attempt, "code", code)
			continue
		}
		if err := s.proxy.Set(ctx, code, longURL, s.cfg.CacheTTL); err != nil {
			return "", err
		}
		return code, nil
	}
	return "", fmt.Errorf("failed to generate unique code after 5 attempts")
}

// allocateCodeDB inserts with ON CONFLICT DO NOTHING and retries when the row was not inserted.
func (s *Server) allocateCodeDB(ctx context.Context, longURL string) (string, error) {
	for attempt := 1; attempt <= 5; attempt++ {
		code := generateCode()
		inserted, err := s.db.InsertURL(ctx, code, longURL)
		if err != nil {
			return "", err
		}
		if inserted {
			return code, nil
		}
		shortenerCodeCollisionsTotal.Inc()
		slog.Warn("short code collision", "mode", "db", "attempt", attempt, "code", code)
	}
	return "", fmt.Errorf("failed to generate unique code after 5 attempts")
}

func newServer(ctx context.Context, cfg Config) (*Server, error) {
	if cfg.CacheMode == "proxy" {
		if cfg.CacheHost == "" {
			return nil, fmt.Errorf("CACHE_HOST is required in proxy mode")
		}
		s := &Server{
			cfg:   cfg,
			proxy: NewProxyClient(cfg.CacheHost, cfg.ProxyPort),
		}
		return s, nil
	}

	connURL := &url.URL{
		Scheme: "postgres",
		Host:   fmt.Sprintf("%s:%s", cfg.DBHost, cfg.DBPort),
		Path:   "/" + cfg.DBName,
		User:   url.UserPassword(cfg.DBUser, cfg.DBPassword),
	}
	connString := connURL.String()

	poolCfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("parse db pool config: %w", err)
	}
	maxConns, _ := strconv.Atoi(envOrDefault("DB_POOL_MAX_CONNS", "25"))
	if maxConns < 1 {
		maxConns = 25
	}
	poolCfg.MaxConns = int32(maxConns)

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, err
	}

	// Create schema.
	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS urls (
			code        VARCHAR(8) PRIMARY KEY,
			original    TEXT NOT NULL,
			created_at  TIMESTAMPTZ DEFAULT NOW()
		);
	`)
	if err != nil {
		pool.Close()
		return nil, err
	}

	s := &Server{
		db:  newDB(pool),
		cfg: cfg,
	}

	// Optional cache.
	if cfg.CacheHost != "" {
		client := redis.NewClient(&redis.Options{
			Addr: fmt.Sprintf("%s:%s", cfg.CacheHost, cfg.CachePort),
		})
		// Best-effort ping: cache errors are never fatal.
		if err := client.Ping(ctx).Err(); err != nil {
			slog.Warn("redis ping failed; continuing with cache disabled", "error", err)
			// treat as disabled
		} else {
			s.cache = newCache(client, cfg.Stampede)
		}
	}

	return s, nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	slog.Info("url-shortener starting")
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	s, err := newServer(ctx, cfg)
	if err != nil {
		slog.Error("newServer failed", "error", err)
		os.Exit(1)
	}
	if s.db != nil && s.db.pool != nil {
		defer s.db.pool.Close()
	}

	srv := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: s.routes(),
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	slog.Info("listening", "port", cfg.Port)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}
