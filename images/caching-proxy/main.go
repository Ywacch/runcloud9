package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

type Config struct {
	DBHost     string
	DBPort     string
	DBName     string
	DBUser     string
	DBPassword string
	RedisHost  string
	RedisPort  string
	Strategy   string

	CacheTTL            time.Duration
	WriteBehindInterval time.Duration
	Port                string
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("required env missing", "key", key)
		os.Exit(1)
	}
	return v
}

func loadConfig() Config {
	dbHost := mustEnv("DB_HOST")
	redisHost := mustEnv("REDIS_HOST")
	strategy := os.Getenv("STRATEGY")
	if strategy == "" {
		slog.Error("required env missing", "key", "STRATEGY")
		os.Exit(1)
	}

	ttlSecs, _ := strconv.Atoi(envOrDefault("CACHE_TTL", "300"))
	wbSecs, _ := strconv.Atoi(envOrDefault("WRITE_BEHIND_INTERVAL", "30"))

	return Config{
		DBHost:              dbHost,
		DBPort:              envOrDefault("DB_PORT", "5432"),
		DBName:              envOrDefault("DB_NAME", "syscrack"),
		DBUser:              envOrDefault("DB_USER", "syscrack"),
		DBPassword:          envOrDefault("DB_PASSWORD", "syscrack"),
		RedisHost:           redisHost,
		RedisPort:           envOrDefault("REDIS_PORT", "6379"),
		Strategy:            strategy,
		CacheTTL:            time.Duration(ttlSecs) * time.Second,
		WriteBehindInterval: time.Duration(wbSecs) * time.Second,
		Port:                envOrDefault("PORT", "8081"),
	}
}

type Server struct {
	cfg   Config
	db    *DB
	redis *redis.Client
	rules []compiledProxyRule
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

func (s *Server) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/get/", withRecovery(s.handleGet))       // GET /get/{code}
	mux.Handle("/set/", withRecovery(s.handleSet))       // POST /set/{code}
	mux.Handle("/del/", withRecovery(s.handleDel))       // DELETE /del/{code}
	mux.Handle("/exists/", withRecovery(s.handleExists)) // GET /exists/{code}
	mux.Handle("/stats/", withRecovery(s.handleStats))   // GET /stats/{code}
	mux.Handle("/codes", withRecovery(s.handleCodes))
	mux.Handle("/health", withRecovery(s.handleHealth))
	mux.Handle("/health/live", withRecovery(s.handleLive))
	mux.Handle("/health/ready", withRecovery(s.handleReady))
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

func withRecovery(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				slog.Error("panic recovered", "error", rec)
				jsonError(w, http.StatusInternalServerError, "internal server error")
			}
		}()
		next(w, r)
	}
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	code := strings.TrimPrefix(r.URL.Path, "/get/")
	code = strings.Trim(code, "/")
	if code == "" {
		jsonError(w, http.StatusNotFound, "not found")
		return
	}

	ctx := r.Context()
	val, err := s.redis.Get(ctx, code).Result()
	if err == nil {
		writeJSON(w, http.StatusOK, map[string]string{"value": val, "source": "cache"})
		return
	}
	if !errors.Is(err, redis.Nil) {
		slog.Warn("redis GET failed", "code", code, "error", err)
		// treat as miss -> DB fallback depends on strategy
	}

	// Cache miss: template-driven DB read-through (queries.yaml mounted at deploy time).
	payload, dbErr := proxyReadThrough(ctx, s.db.pool, s.rules, code)
	if errors.Is(dbErr, errNoMatchingProxyRule) {
		jsonError(w, http.StatusNotFound, "no matching cache key rule")
		return
	}
	if errors.Is(dbErr, errBadCapture) {
		jsonError(w, http.StatusBadRequest, "invalid capture for key")
		return
	}
	if errors.Is(dbErr, pgx.ErrNoRows) {
		jsonError(w, http.StatusNotFound, "not found")
		return
	}
	if dbErr != nil {
		slog.Error("proxy read-through query failed", "code", code, "error", dbErr)
		jsonError(w, http.StatusBadGateway, "db read failed")
		return
	}

	if err := s.redis.Set(ctx, code, payload, s.cfg.CacheTTL).Err(); err != nil {
		slog.Warn("redis SET after miss failed", "code", code, "error", err)
	}

	writeJSON(w, http.StatusOK, map[string]string{"value": payload, "source": "db"})
}

type setReq struct {
	Value string `json:"value"`
	TTL   int    `json:"ttl"`
}

func (s *Server) handleSet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	code := strings.TrimPrefix(r.URL.Path, "/set/")
	code = strings.Trim(code, "/")
	if code == "" {
		jsonError(w, http.StatusBadRequest, "missing code")
		return
	}

	var req setReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, http.StatusBadRequest, "invalid json")
		return
	}
	req.Value = strings.TrimSpace(req.Value)
	if req.Value == "" {
		jsonError(w, http.StatusBadRequest, "value is required")
		return
	}

	ttl := s.cfg.CacheTTL
	if req.TTL > 0 {
		ttl = time.Duration(req.TTL) * time.Second
	}

	ctx := r.Context()

	switch s.cfg.Strategy {
	case "write-through", "read-through":
		// URLs table persistence only accepts valid URLs (url-shortener). Opaque bodies go to Redis-only paths.
		if _, err := url.ParseRequestURI(req.Value); err != nil {
			jsonError(w, http.StatusBadRequest, "value is not a valid url")
			return
		}
		// Synchronously persist to DB.
		if err := s.db.SaveURL(ctx, code, req.Value); err != nil {
			slog.Error("db SaveURL failed (write-through)", "code", code, "error", err)
			jsonError(w, http.StatusInternalServerError, "db write failed")
			return
		}
		if err := s.redis.Set(ctx, code, req.Value, ttl).Err(); err != nil {
			slog.Warn("redis SET failed", "code", code, "error", err)
		}
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	case "write-behind":
		// Opaque JSON cache populate (ecommerce/feeds): no URL validation.
		if err := s.redis.Set(ctx, code, req.Value, ttl).Err(); err != nil {
			slog.Warn("redis SET failed", "code", code, "error", err)
		}
		// Store pending write under wb:{code}.
		if err := s.redis.Set(ctx, "wb:"+code, req.Value, ttl).Err(); err != nil {
			slog.Warn("redis SET wb failed", "code", code, "error", err)
		}
		// DB write happens in background.
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	default:
		jsonError(w, http.StatusBadRequest, "unknown STRATEGY")
	}
}

func (s *Server) handleDel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	code := strings.TrimPrefix(r.URL.Path, "/del/")
	code = strings.Trim(code, "/")
	if code == "" {
		jsonError(w, http.StatusBadRequest, "missing code")
		return
	}

	ctx := r.Context()
	if err := s.redis.Del(ctx, code).Err(); err != nil {
		slog.Warn("redis DEL failed", "code", code, "error", err)
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) handleExists(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	code := strings.TrimPrefix(r.URL.Path, "/exists/")
	code = strings.Trim(code, "/")
	if code == "" {
		jsonError(w, http.StatusBadRequest, "missing code")
		return
	}

	ctx := r.Context()
	ok, err := s.db.URLExists(ctx, code)
	if err != nil {
		slog.Error("db URLExists failed", "code", code, "error", err)
		jsonError(w, http.StatusInternalServerError, "db read failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"exists": ok})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	code := strings.TrimPrefix(r.URL.Path, "/stats/")
	code = strings.Trim(code, "/")
	if code == "" {
		jsonError(w, http.StatusBadRequest, "missing code")
		return
	}

	ctx := r.Context()
	rec, err := s.db.GetURL(ctx, code)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			jsonError(w, http.StatusNotFound, "not found")
			return
		}
		slog.Error("db GetURL failed (stats)", "code", code, "error", err)
		jsonError(w, http.StatusInternalServerError, "db read failed")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"code":         rec.Code,
		"original_url": rec.Original,
		"created_at":   rec.CreatedAt,
	})
}

func (s *Server) handleCodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
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

	if err := s.redis.Ping(ctx).Err(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"status": "error",
			"redis":  "unreachable",
		})
		return
	}

	var n int
	if err := s.db.pool.QueryRow(ctx, "SELECT 1").Scan(&n); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"status": "error",
			"db":     "unreachable",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
		"redis":  "ok",
		"db":     "ok",
	})
}

// handleLive is the Kubernetes liveness endpoint. It reports only that the
// process is up and serving HTTP; it never touches Redis or Postgres. Keeping
// it dependency-free means a slow or briefly-unavailable backing store can
// never trigger a container restart storm.
func (s *Server) handleLive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReady is the Kubernetes readiness endpoint. It pings only the backing
// Redis (fast, in-namespace) with a tight timeout. The Postgres check stays in
// GET /health: a slow DB must not remove the proxy from rotation, since that
// would cascade into app-server readiness failures and LB connection pile-up.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if err := s.redis.Ping(ctx).Err(); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "unready",
			"redis":  "unreachable",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) writeBehindFlusher(ctx context.Context) {
	interval := s.cfg.WriteBehindInterval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.flushPendingWrites(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) flushPendingWrites(ctx context.Context) {
	var cursor uint64
	for {
		keys, nextCursor, err := s.redis.Scan(ctx, cursor, "wb:*", 100).Result()
		if err != nil {
			slog.Error("write-behind scan failed", "error", err)
			return
		}
		for _, key := range keys {
			code := strings.TrimPrefix(key, "wb:")
			original, err := s.redis.Get(ctx, key).Result()
			if err != nil {
				continue
			}
			if err := s.db.SaveURL(ctx, code, original); err != nil {
				slog.Error("write-behind flush failed", "code", code, "error", err)
				continue
			}
			_ = s.redis.Del(ctx, key).Err()
		}
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
}

func main() {
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	// Postgres.
	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s",
		cfg.DBUser,
		cfg.DBPassword,
		cfg.DBHost,
		cfg.DBPort,
		cfg.DBName,
	)
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		slog.Error("pgxpool connect failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	db := newDB(pool)
	// Create schema.
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS urls (
			code        VARCHAR(8) PRIMARY KEY,
			original    TEXT NOT NULL,
			created_at  TIMESTAMPTZ DEFAULT NOW()
		);
	`); err != nil {
		slog.Error("create urls table failed", "error", err)
		os.Exit(1)
	}

	// Redis.
	redisClient := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%s", cfg.RedisHost, cfg.RedisPort),
	})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		slog.Error("redis ping failed", "error", err)
		os.Exit(1)
	}

	queryPath := envOrDefault("PROXY_QUERIES_FILE", "/etc/proxy/queries.yaml")
	rules := mustLoadCompiledProxyRules(queryPath)

	s := &Server{
		cfg:   cfg,
		db:    db,
		redis: redisClient,
		rules: rules,
	}

	// Write-behind background flusher.
	if cfg.Strategy == "write-behind" {
		go s.writeBehindFlusher(ctx)
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

	slog.Info("caching-proxy listening", "port", cfg.Port, "strategy", cfg.Strategy)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}
