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
	"sync/atomic"
	"syscall"
	"time"

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

	CacheHost     string
	CachePort     string
	CacheMode     string
	ProxyPort     string
	CacheStrategy string
	CacheTTL      time.Duration
	Stampede      bool

	MQHost string
	MQPort string

	ServiceRole   string
	InventoryMode string

	Port string
}

type Server struct {
	db    *DB
	cache *Cache
	proxy *ProxyClient
	mq    *MQPublisher
	cfg   Config

	cacheHits   atomic.Int64
	cacheMisses atomic.Int64
}

type checkoutRequest struct {
	ProductID int `json:"product_id"`
	Quantity  int `json:"quantity"`
}

func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func loadConfig() Config {
	dbHost := os.Getenv("DB_HOST")
	if dbHost == "" {
		slog.Error("DB_HOST missing")
		os.Exit(1)
	}

	cacheTTL := 60
	if v := os.Getenv("CATALOG_CACHE_TTL"); v != "" {
		if sec, err := strconv.Atoi(v); err == nil && sec > 0 {
			cacheTTL = sec
		}
	} else if v := os.Getenv("CACHE_TTL"); v != "" {
		if sec, err := strconv.Atoi(v); err == nil && sec > 0 {
			cacheTTL = sec
		}
	}

	cacheStrategy := os.Getenv("CACHE_STRATEGY")
	if cacheStrategy == "" {
		cacheStrategy = "cache-aside"
	}
	cacheMode := os.Getenv("CACHE_MODE")
	if cacheMode == "" {
		cacheMode = "redis"
	}

	role := strings.TrimSpace(os.Getenv("SERVICE_ROLE"))
	if role == "monolith" {
		role = ""
	}
	inventory := strings.TrimSpace(os.Getenv("INVENTORY_MODE"))
	if inventory == "" {
		inventory = "optimistic"
	}

	return Config{
		DBHost:     dbHost,
		DBPort:     envOrDefault("DB_PORT", "5432"),
		DBName:     envOrDefault("DB_NAME", "syscrack"),
		DBUser:     envOrDefault("DB_USER", "syscrack"),
		DBPassword: envOrDefault("DB_PASSWORD", "syscrack"),

		CacheHost:     os.Getenv("CACHE_HOST"),
		CachePort:     envOrDefault("CACHE_PORT", "6379"),
		CacheMode:     cacheMode,
		ProxyPort:     envOrDefault("CACHE_PROXY_PORT", "8081"),
		CacheStrategy: cacheStrategy,
		CacheTTL:      time.Duration(cacheTTL) * time.Second,
		Stampede:      os.Getenv("STAMPEDE_PROTECTION") == "true",

		MQHost: os.Getenv("MQ_HOST"),
		MQPort: envOrDefault("MQ_PORT", "5672"),

		ServiceRole:   role,
		InventoryMode: inventory,
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
	role := s.cfg.ServiceRole

	if role == "" || role == "catalog" {
		mux.Handle("/catalog", withRecovery(withMetrics("/catalog", http.HandlerFunc(s.handleCatalog))))
		mux.Handle("/products/", withRecovery(withMetrics("/products", http.HandlerFunc(s.handleProduct))))
		mux.Handle("/product-pool", withRecovery(http.HandlerFunc(s.handleProductPool)))
	}

	if role == "" || role == "checkout" {
		mux.Handle("/checkout", withRecovery(withMetrics("/checkout", http.HandlerFunc(s.handleCheckout))))
		mux.Handle("/orders/", withRecovery(withMetrics("/orders", http.HandlerFunc(s.handleOrder))))
	}

	mux.Handle("/health", withRecovery(http.HandlerFunc(s.handleHealth)))
	mux.Handle("/cache-stats", withRecovery(http.HandlerFunc(s.handleCacheStats)))
	mux.Handle("/metrics", promhttp.Handler())

	return mux
}

func (s *Server) handleCatalog(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	ctx := r.Context()
	const key = "catalog"

	if s.proxy != nil {
		value, source, ok, err := s.proxy.Get(ctx, key)
		if err != nil {
			slog.Error("catalog proxy GET failed", "error", err)
			jsonError(w, http.StatusBadGateway, "failed to fetch catalog")
			return
		}
		if !ok {
			jsonError(w, http.StatusNotFound, "catalog unavailable")
			return
		}
		if source == "cache" {
			s.cacheHits.Add(1)
			cacheHitsTotal.Inc()
		} else {
			s.cacheMisses.Add(1)
			cacheMissesTotal.Inc()
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(value))
		return
	}

	if s.cache != nil {
		if cached, ok := s.cache.Get(ctx, key); ok {
			s.cacheHits.Add(1)
			cacheHitsTotal.Inc()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(cached))
			return
		}
		s.cacheMisses.Add(1)
		cacheMissesTotal.Inc()

		fetchFn := func() (string, error) {
			products, err := s.db.ListProducts(ctx)
			if err != nil {
				return "", err
			}
			body, err := json.Marshal(map[string]any{"products": products})
			if err != nil {
				return "", err
			}
			return string(body), nil
		}
		payload, err := s.cache.GetProtected(ctx, key, fetchFn)
		if err != nil {
			slog.Error("catalog fetch failed", "error", err)
			jsonError(w, http.StatusInternalServerError, "failed to list products")
			return
		}
		_ = s.cache.Set(ctx, key, payload, s.cfg.CacheTTL)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(payload))
		return
	}

	products, err := s.db.ListProducts(ctx)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "failed to list products")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"products": products})
}

func (s *Server) handleProduct(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	idRaw := strings.Trim(strings.TrimPrefix(r.URL.Path, "/products/"), "/")
	id, err := strconv.Atoi(idRaw)
	if err != nil || id <= 0 {
		jsonError(w, http.StatusBadRequest, "invalid product id")
		return
	}
	ctx := r.Context()
	cacheKey := fmt.Sprintf("product:%d", id)

	if s.proxy != nil {
		value, source, ok, err := s.proxy.Get(ctx, cacheKey)
		if err != nil {
			slog.Error("product proxy GET failed", "error", err)
			jsonError(w, http.StatusBadGateway, "failed to fetch product")
			return
		}
		if !ok {
			jsonError(w, http.StatusNotFound, "product not found")
			return
		}
		if source == "cache" {
			s.cacheHits.Add(1)
			cacheHitsTotal.Inc()
		} else {
			s.cacheMisses.Add(1)
			cacheMissesTotal.Inc()
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(value))
		return
	}

	if s.cache != nil {
		if cached, ok := s.cache.Get(ctx, cacheKey); ok {
			s.cacheHits.Add(1)
			cacheHitsTotal.Inc()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(cached))
			return
		}
		s.cacheMisses.Add(1)
		cacheMissesTotal.Inc()
		fetchFn := func() (string, error) {
			p, err := s.db.GetProduct(ctx, id)
			if errors.Is(err, ErrNotFound) {
				return "", ErrNotFound
			}
			if err != nil {
				return "", err
			}
			b, err := json.Marshal(p)
			if err != nil {
				return "", err
			}
			return string(b), nil
		}
		payload, err := s.cache.GetProtected(ctx, cacheKey, fetchFn)
		if errors.Is(err, ErrNotFound) {
			jsonError(w, http.StatusNotFound, "product not found")
			return
		}
		if err != nil {
			jsonError(w, http.StatusInternalServerError, "failed to fetch product")
			return
		}
		_ = s.cache.Set(ctx, cacheKey, payload, s.cfg.CacheTTL)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(payload))
		return
	}

	p, err := s.db.GetProduct(ctx, id)
	if errors.Is(err, ErrNotFound) {
		jsonError(w, http.StatusNotFound, "product not found")
		return
	}
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "failed to fetch product")
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (s *Server) handleCheckout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	start := time.Now()
	defer func() {
		mode := "sync"
		if s.mq != nil {
			mode = "async"
		}
		checkoutDuration.WithLabelValues(mode).Observe(time.Since(start).Seconds())
	}()

	var req checkoutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	if req.ProductID <= 0 || req.Quantity <= 0 {
		jsonError(w, http.StatusBadRequest, "product_id and quantity must be > 0")
		return
	}
	ctx := r.Context()
	if _, err := s.db.GetProduct(ctx, req.ProductID); errors.Is(err, ErrNotFound) {
		jsonError(w, http.StatusNotFound, "product not found")
		return
	} else if err != nil {
		jsonError(w, http.StatusInternalServerError, "failed to validate product")
		return
	}

	if s.mq != nil {
		orderID, err := s.db.InsertPendingOrder(ctx, req.ProductID, req.Quantity)
		if err != nil {
			checkoutTotal.WithLabelValues("async", "error").Inc()
			jsonError(w, http.StatusInternalServerError, "failed to create order")
			return
		}
		msg, _ := json.Marshal(map[string]any{
			"order_id":   orderID,
			"product_id": req.ProductID,
			"quantity":   req.Quantity,
		})
		if err := s.mq.Publish(ctx, msg); err != nil {
			checkoutTotal.WithLabelValues("async", "error").Inc()
			mqPublishErrors.Inc()
			jsonError(w, http.StatusInternalServerError, "failed to enqueue order")
			return
		}
		mqPublishTotal.Inc()
		checkoutTotal.WithLabelValues("async", "success").Inc()
		writeJSON(w, http.StatusAccepted, map[string]any{
			"order_id": orderID,
			"status":   "pending",
			"mode":     "async",
		})
		return
	}

	var orderID int
	var err error
	if s.cfg.InventoryMode == "pessimistic" {
		orderID, err = s.db.CheckoutPessimistic(ctx, req.ProductID, req.Quantity)
	} else {
		orderID, err = s.db.CheckoutOptimistic(ctx, req.ProductID, req.Quantity)
	}
	if errors.Is(err, ErrInsufficientStock) {
		inventoryConflicts.Inc()
		checkoutTotal.WithLabelValues("sync", "conflict").Inc()
		jsonError(w, http.StatusConflict, "insufficient stock")
		return
	}
	if errors.Is(err, ErrNotFound) {
		checkoutTotal.WithLabelValues("sync", "error").Inc()
		jsonError(w, http.StatusNotFound, "product not found")
		return
	}
	if err != nil {
		checkoutTotal.WithLabelValues("sync", "error").Inc()
		jsonError(w, http.StatusInternalServerError, "checkout failed")
		return
	}
	if s.proxy != nil {
		p, err := s.db.GetProduct(ctx, req.ProductID)
		if err == nil {
			b, _ := json.Marshal(p)
			_ = s.proxy.Set(ctx, fmt.Sprintf("product:%d", req.ProductID), string(b), s.cfg.CacheTTL)
		}
		_ = s.proxy.Set(ctx, "catalog", "", 1*time.Millisecond)
	} else if s.cache != nil {
		_ = s.cache.Delete(ctx, "catalog", fmt.Sprintf("product:%d", req.ProductID))
	}
	checkoutTotal.WithLabelValues("sync", "success").Inc()
	writeJSON(w, http.StatusOK, map[string]any{
		"order_id": orderID,
		"status":   "fulfilled",
		"mode":     "sync",
	})
}

func (s *Server) handleOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	idRaw := strings.Trim(strings.TrimPrefix(r.URL.Path, "/orders/"), "/")
	id, err := strconv.Atoi(idRaw)
	if err != nil || id <= 0 {
		jsonError(w, http.StatusBadRequest, "invalid order id")
		return
	}
	order, err := s.db.GetOrder(r.Context(), id)
	if errors.Is(err, ErrNotFound) {
		jsonError(w, http.StatusNotFound, "order not found")
		return
	}
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "failed to fetch order")
		return
	}
	writeJSON(w, http.StatusOK, order)
}

func (s *Server) handleProductPool(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	ctx := r.Context()
	if s.proxy != nil {
		const poolKey = "product-pool"
		value, source, ok, err := s.proxy.Get(ctx, poolKey)
		if err != nil {
			slog.Error("product-pool proxy GET failed", "error", err)
			jsonError(w, http.StatusBadGateway, "failed to fetch product pool")
			return
		}
		if !ok {
			jsonError(w, http.StatusNotFound, "product pool unavailable")
			return
		}
		if source == "cache" {
			s.cacheHits.Add(1)
			cacheHitsTotal.Inc()
		} else {
			s.cacheMisses.Add(1)
			cacheMissesTotal.Inc()
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(value))
		return
	}
	rows, err := s.db.pool.Query(r.Context(), `SELECT id FROM products ORDER BY id LIMIT 50`)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "failed to fetch product ids")
		return
	}
	defer rows.Close()
	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err == nil {
			ids = append(ids, id)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"productIds": ids})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	ctx := r.Context()
	var n int
	if err := s.db.pool.QueryRow(ctx, "SELECT 1").Scan(&n); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"status": "error",
			"db":     "unreachable",
		})
		return
	}
	cacheHealth := "disabled"
	if s.proxy != nil {
		status, _, _, err := s.proxy.Health(ctx)
		if err != nil || status != "ok" {
			cacheHealth = "error"
		} else {
			cacheHealth = "ok"
		}
	} else if s.cache != nil {
		if err := s.cache.client.Ping(ctx).Err(); err != nil {
			cacheHealth = "error"
		} else {
			cacheHealth = "ok"
		}
	}
	mqHealth := "disabled"
	mode := "sync"
	if s.mq != nil {
		mode = "async"
		if err := s.mq.Ping(); err != nil {
			mqHealth = "error"
		} else {
			mqHealth = "ok"
		}
	}
	role := s.cfg.ServiceRole
	if role == "" {
		role = "monolith"
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
		"db":     "ok",
		"cache":  cacheHealth,
		"mq":     mqHealth,
		"mode":   mode,
		"role":   role,
	})
}

func (s *Server) handleCacheStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	hits := s.cacheHits.Load()
	misses := s.cacheMisses.Load()
	total := hits + misses
	hitRate := 0.0
	if total > 0 {
		hitRate = float64(hits) / float64(total)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"cache_hits":          hits,
		"cache_misses":        misses,
		"hit_rate":            hitRate,
		"strategy":            s.cfg.CacheStrategy,
		"cache_mode":          s.cfg.CacheMode,
		"stampede_protection": s.cfg.Stampede,
	})
}

func newServer(ctx context.Context, cfg Config) (*Server, error) {
	connURL := &url.URL{
		Scheme: "postgres",
		Host:   fmt.Sprintf("%s:%s", cfg.DBHost, cfg.DBPort),
		Path:   "/" + cfg.DBName,
		User:   url.UserPassword(cfg.DBUser, cfg.DBPassword),
	}
	pool, err := pgxpool.New(ctx, connURL.String())
	if err != nil {
		return nil, err
	}

	s := &Server{db: newDB(pool), cfg: cfg}
	if err := s.db.InitSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if err := s.db.SeedIfEmpty(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	if cfg.CacheHost != "" {
		if cfg.CacheMode == "proxy" {
			s.proxy = NewProxyClient(cfg.CacheHost, cfg.ProxyPort)
			slog.Info("cache mode: proxy", "host", cfg.CacheHost, "port", cfg.ProxyPort)
		} else {
			client := redis.NewClient(&redis.Options{
				Addr: fmt.Sprintf("%s:%s", cfg.CacheHost, cfg.CachePort),
			})
			if err := client.Ping(ctx).Err(); err != nil {
				slog.Warn("redis ping failed; continuing with cache disabled", "error", err)
			} else {
				s.cache = newCache(client, cfg.Stampede)
				slog.Info("cache mode: redis", "host", cfg.CacheHost, "port", cfg.CachePort)
			}
		}
	}

	if cfg.MQHost != "" {
		mq, err := NewMQPublisher(cfg.MQHost, cfg.MQPort, "orders")
		if err != nil {
			slog.Warn("rabbitmq connection failed; falling back to sync mode", "error", err)
		} else {
			s.mq = mq
		}
	}

	return s, nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	slog.Info("ecommerce starting")

	cfg := loadConfig()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	s, err := newServer(ctx, cfg)
	if err != nil {
		slog.Error("newServer failed", "error", err)
		os.Exit(1)
	}
	defer s.db.pool.Close()
	if s.mq != nil {
		defer s.mq.Close()
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

	slog.Info("listening", "port", cfg.Port, "role", cfg.ServiceRole)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}
