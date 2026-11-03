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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/ywacch/cloud9/pkg/templateapp/jobs/queue"
	"github.com/ywacch/cloud9/pkg/templateapp/jobs/timeline"
	"github.com/ywacch/cloud9/pkg/templateapp/postgres"
	"github.com/ywacch/cloud9/pkg/templateapp/readrouter"
	"github.com/ywacch/cloud9/pkg/templateapp/trace"
)

type Config struct {
	DBHost          string
	DBPort          string
	DBName          string
	DBUser          string
	DBPassword      string
	ReadReplicaHost string
	ReadReplicaPort string
	ReadConsistency readrouter.RoutingPolicy

	CacheHost     string
	CachePort     string
	CacheMode     string
	ProxyPort     string
	CacheStrategy string
	CacheTTL      time.Duration
	Stampede      bool

	MQHost string
	MQPort string

	FeedPageSize int
	ServiceRole  string
	Port         string
}

type Server struct {
	store    SocialStore
	pg       *DB
	cache    *Cache
	timeline timeline.Store
	proxy    *ProxyClient
	workQ    queue.Queue
	cfg      Config

	cacheHits   atomic.Int64
	cacheMisses atomic.Int64
}

type postRequest struct {
	UserID    int    `json:"user_id"`
	Content   string `json:"content"`
	ReplyToID int    `json:"reply_to_id"`
}

type followRequest struct {
	FollowerID  int `json:"follower_id"`
	FollowingID int `json:"following_id"`
}

type createUserRequest struct {
	Username string `json:"username"`
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

	// Default 0 = persistent timelines (the template's historical behavior:
	// timeline ZSETs never expired). A positive TTL applies EXPIRE after each
	// fan-out write.
	cacheTTL := cacheTTLSecondsFromEnv(0, "FEED_CACHE_TTL", "CACHE_TTL")

	pageSize := 20
	if v := os.Getenv("FEED_PAGE_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			pageSize = n
		}
	}

	role := strings.TrimSpace(os.Getenv("SERVICE_ROLE"))
	if role == "monolith" {
		role = ""
	}

	cacheMode := os.Getenv("CACHE_MODE")
	if cacheMode == "" {
		cacheMode = "redis"
	}

	cacheStrategy := os.Getenv("CACHE_STRATEGY")
	if cacheStrategy == "" {
		cacheStrategy = "cache-aside"
	}

	return Config{
		DBHost:          dbHost,
		DBPort:          envOrDefault("DB_PORT", "5432"),
		DBName:          envOrDefault("DB_NAME", "syscrack"),
		DBUser:          envOrDefault("DB_USER", "syscrack"),
		DBPassword:      envOrDefault("DB_PASSWORD", "syscrack"),
		ReadReplicaHost: os.Getenv("READ_REPLICA_HOST"),
		ReadReplicaPort: envOrDefault("READ_REPLICA_PORT", "5432"),
		ReadConsistency: readrouter.PolicyFromEnv(os.Getenv("READ_CONSISTENCY")),

		CacheHost:     os.Getenv("CACHE_HOST"),
		CachePort:     envOrDefault("CACHE_PORT", "6379"),
		CacheMode:     cacheMode,
		ProxyPort:     envOrDefault("CACHE_PROXY_PORT", "8081"),
		CacheStrategy: cacheStrategy,
		CacheTTL:      time.Duration(cacheTTL) * time.Second,
		Stampede:      os.Getenv("STAMPEDE_PROTECTION") == "true",

		MQHost: os.Getenv("MQ_HOST"),
		MQPort: envOrDefault("MQ_PORT", "5672"),

		FeedPageSize: pageSize,
		ServiceRole:  role,
		Port:         envOrDefault("PORT", "8080"),
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

	if role == "" || role == "feed" {
		mux.Handle("/feed/", withRecovery(withMetrics("/feed", http.HandlerFunc(s.handleFeed))))
		mux.Handle("/user-pool", withRecovery(http.HandlerFunc(s.handleUserPool)))
	}

	if role == "" || role == "post" {
		mux.Handle("/post", withRecovery(withMetrics("/post", http.HandlerFunc(s.handlePost))))
		mux.Handle("/thread-pool", withRecovery(http.HandlerFunc(s.handleThreadPool)))
		mux.Handle("/threads/", withRecovery(withMetrics("/threads", http.HandlerFunc(s.handleThread))))
	}

	mux.Handle("/users", withRecovery(http.HandlerFunc(s.handleUsers)))
	mux.Handle("/users/", withRecovery(withMetrics("/users", http.HandlerFunc(s.handleUser))))
	mux.Handle("/follow", withRecovery(http.HandlerFunc(s.handleFollow)))
	mux.Handle("/unfollow", withRecovery(http.HandlerFunc(s.handleUnfollow)))
	mux.Handle("/health", withRecovery(http.HandlerFunc(s.handleHealth)))
	mux.Handle("/health/live", withRecovery(http.HandlerFunc(s.handleLive)))
	mux.Handle("/health/ready", withRecovery(http.HandlerFunc(s.handleReady)))
	mux.Handle("/cache-stats", withRecovery(http.HandlerFunc(s.handleCacheStats)))
	mux.Handle("/trace/samples", withRecovery(trace.SamplesHandler(trace.DefaultStore())))
	mux.Handle("/metrics", promhttp.Handler())

	return mux
}

func (s *Server) handlePost(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req postRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	req.Content = strings.TrimSpace(req.Content)
	if req.UserID <= 0 {
		jsonError(w, http.StatusBadRequest, "user_id must be > 0")
		return
	}
	if req.Content == "" || len(req.Content) > 280 {
		jsonError(w, http.StatusBadRequest, "content must be 1-280 chars")
		return
	}

	ctx := r.Context()
	user, err := s.store.GetUser(ctx, req.UserID)
	if errors.Is(err, ErrNotFound) {
		jsonError(w, http.StatusNotFound, "user not found")
		return
	}
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "failed to validate user")
		return
	}

	post, err := s.store.InsertPost(ctx, req.UserID, req.Content, req.ReplyToID)
	if errors.Is(err, ErrNotFound) {
		jsonError(w, http.StatusNotFound, "reply parent not found")
		return
	}
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "failed to insert post")
		return
	}

	if s.workQ != nil {
		msg, _ := json.Marshal(map[string]any{
			"post_id":    post.ID,
			"user_id":    post.UserID,
			"content":    post.Content,
			"created_at": post.CreatedAt,
		})
		if err := s.workQ.Publish(ctx, msg); err != nil {
			postTotal.WithLabelValues("async_error").Inc()
			mqPublishErrors.Inc()
			jsonError(w, http.StatusInternalServerError, "failed to enqueue post")
			return
		}
		postTotal.WithLabelValues("async").Inc()
		mqPublishTotal.Inc()
		writeJSON(w, http.StatusAccepted, map[string]any{"post_id": post.ID, "mode": "async"})
		return
	}

	followers, err := s.store.GetFollowers(ctx, post.UserID)
	if err == nil && len(followers) > 0 && s.timeline != nil {
		payload, _ := json.Marshal(map[string]any{
			"post_id":    post.ID,
			"author_id":  post.UserID,
			"username":   user.Username,
			"content":    post.Content,
			"created_at": post.CreatedAt,
		})
		userIDs := make([]int64, len(followers))
		for i, followerID := range followers {
			userIDs[i] = int64(followerID)
		}
		item := timeline.Item{
			PostID:    int64(post.ID),
			AuthorID:  int64(post.UserID),
			Payload:   payload,
			CreatedAt: post.CreatedAt,
		}
		followerCount := len(followers)
		go func(postID int, n int) {
			bgCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := s.timeline.Append(bgCtx, userIDs, item); err != nil {
				slog.Warn("sync fanout timeline append failed", "error", err, "post_id", postID)
				return
			}
			slog.Info("sync fanout complete",
				"post_id", postID,
				"followers_updated", n,
				"operation", "fanout_complete",
			)
		}(post.ID, followerCount)
	}
	postTotal.WithLabelValues("sync").Inc()
	writeJSON(w, http.StatusOK, map[string]any{"post_id": post.ID, "mode": "sync"})
}

func (s *Server) handleThreadPool(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	entries, err := s.store.ListThreadPool(r.Context(), 50)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "failed to list threads")
		return
	}
	writeThreadPool(w, entries)
}

func (s *Server) handleThread(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	idRaw := strings.Trim(strings.TrimPrefix(r.URL.Path, "/threads/"), "/")
	rootID, err := strconv.Atoi(idRaw)
	if err != nil || rootID <= 0 {
		jsonError(w, http.StatusBadRequest, "invalid thread root id")
		return
	}
	thread, err := s.store.GetThread(r.Context(), rootID)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "failed to fetch thread")
		return
	}
	source := "user"
	if r.Header.Get("X-C9-Observer") == "1" {
		source = "observer"
	}
	recordThreadReadMetrics(source, thread.Replies, thread.Parents)
	writeJSON(w, http.StatusOK, map[string]any{
		"rootId":   thread.RootID,
		"replies":  thread.Replies,
		"parents":  thread.Parents,
		"servedBy": thread.ServedBy,
	})
}

func (s *Server) handleFeed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	start := time.Now()
	mode := "pull" // default for latency label on validation errors before fetchFeedItems runs
	defer func() {
		feedLatency.WithLabelValues(mode).Observe(time.Since(start).Seconds())
	}()

	idRaw := strings.Trim(strings.TrimPrefix(r.URL.Path, "/feed/"), "/")
	userID, err := strconv.Atoi(idRaw)
	if err != nil || userID <= 0 {
		jsonError(w, http.StatusBadRequest, "invalid user id")
		return
	}
	ctx := r.Context()

	var items []FeedItem
	stats := &feedCacheStats{hits: &s.cacheHits, misses: &s.cacheMisses}
	items, mode, err = fetchFeedItems(ctx, s.timeline, s.cfg.FeedPageSize, userID, stats, s.store.GetFeedPull)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "failed to fetch feed")
		return
	}
	body, _ := json.Marshal(map[string]any{"feed": items, "mode": mode, "count": len(items)})
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

func decodeFollowRequest(w http.ResponseWriter, r *http.Request) (followRequest, bool) {
	var req followRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, http.StatusBadRequest, "invalid json body")
		return followRequest{}, false
	}
	if req.FollowerID <= 0 || req.FollowingID <= 0 {
		jsonError(w, http.StatusBadRequest, "follower_id and following_id must be > 0")
		return followRequest{}, false
	}
	return req, true
}

func (s *Server) validateFollowUsers(ctx context.Context, w http.ResponseWriter, followerID, followingID int) bool {
	if _, err := s.store.GetUser(ctx, followerID); errors.Is(err, ErrNotFound) {
		jsonError(w, http.StatusNotFound, "follower not found")
		return false
	} else if err != nil {
		jsonError(w, http.StatusInternalServerError, "failed to validate follower")
		return false
	}
	if _, err := s.store.GetUser(ctx, followingID); errors.Is(err, ErrNotFound) {
		jsonError(w, http.StatusNotFound, "following user not found")
		return false
	} else if err != nil {
		jsonError(w, http.StatusInternalServerError, "failed to validate following user")
		return false
	}
	return true
}

func (s *Server) handleFollow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	req, ok := decodeFollowRequest(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	if !s.validateFollowUsers(ctx, w, req.FollowerID, req.FollowingID) {
		return
	}
	if err := s.store.Follow(ctx, req.FollowerID, req.FollowingID); err != nil {
		jsonError(w, http.StatusInternalServerError, "failed to follow")
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"followed": true})
}

func (s *Server) handleUnfollow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	req, ok := decodeFollowRequest(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	if !s.validateFollowUsers(ctx, w, req.FollowerID, req.FollowingID) {
		return
	}
	if err := s.store.Unfollow(ctx, req.FollowerID, req.FollowingID); err != nil {
		jsonError(w, http.StatusInternalServerError, "failed to unfollow")
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"unfollowed": true})
}

func (s *Server) handleUsers(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		query := strings.TrimSpace(r.URL.Query().Get("username"))
		if alt := strings.TrimSpace(r.URL.Query().Get("q")); query == "" {
			query = alt
		}
		var (
			users []User
			err   error
		)
		if query != "" {
			users, err = s.store.SearchUsersByUsername(r.Context(), query, 20)
			if err != nil {
				jsonError(w, http.StatusInternalServerError, "failed to search users")
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"users": users, "query": query})
			return
		}
		users, err = s.store.ListUsers(r.Context(), 20)
		if err != nil {
			jsonError(w, http.StatusInternalServerError, "failed to list users")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"users": users})
	case http.MethodPost:
		var req createUserRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			jsonError(w, http.StatusBadRequest, "invalid json body")
			return
		}
		username := strings.TrimSpace(req.Username)
		if username == "" {
			jsonError(w, http.StatusBadRequest, "username is required")
			return
		}
		user, err := s.store.CreateUser(r.Context(), username)
		if errors.Is(err, ErrUsernameTaken) {
			jsonError(w, http.StatusConflict, "username already exists")
			return
		}
		if err != nil {
			jsonError(w, http.StatusInternalServerError, "failed to create user")
			return
		}
		writeJSON(w, http.StatusCreated, map[string]any{
			"id":       user.ID,
			"username": user.Username,
		})
	default:
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	idRaw := strings.Trim(strings.TrimPrefix(r.URL.Path, "/users/"), "/")
	id, err := strconv.Atoi(idRaw)
	if err != nil || id <= 0 {
		jsonError(w, http.StatusBadRequest, "invalid user id")
		return
	}
	user, err := s.store.GetUser(r.Context(), id)
	if errors.Is(err, ErrNotFound) {
		jsonError(w, http.StatusNotFound, "user not found")
		return
	}
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "failed to fetch user")
		return
	}
	writeJSON(w, http.StatusOK, user)
}

func userPoolSQL(segment string, limit int) string {
	lim := strconv.Itoa(limit)
	switch segment {
	case "celebrity":
		return `SELECT id FROM users WHERE id <= 10 ORDER BY id`
	case "normal":
		return `SELECT id FROM users WHERE id > 10 ORDER BY id LIMIT ` + lim
	default:
		return `SELECT id FROM users ORDER BY id LIMIT ` + lim
	}
}

func parseUserPoolLimit(raw string) int {
	if raw == "" {
		return 50
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > 10000 {
		return 50
	}
	return n
}

func (s *Server) handleUserPool(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	limit := parseUserPoolLimit(r.URL.Query().Get("limit"))
	ids, err := s.store.QueryUserPool(r.Context(), userPoolSQL(r.URL.Query().Get("segment"), limit))
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "failed to fetch user ids")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"userIds": ids})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	ctx := r.Context()
	if err := s.store.Ping(ctx); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"status": "error", "db": "unreachable"})
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
	mode := "pull"
	if s.workQ != nil {
		mode = "push"
		switch q := s.workQ.(type) {
		case *queue.RabbitMQ:
			if err := q.Ping(); err != nil {
				mqHealth = "error"
			} else {
				mqHealth = "ok"
			}
		case *queue.Streams:
			if err := q.Ping(ctx); err != nil {
				mqHealth = "error"
			} else {
				mqHealth = "ok"
			}
		default:
			mqHealth = "ok"
		}
	}
	role := s.cfg.ServiceRole
	if role == "" {
		role = "monolith"
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok", "db": "ok", "cache": cacheHealth, "mq": mqHealth, "mode": mode, "role": role,
	})
}

// handleLive is the Kubernetes liveness endpoint. It reports only that the
// process is up and serving HTTP; it never touches Postgres, Redis, the
// caching-proxy, or RabbitMQ. Keeping it dependency-free means a slow or
// briefly-unavailable downstream can never trigger a container restart storm.
func (s *Server) handleLive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReady is the Kubernetes readiness endpoint. It is intentionally
// dependency-free: the pod can accept traffic as soon as it is listening.
// Chaining downstream checks here (DB SELECT, proxy /health, MQ ping) is what
// previously flipped every pod NotReady under load and left the load balancer
// with no backends, amplifying failures. Deep diagnostics remain at GET /health.
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
	hitRate := 0.0
	if total > 0 {
		hitRate = float64(hits) / float64(total)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"cache_hits": hits, "cache_misses": misses, "hit_rate": hitRate,
		"strategy": s.cfg.CacheStrategy, "cache_mode": s.cfg.CacheMode, "stampede_protection": s.cfg.Stampede,
	})
}

func newServer(ctx context.Context, cfg Config) (*Server, error) {
	connURL := &url.URL{
		Scheme: "postgres",
		Host:   fmt.Sprintf("%s:%s", cfg.DBHost, cfg.DBPort),
		Path:   "/" + cfg.DBName,
		User:   url.UserPassword(cfg.DBUser, cfg.DBPassword),
	}
	poolCfg, err := pgxpool.ParseConfig(connURL.String())
	if err != nil {
		return nil, fmt.Errorf("parse db pool config: %w", err)
	}
	replicaHosts := postgres.ReadReplicaHostsFromEnv(cfg.ReadReplicaHost)
	writeMax, replicaEachMax := postgres.SplitPoolMaxConns(len(replicaHosts))
	poolCfg.MaxConns = writeMax

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, err
	}

	var readPools []*pgxpool.Pool
	var router readSplitRouter
	if len(replicaHosts) > 0 {
		replicaPools := make([]readrouter.ReplicaPool, 0, len(replicaHosts))
		pgCfg := postgres.Config{
			Port:     cfg.ReadReplicaPort,
			Name:     cfg.DBName,
			User:     cfg.DBUser,
			Password: cfg.DBPassword,
		}
		for _, host := range replicaHosts {
			pgCfg.Host = postgres.ReplicaConnectHost(host)
			readPool, poolErr := postgres.NewPoolWithMaxConns(ctx, pgCfg, replicaEachMax)
			if poolErr != nil {
				pool.Close()
				for _, rp := range readPools {
					rp.Close()
				}
				return nil, fmt.Errorf("connect read replica %q: %w", host, poolErr)
			}
			readPools = append(readPools, readPool)
			replicaPools = append(replicaPools, readrouter.ReplicaPool{Pool: readPool, Host: host})
		}
		router = readrouter.NewReadRouter(pool, replicaPools, readrouter.PostgresEngine{}, cfg.ReadConsistency)
		slog.Info("read replica routing enabled",
			"hosts", replicaHosts,
			"policy", cfg.ReadConsistency,
		)
	}

	pg := newDB(pool, readPools, router)
	s := &Server{store: pg, pg: pg, cfg: cfg}
	if err := s.store.InitSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if err := s.store.SeedIfEmpty(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	if cfg.CacheHost != "" {
		if cfg.CacheMode == "proxy" {
			s.proxy = NewProxyClient(cfg.CacheHost, cfg.ProxyPort)
			slog.Info("cache mode: proxy", "host", cfg.CacheHost, "port", cfg.ProxyPort)
		} else {
			client := redis.NewClient(&redis.Options{Addr: fmt.Sprintf("%s:%s", cfg.CacheHost, cfg.CachePort)})
			if err := client.Ping(ctx).Err(); err != nil {
				slog.Warn("redis ping failed; continuing with cache disabled", "error", err)
			} else {
				s.cache = newCache(client, cfg.Stampede)
				ts, err := timeline.Open(os.Getenv("TIMELINE_ENGINE"), prometheus.DefaultRegisterer, client, timeline.RedisOptions{
					MaxEntries: timeline.DefaultMaxEntries,
					TTL:        cfg.CacheTTL,
				}, pool, nil, timeline.PostgresOptions{MaxEntries: timeline.DefaultMaxEntries})
				if err != nil {
					pool.Close()
					return nil, fmt.Errorf("timeline store: %w", err)
				}
				s.timeline = ts
				go runTimelineKeyReporter(ctx, s.cache.redisClient())
				slog.Info("cache mode: redis", "host", cfg.CacheHost, "port", cfg.CachePort)
			}
		}
	}

	if s.timeline == nil {
		engine := strings.ToLower(strings.TrimSpace(os.Getenv("TIMELINE_ENGINE")))
		if engine == "postgresql" || engine == "postgres" {
			var reader timeline.PGReader = pool
			if router != nil {
				reader = newTimelineRouterReader(router)
			}
			ts, err := timeline.Open("postgresql", prometheus.DefaultRegisterer, nil, timeline.RedisOptions{}, pool, reader, timeline.PostgresOptions{
				MaxEntries: timeline.DefaultMaxEntries,
			})
			if err != nil {
				pool.Close()
				return nil, err
			}
			s.timeline = ts
			slog.Info("timeline engine: postgresql")
		}
	}

	var queueRedis redis.UniversalClient
	qCfg := queue.EnvConfig("fanout", prometheus.DefaultRegisterer, nil)
	if s.cache != nil {
		cacheHost := strings.TrimSpace(os.Getenv("CACHE_HOST"))
		queueHost := strings.TrimSpace(qCfg.HostEnv)
		if queueHost == "" || queueHost == cacheHost {
			queueRedis = s.cache.redisClient()
		}
	}
	qCfg.RedisClient = queueRedis
	if strings.TrimSpace(qCfg.HostEnv) != "" {
		q, err := queue.Open(qCfg)
		if err != nil {
			slog.Warn("work queue connection failed; falling back to pull mode", "error", err, "engine", qCfg.EngineEnv)
		} else {
			s.workQ = q
		}
	}

	if s.workQ != nil {
		slog.Info("social-feed mode: push (fan-out on write)")
	} else {
		slog.Info("social-feed mode: pull (fan-out on read)")
	}
	return s, nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	slog.Info("social-feed starting")

	cfg := loadConfig()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	s, err := newServer(ctx, cfg)
	if err != nil {
		slog.Error("newServer failed", "error", err)
		os.Exit(1)
	}
	defer s.pg.pool.Close()
	defer s.pg.Close()
	if s.workQ != nil {
		defer func() {
			if rmq, ok := s.workQ.(*queue.RabbitMQ); ok {
				_ = rmq.Close()
			}
		}()
	}

	srv := &http.Server{Addr: ":" + cfg.Port, Handler: s.routes()}
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
