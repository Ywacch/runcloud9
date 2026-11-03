package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/ywacch/cloud9/pkg/templateapp/jobs/queue"
	"github.com/ywacch/cloud9/pkg/templateapp/jobs/timeline"
	"github.com/ywacch/cloud9/pkg/templateapp/trace"
)

var (
	workerProcessedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "social_worker_processed_total",
		Help: "Total posts successfully fanned out",
	})
	workerErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "social_worker_errors_total",
		Help: "Total failed fan-out attempts by bounded reason",
	}, []string{"reason"})
	workerProcessingDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "social_worker_processing_duration_seconds",
		Help:    "Time to fan out one post to all followers",
		Buckets: prometheus.DefBuckets,
	})
	workerFanoutSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "social_worker_fanout_size",
		Help:    "Number of followers per fan-out (write amplification)",
		Buckets: []float64{1, 5, 10, 20, 30, 50, 100, 250, 500, 1000, 2500, 5000},
	})
	workerWriteTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "social_worker_writes_total",
		Help: "Total timeline writes by destination store",
	}, []string{"store"})
)

type fanoutMessage struct {
	PostID    int       `json:"post_id"`
	UserID    int       `json:"user_id"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func envIntOrDefault(key string, defaultVal int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		slog.Warn("invalid integer env; using default", "key", key, "value", raw, "default", defaultVal)
		return defaultVal
	}
	return n
}

func requiredEnv(key, msg string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error(msg)
		os.Exit(1)
	}
	return v
}

func openDB(ctx context.Context) (*pgxpool.Pool, error) {
	dbHost := requiredEnv("DB_HOST", "DB_HOST required for worker")
	dbPort := envOrDefault("DB_PORT", "5432")
	dbName := envOrDefault("DB_NAME", "syscrack")
	dbUser := envOrDefault("DB_USER", "syscrack")
	dbPassword := envOrDefault("DB_PASSWORD", "syscrack")

	connURL := &url.URL{
		Scheme: "postgres",
		Host:   fmt.Sprintf("%s:%s", dbHost, dbPort),
		Path:   "/" + dbName,
		User:   url.UserPassword(dbUser, dbPassword),
	}
	return pgxpool.New(ctx, connURL.String())
}

func classifyFanoutError(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "db "):
		return "db"
	case strings.Contains(msg, "redis") && strings.Contains(msg, "connection refused"):
		return "redis_connection_refused"
	case strings.Contains(msg, "redis") && strings.Contains(msg, "connect:"):
		return "redis_connect"
	case strings.Contains(msg, "redis") && strings.Contains(msg, "timeout"):
		return "redis_timeout"
	case strings.Contains(msg, "redis") && strings.Contains(msg, "i/o timeout"):
		return "redis_timeout"
	case strings.Contains(msg, "no rows"), strings.Contains(msg, "sql"), strings.Contains(msg, "postgres"):
		return "db"
	default:
		return "unknown"
	}
}

func fanoutPost(ctx context.Context, followers FollowerStore, ts timeline.Store, msg fanoutMessage) error {
	dbStart := time.Now()
	username, err := followers.GetAuthor(ctx, msg.UserID)
	if err != nil {
		return err
	}
	trace.LogWorker(ctx, "db_user_lookup", dbStart)

	dbStart = time.Now()
	followerIDs, err := followers.GetFollowers(ctx, msg.UserID)
	if err != nil {
		return err
	}
	trace.LogWorker(ctx, "db_follower_lookup", dbStart)

	workerFanoutSize.Observe(float64(len(followerIDs)))
	workerWriteTotal.WithLabelValues(ts.Engine()).Add(float64(len(followerIDs)))
	payload, _ := json.Marshal(map[string]any{
		"post_id":    msg.PostID,
		"author_id":  msg.UserID,
		"username":   username,
		"content":    msg.Content,
		"created_at": msg.CreatedAt,
	})
	redisStart := time.Now()
	userIDs := make([]int64, len(followerIDs))
	for i, followerID := range followerIDs {
		userIDs[i] = int64(followerID)
	}
	item := timeline.Item{
		PostID:    int64(msg.PostID),
		AuthorID:  int64(msg.UserID),
		Payload:   payload,
		CreatedAt: msg.CreatedAt,
	}
	if err := ts.Append(ctx, userIDs, item); err != nil {
		return fmt.Errorf("redis timeline: %w", err)
	}
	trace.LogWorker(ctx, "redis_timeline_write", redisStart)
	return nil
}

func processMessage(ctx context.Context, followers FollowerStore, ts timeline.Store, body []byte) error {
	start := time.Now()
	defer func() {
		workerProcessingDuration.Observe(time.Since(start).Seconds())
	}()

	var msg fanoutMessage
	if err := json.Unmarshal(body, &msg); err != nil {
		workerErrorsTotal.WithLabelValues("decode").Inc()
		slog.Error("invalid fanout message", "error", err)
		return err
	}

	if err := fanoutPost(ctx, followers, ts, msg); err != nil {
		reason := classifyFanoutError(err)
		workerErrorsTotal.WithLabelValues(reason).Inc()
		slog.Error("fanout failed", "post_id", msg.PostID, "author_id", msg.UserID, "reason", reason, "error", err, "request_id", trace.RequestID(ctx))
		return err
	}
	trace.LogWorker(ctx, "fanout", start)
	workerProcessedTotal.Inc()
	return nil
}

func healthHandler(pool *pgxpool.Pool, mqStatus func(context.Context) string, rdb *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		db := "ok"
		if err := pool.QueryRow(ctx, "SELECT 1").Scan(new(int)); err != nil {
			db = "error"
		}
		mq := mqStatus(ctx)
		cache := "disabled"
		if rdb != nil {
			if rdb.Ping(ctx).Err() != nil {
				cache = "error"
			} else {
				cache = "ok"
			}
		}
		status := http.StatusOK
		if db != "ok" || mq != "ok" || cache != "ok" {
			status = http.StatusInternalServerError
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok", "mq": mq, "db": db, "cache": cache})
	}
}

func startMetricsHTTP(pool *pgxpool.Pool, mqStatus func(context.Context) string, rdb *redis.Client) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/health", healthHandler(pool, mqStatus, rdb))
	srv := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("worker metrics server failed", "error", err)
		}
	}()
	return srv
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	defer func() {
		if r := recover(); r != nil {
			slog.Error("worker panic", "recover", r, "stack", string(debug.Stack()))
			os.Exit(1)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	slog.Info("fanout-worker startup begin")
	timelineEngine := strings.ToLower(strings.TrimSpace(os.Getenv("TIMELINE_ENGINE")))
	timelineTTL := time.Duration(parseCacheTTLSeconds(os.Getenv("CACHE_TTL"), 0)) * time.Second
	queueCfg := queue.EnvConfig("fanout", prometheus.DefaultRegisterer, nil)
	slog.Info("startup config resolved",
		"timelineEngine", timelineEngine,
		"timelineTTL", timelineTTL,
		"workQueueEngine", queueCfg.EngineEnv,
		"workQueueHost", queueCfg.HostEnv,
		"workQueuePort", queueCfg.PortEnv,
	)

	// DB connection with retry so worker doesn't crash-loop while dependencies boot.
	var pool *pgxpool.Pool
	for attempt := 1; ; attempt++ {
		var err error
		slog.Info("attempting db connect", "attempt", attempt)
		pool, err = openDB(ctx)
		if err == nil {
			slog.Info("db connect established", "attempt", attempt)
			break
		}
		if attempt >= 30 {
			slog.Error("db connect failed after retries", "error", err, "attempts", attempt)
			os.Exit(1)
		}
		delay := time.Duration(attempt) * time.Second
		if delay > 10*time.Second {
			delay = 10 * time.Second
		}
		slog.Warn("db connect failed, retrying", "error", err, "attempt", attempt, "retryIn", delay)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			os.Exit(0)
		}
	}
	defer pool.Close()
	followers := newPGFollowerStore(pool)

	var rdb *redis.Client
	var timelineStore timeline.Store
	switch timelineEngine {
	case "", "redis":
		cacheHost := requiredEnv("CACHE_HOST", "CACHE_HOST required for redis timeline worker")
		cachePort := envOrDefault("CACHE_PORT", "6379")
		rdb = redis.NewClient(&redis.Options{
			Addr: fmt.Sprintf("%s:%s", cacheHost, cachePort),
		})
		for attempt := 1; ; attempt++ {
			err := rdb.Ping(ctx).Err()
			if err == nil {
				slog.Info("redis connected", "attempt", attempt)
				break
			}
			if attempt >= 30 {
				slog.Error("redis ping failed after retries", "error", err, "attempts", attempt)
				os.Exit(1)
			}
			delay := time.Duration(attempt) * time.Second
			if delay > 10*time.Second {
				delay = 10 * time.Second
			}
			slog.Warn("redis ping failed, retrying", "error", err, "attempt", attempt, "retryIn", delay)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				os.Exit(0)
			}
		}
		var err error
		timelineStore, err = timeline.Open("", prometheus.DefaultRegisterer, rdb, timeline.RedisOptions{
			MaxEntries: timeline.DefaultMaxEntries,
			TTL:        timelineTTL,
		}, nil, nil, timeline.PostgresOptions{})
		if err != nil {
			slog.Error("timeline store init failed", "error", err)
			os.Exit(1)
		}
	case "postgresql", "postgres":
		if err := timeline.InitPostgresSchema(ctx, pool); err != nil {
			slog.Error("timeline postgres schema failed", "error", err)
			os.Exit(1)
		}
		var err error
		timelineStore, err = timeline.Open("postgresql", prometheus.DefaultRegisterer, nil, timeline.RedisOptions{}, pool, pool, timeline.PostgresOptions{
			MaxEntries: timeline.DefaultMaxEntries,
		})
		if err != nil {
			slog.Error("timeline store init failed", "error", err)
			os.Exit(1)
		}
	default:
		slog.Error("unsupported TIMELINE_ENGINE", "value", timelineEngine)
		os.Exit(1)
	}

	// A nil *redis.Client inside the interface compares unequal to nil.
	cacheHost := strings.TrimSpace(os.Getenv("CACHE_HOST"))
	queueHost := strings.TrimSpace(queueCfg.HostEnv)
	if rdb != nil && (queueHost == "" || queueHost == cacheHost) {
		queueCfg.RedisClient = rdb
	}
	var queueRdb *redis.Client
	var workQ queue.Queue
	for attempt := 1; ; attempt++ {
		if strings.EqualFold(strings.TrimSpace(queueCfg.EngineEnv), "redis") && queueCfg.RedisClient == nil {
			if queueHost == "" {
				slog.Error("WORK_QUEUE_HOST required for redis work queue")
				os.Exit(1)
			}
			cachePort := queueCfg.PortEnv
			if cachePort == "" {
				cachePort = "6379"
			}
			client := redis.NewClient(&redis.Options{
				Addr: fmt.Sprintf("%s:%s", queueHost, cachePort),
			})
			queueCfg.RedisClient = client
			queueRdb = client
		}
		var err error
		workQ, err = queue.Open(queueCfg)
		if err == nil {
			slog.Info("work queue opened", "engine", workQ.Engine(), "attempt", attempt)
			break
		}
		if attempt >= 30 {
			slog.Error("work queue open failed after retries", "error", err, "attempts", attempt)
			os.Exit(1)
		}
		delay := time.Duration(attempt) * time.Second
		if delay > 10*time.Second {
			delay = 10 * time.Second
		}
		slog.Warn("work queue open failed, retrying", "error", err, "attempt", attempt, "retryIn", delay)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			os.Exit(0)
		}
	}
	if rmq, ok := workQ.(*queue.RabbitMQ); ok {
		defer func() { _ = rmq.Close() }()
	}
	if rdb != nil {
		defer rdb.Close()
	}
	if queueRdb != nil && queueRdb != rdb {
		defer queueRdb.Close()
	}

	mqStatus := func(ctx context.Context) string {
		switch q := workQ.(type) {
		case *queue.RabbitMQ:
			if q.Ping() != nil {
				return "error"
			}
		case *queue.Streams:
			if q.Ping(ctx) != nil {
				return "error"
			}
		}
		return "ok"
	}

	srv := startMetricsHTTP(pool, mqStatus, rdb)
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	concurrency := envIntOrDefault("WORKER_CONCURRENCY", 1)
	slog.Info("fanout-worker started", "queue", "fanout", "engine", workQ.Engine(), "concurrency", concurrency)

	var wg sync.WaitGroup
	for workerID := 1; workerID <= concurrency; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			slog.Info("fanout worker goroutine started", "worker_id", id)
			err := workQ.Consume(ctx, func(ctx context.Context, body []byte) error {
				return processMessage(ctx, followers, timelineStore, body)
			})
			if err != nil && ctx.Err() == nil {
				slog.Error("work queue consume ended", "worker_id", id, "error", err)
			}
		}(workerID)
	}
	wg.Wait()
	slog.Info("fanout-worker shutting down")
}
