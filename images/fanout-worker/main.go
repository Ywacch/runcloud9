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
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

var (
	workerProcessedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "social_worker_processed_total",
		Help: "Total posts successfully fanned out",
	})
	workerErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "social_worker_errors_total",
		Help: "Total failed fan-out attempts",
	})
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

func fanoutPost(ctx context.Context, pool *pgxpool.Pool, rdb *redis.Client, msg fanoutMessage) error {
	var username string
	if err := pool.QueryRow(ctx, `SELECT username FROM users WHERE id = $1`, msg.UserID).Scan(&username); err != nil {
		return err
	}

	rows, err := pool.Query(ctx, `SELECT follower_id FROM follows WHERE following_id = $1`, msg.UserID)
	if err != nil {
		return err
	}
	defer rows.Close()

	var followers []int
	for rows.Next() {
		var followerID int
		if err := rows.Scan(&followerID); err != nil {
			return err
		}
		followers = append(followers, followerID)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	workerFanoutSize.Observe(float64(len(followers)))
	workerWriteTotal.WithLabelValues("redis").Add(float64(len(followers)))
	payload, _ := json.Marshal(map[string]any{
		"post_id":    msg.PostID,
		"author_id":  msg.UserID,
		"username":   username,
		"content":    msg.Content,
		"created_at": msg.CreatedAt,
	})
	pipe := rdb.Pipeline()
	for _, followerID := range followers {
		key := fmt.Sprintf("timeline:%d", followerID)
		pipe.ZAdd(ctx, key, redis.Z{
			Score:  float64(msg.CreatedAt.UnixMilli()),
			Member: string(payload),
		})
		pipe.ZRemRangeByRank(ctx, key, 0, -1001)
	}
	_, err = pipe.Exec(ctx)
	return err
}

func healthHandler(pool *pgxpool.Pool, conn *amqp.Connection, rdb *redis.Client) http.HandlerFunc {
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
		mq := "ok"
		if conn == nil || conn.IsClosed() {
			mq = "error"
		}
		cache := "ok"
		if rdb == nil || rdb.Ping(ctx).Err() != nil {
			cache = "error"
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

func startMetricsHTTP(pool *pgxpool.Pool, conn *amqp.Connection, rdb *redis.Client) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/health", healthHandler(pool, conn, rdb))
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
			slog.Error("worker panic", "recover", r)
			os.Exit(1)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	slog.Info("fanout-worker startup begin")
	mqHost := requiredEnv("MQ_HOST", "MQ_HOST required for worker")
	mqPort := envOrDefault("MQ_PORT", "5672")
	cacheHost := requiredEnv("CACHE_HOST", "CACHE_HOST required for worker")
	cachePort := envOrDefault("CACHE_PORT", "6379")
	slog.Info("startup config resolved", "mqHost", mqHost, "mqPort", mqPort)

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

	// AMQP connection with retry for RabbitMQ startup lag.
	var conn *amqp.Connection
	for attempt := 1; ; attempt++ {
		var err error
		slog.Info("attempting mq dial", "attempt", attempt, "mqHost", mqHost, "mqPort", mqPort)
		conn, err = amqp.Dial(fmt.Sprintf("amqp://cloud9:cloud9@%s:%s/", mqHost, mqPort))
		if err == nil {
			slog.Info("mq dial established", "attempt", attempt)
			break
		}
		if attempt >= 30 {
			slog.Error("mq dial failed after retries", "error", err, "attempts", attempt)
			os.Exit(1)
		}
		delay := time.Duration(attempt) * time.Second
		if delay > 10*time.Second {
			delay = 10 * time.Second
		}
		slog.Warn("mq dial failed, retrying", "error", err, "attempt", attempt, "retryIn", delay)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			os.Exit(0)
		}
	}
	defer conn.Close()

	rdb := redis.NewClient(&redis.Options{
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
	defer rdb.Close()

	ch, err := conn.Channel()
	if err != nil {
		slog.Error("mq channel failed", "error", err)
		os.Exit(1)
	}
	slog.Info("mq channel opened")
	defer ch.Close()

	slog.Info("declaring queue", "queue", "fanout")
	if _, err := ch.QueueDeclare("fanout", true, false, false, false, nil); err != nil {
		slog.Error("queue declare failed", "queue", "fanout", "error", err)
		os.Exit(1)
	}
	slog.Info("queue declared", "queue", "fanout")

	srv := startMetricsHTTP(pool, conn, rdb)
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	if err := ch.Qos(10, 0, false); err != nil {
		slog.Error("failed to set QoS prefetch", "error", err)
		os.Exit(1)
	}

	msgs, err := ch.Consume("fanout", "", false, false, false, false, nil)
	if err != nil {
		slog.Error("mq consume failed", "error", err)
		os.Exit(1)
	}
	slog.Info("mq consumer registered", "queue", "fanout")

	slog.Info("fanout-worker started", "queue", "fanout")
	for {
		select {
		case <-ctx.Done():
			slog.Info("fanout-worker shutting down")
			return
		case d, ok := <-msgs:
			if !ok {
				slog.Error("mq consumer channel closed")
				return
			}
			start := time.Now()
			var msg fanoutMessage
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				workerErrorsTotal.Inc()
				slog.Error("invalid fanout message", "error", err)
				_ = d.Ack(false)
				workerProcessingDuration.Observe(time.Since(start).Seconds())
				continue
			}

			if err := fanoutPost(ctx, pool, rdb, msg); err != nil {
				workerErrorsTotal.Inc()
				slog.Error("fanout failed", "post_id", msg.PostID, "author_id", msg.UserID, "error", err)
			} else {
				workerProcessedTotal.Inc()
			}
			workerProcessingDuration.Observe(time.Since(start).Seconds())
			_ = d.Ack(false)
		}
	}
}
