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
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/ywacch/cloud9/pkg/templateapp/jobs/queue"
	"github.com/ywacch/cloud9/pkg/templateapp/trace"
)

var (
	workerProcessedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ecommerce_worker_processed_total",
		Help: "Total orders successfully processed",
	})
	workerErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ecommerce_worker_errors_total",
		Help: "Total failed order processing attempts",
	})
	workerProcessingDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ecommerce_worker_processing_duration_seconds",
		Help:    "Time to process one order",
		Buckets: prometheus.DefBuckets,
	})
)

type orderMessage struct {
	OrderID   int `json:"order_id"`
	ProductID int `json:"product_id"`
	Quantity  int `json:"quantity"`
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

func processOrder(ctx context.Context, pool *pgxpool.Pool, order orderMessage) error {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	dbStart := time.Now()
	tag, err := tx.Exec(ctx,
		`UPDATE products SET stock = stock - $2 WHERE id = $1 AND stock >= $2`,
		order.ProductID, order.Quantity,
	)
	if err != nil {
		return err
	}
	trace.LogWorker(ctx, "db_stock_update", dbStart)
	if tag.RowsAffected() == 0 {
		_, err = tx.Exec(ctx, `UPDATE orders SET status = 'failed' WHERE id = $1`, order.OrderID)
		if err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	if _, err := tx.Exec(ctx, `UPDATE orders SET status = 'fulfilled' WHERE id = $1`, order.OrderID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func healthHandler(pool *pgxpool.Pool, pingQueue func(context.Context) error) http.HandlerFunc {
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
		if pingQueue == nil || pingQueue(ctx) != nil {
			mq = "error"
		}
		status := http.StatusOK
		if db != "ok" || mq != "ok" {
			status = http.StatusInternalServerError
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status": "ok",
			"mq":     mq,
			"db":     db,
		})
	}
}

func startMetricsHTTP(pool *pgxpool.Pool, pingQueue func(context.Context) error) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/health", healthHandler(pool, pingQueue))
	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}
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

	slog.Info("ecommerce-worker startup begin")
	queueCfg := queue.EnvConfig("orders", prometheus.DefaultRegisterer, nil)
	if strings.TrimSpace(queueCfg.HostEnv) == "" {
		slog.Error("WORK_QUEUE_HOST required for worker")
		os.Exit(1)
	}
	slog.Info("startup config resolved",
		"workQueueEngine", queueCfg.EngineEnv,
		"workQueueHost", queueCfg.HostEnv,
		"workQueuePort", queueCfg.PortEnv,
	)

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

	var workQ queue.Queue
	for attempt := 1; ; attempt++ {
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

	pingQueue := func(ctx context.Context) error {
		switch q := workQ.(type) {
		case *queue.RabbitMQ:
			return q.Ping()
		case *queue.Streams:
			return q.Ping(ctx)
		default:
			return fmt.Errorf("unsupported queue engine %q", workQ.Engine())
		}
	}

	srv := startMetricsHTTP(pool, pingQueue)
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	slog.Info("ecommerce-worker started", "queue", "orders", "engine", workQ.Engine())

	err := workQ.Consume(ctx, func(ctx context.Context, body []byte) error {
		start := time.Now()
		var order orderMessage
		if err := json.Unmarshal(body, &order); err != nil {
			workerErrorsTotal.Inc()
			slog.Error("invalid order message", "error", err)
			workerProcessingDuration.Observe(time.Since(start).Seconds())
			return nil
		}
		if err := processOrder(ctx, pool, order); err != nil {
			workerErrorsTotal.Inc()
			slog.Error("order processing failed", "order_id", order.OrderID, "error", err, "request_id", trace.RequestID(ctx))
			workerProcessingDuration.Observe(time.Since(start).Seconds())
			return err
		}
		trace.LogWorker(ctx, "process_order", start)
		workerProcessedTotal.Inc()
		workerProcessingDuration.Observe(time.Since(start).Seconds())
		return nil
	})
	if err != nil && ctx.Err() == nil {
		slog.Error("work queue consume ended", "error", err)
	}
	slog.Info("ecommerce-worker shutting down")
}
