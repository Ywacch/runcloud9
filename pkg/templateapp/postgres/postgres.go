// Package postgres provides a thin pgx pool helper and query observer for template apps.
package postgres

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/ywacch/cloud9/pkg/templateapp/trace"
)

// Config holds Postgres connection settings (same env shape as template images).
type Config struct {
	Host     string
	Port     string
	Name     string
	User     string
	Password string
}

var dbQueryDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "db_query_duration_seconds",
		Help:    "Database query latency in seconds",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"operation"},
)

// NewPool builds a pgx pool from cfg and sets MaxConns from DB_POOL_MAX_CONNS (default 25).
func NewPool(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	connURL := &url.URL{
		Scheme: "postgres",
		Host:   fmt.Sprintf("%s:%s", cfg.Host, cfg.Port),
		Path:   "/" + cfg.Name,
		User:   url.UserPassword(cfg.User, cfg.Password),
	}
	poolCfg, err := pgxpool.ParseConfig(connURL.String())
	if err != nil {
		return nil, fmt.Errorf("parse db pool config: %w", err)
	}
	maxConns, convErr := strconv.Atoi(envOrDefault("DB_POOL_MAX_CONNS", "25"))
	if convErr != nil || maxConns < 1 {
		maxConns = 25
	}
	poolCfg.MaxConns = int32(maxConns)
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create db pool: %w", err)
	}
	return pool, nil
}

// Observe times fn, records db_query_duration_seconds, and emits trace logs when enabled.
func Observe(ctx context.Context, op string, fn func() error) error {
	start := time.Now()
	err := fn()
	dbQueryDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())
	trace.LogDB(ctx, op, start)
	return err
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
