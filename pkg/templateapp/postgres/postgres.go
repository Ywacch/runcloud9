// Package postgres provides a thin pgx pool helper and query observer for template apps.
package postgres

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/ywacch/cloud9/pkg/templateapp/trace"
)

const primaryObserveTarget = "postgres"

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
	[]string{"operation", "target"},
)

// NewPool builds a pgx pool from cfg and sets MaxConns from DB_POOL_MAX_CONNS (default 25).
func NewPool(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	writeMax, _ := SplitPoolMaxConns(0)
	return NewPoolWithMaxConns(ctx, cfg, writeMax)
}

// NewPoolWithMaxConns builds a pgx pool with an explicit MaxConns limit.
func NewPoolWithMaxConns(ctx context.Context, cfg Config, maxConns int32) (*pgxpool.Pool, error) {
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
	if maxConns < 1 {
		maxConns = 1
	}
	poolCfg.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create db pool: %w", err)
	}
	return pool, nil
}

// SplitPoolMaxConns divides DB_POOL_MAX_CONNS across one write pool and replicaCount
// replica pools using integer division with a minimum of 1 connection per pool.
func SplitPoolMaxConns(replicaCount int) (writeMax int32, replicaEachMax int32) {
	total := poolMaxConnsFromEnv()
	poolCount := 1 + replicaCount
	if poolCount < 1 {
		poolCount = 1
	}
	each := total / poolCount
	if each < 1 {
		each = 1
	}
	return int32(each), int32(each)
}

func poolMaxConnsFromEnv() int {
	maxConns, convErr := strconv.Atoi(envOrDefault("DB_POOL_MAX_CONNS", "25"))
	if convErr != nil || maxConns < 1 {
		return 25
	}
	return maxConns
}

// ReplicaConnectHost maps a stable ordinal hostname (postgres-replica-0) to the
// in-cluster StatefulSet DNS name (postgres-replica-0.postgres-replica).
// Histogram Observe targets must keep the bare ordinal so PR9 can map target → layer id.
// Already-qualified names (containing a dot) are returned unchanged.
func ReplicaConnectHost(host string) string {
	host = strings.TrimSpace(host)
	if host == "" || strings.Contains(host, ".") {
		return host
	}
	dash := strings.LastIndex(host, "-")
	if dash <= 0 || dash == len(host)-1 {
		return host
	}
	for _, c := range host[dash+1:] {
		if c < '0' || c > '9' {
			return host
		}
	}
	return host + "." + host[:dash]
}

// NormalizeObserveTarget maps an empty target to the primary hostname label.
func NormalizeObserveTarget(target string) string {
	if strings.TrimSpace(target) == "" {
		return primaryObserveTarget
	}
	return target
}

// Observe times fn, records db_query_duration_seconds{operation,target}, and emits trace logs when enabled.
func Observe(ctx context.Context, op, target string, fn func() error) error {
	start := time.Now()
	err := fn()
	recordQueryDuration(ctx, op, target, start)
	return err
}

// ObserveTarget times fn and records latency under the target hostname fn returns.
// Use for reads where the chosen pool is only known after routing.
func ObserveTarget(ctx context.Context, op string, fn func() (target string, err error)) error {
	start := time.Now()
	target, err := fn()
	recordQueryDuration(ctx, op, target, start)
	return err
}

func recordQueryDuration(ctx context.Context, op, target string, start time.Time) {
	dbQueryDuration.WithLabelValues(op, NormalizeObserveTarget(target)).Observe(time.Since(start).Seconds())
	trace.LogDB(ctx, op, start)
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
