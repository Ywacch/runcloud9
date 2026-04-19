package main

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type URLStats struct {
	Code      string
	Original  string
	HitCount  int
	CreatedAt time.Time
}

type DB struct {
	pool *pgxpool.Pool
}

func newDB(pool *pgxpool.Pool) *DB {
	return &DB{pool: pool}
}

// InsertURL inserts a row; on primary-key conflict it does nothing and returns inserted=false.
func (db *DB) InsertURL(ctx context.Context, code, original string) (inserted bool, err error) {
	start := time.Now()
	defer func() {
		dbQueryDuration.WithLabelValues("insert_url").Observe(time.Since(start).Seconds())
		logDBOp(ctx, "insert_url", start)
	}()
	tag, err := db.pool.Exec(ctx, `
		INSERT INTO urls (code, original)
		VALUES ($1, $2)
		ON CONFLICT (code) DO NOTHING
	`, code, original)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func (db *DB) GetURL(ctx context.Context, code string) (URLStats, error) {
	start := time.Now()
	defer func() {
		dbQueryDuration.WithLabelValues("get_url").Observe(time.Since(start).Seconds())
		logDBOp(ctx, "get_url", start)
	}()
	row := db.pool.QueryRow(ctx, `
		SELECT code, original, hit_count, created_at
		FROM urls
		WHERE code = $1
	`, code)

	var record URLStats
	if err := row.Scan(&record.Code, &record.Original, &record.HitCount, &record.CreatedAt); err != nil {
		return URLStats{}, err
	}
	return record, nil
}

func (db *DB) IncrementHitCount(ctx context.Context, code string) error {
	start := time.Now()
	defer func() {
		dbQueryDuration.WithLabelValues("increment_hit").Observe(time.Since(start).Seconds())
		logDBOp(ctx, "increment_hit", start)
	}()
	_, err := db.pool.Exec(ctx, `
		UPDATE urls
		SET hit_count = hit_count + 1
		WHERE code = $1
	`, code)
	return err
}

func (db *DB) URLExists(ctx context.Context, code string) (bool, error) {
	start := time.Now()
	defer func() {
		dbQueryDuration.WithLabelValues("url_exists").Observe(time.Since(start).Seconds())
		logDBOp(ctx, "url_exists", start)
	}()
	var exists bool
	err := db.pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM urls WHERE code = $1
		)
	`, code).Scan(&exists)
	return exists, err
}
