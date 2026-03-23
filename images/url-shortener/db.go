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

func (db *DB) SaveURL(ctx context.Context, code, original string) error {
	// UPSERT keeps inserts idempotent (e.g. retries, concurrent writers).
	// We only update original; hit_count/created_at should remain intact.
	_, err := db.pool.Exec(ctx, `
		INSERT INTO urls (code, original)
		VALUES ($1, $2)
		ON CONFLICT (code) DO UPDATE
		SET original = EXCLUDED.original
	`, code, original)
	return err
}

func (db *DB) GetURL(ctx context.Context, code string) (URLStats, error) {
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
	_, err := db.pool.Exec(ctx, `
		UPDATE urls
		SET hit_count = hit_count + 1
		WHERE code = $1
	`, code)
	return err
}

func (db *DB) URLExists(ctx context.Context, code string) (bool, error) {
	var exists bool
	err := db.pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM urls WHERE code = $1
		)
	`, code).Scan(&exists)
	return exists, err
}
