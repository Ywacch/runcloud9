package main

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type URLRecord struct {
	Code      string
	Original  string
	CreatedAt time.Time
}

type DB struct {
	pool *pgxpool.Pool
}

func newDB(pool *pgxpool.Pool) *DB {
	return &DB{pool: pool}
}

func (db *DB) SaveURL(ctx context.Context, code, original string) error {
	// UNDERSTAND: UPSERT keeps write-behind idempotent.
	_, err := db.pool.Exec(ctx, `
		INSERT INTO urls (code, original)
		VALUES ($1, $2)
		ON CONFLICT (code) DO UPDATE
		SET original = EXCLUDED.original
	`, code, original)
	return err
}

func (db *DB) GetURL(ctx context.Context, code string) (URLRecord, error) {
	row := db.pool.QueryRow(ctx, `
		SELECT code, original, created_at
		FROM urls
		WHERE code = $1
	`, code)

	var rec URLRecord
	if err := row.Scan(&rec.Code, &rec.Original, &rec.CreatedAt); err != nil {
		return URLRecord{}, err
	}
	return rec, nil
}

func (db *DB) URLExists(ctx context.Context, code string) (bool, error) {
	var exists bool
	if err := db.pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM urls WHERE code=$1)
	`, code).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}
