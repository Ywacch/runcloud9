package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ywacch/cloud9/pkg/templateapp/readrouter"
	"github.com/ywacch/cloud9/pkg/templateapp/trace"
)

type URLStats struct {
	Code      string
	Original  string
	CreatedAt time.Time
}

type readSplitRouter interface {
	QueryRead(
		ctx context.Context,
		clientID string,
		build func(readTokenExpr string) (sql string, args []any),
		scan func(row pgx.Row, replayToken *string) error,
	) (readrouter.Route, error)
	AfterWrite(clientID string, t readrouter.ConsistencyToken)
}

type DB struct {
	pool     *pgxpool.Pool
	readPool *pgxpool.Pool
	router   readSplitRouter
	engine   readrouter.Engine
}

func newDB(pool, readPool *pgxpool.Pool, router readSplitRouter) *DB {
	eng := readrouter.PostgresEngine{}
	return &DB{
		pool:     pool,
		readPool: readPool,
		router:   router,
		engine:   eng,
	}
}

func (db *DB) Close() {
	if db.readPool != nil {
		db.readPool.Close()
	}
}

func getURLQuery(tokenExpr string) string {
	if tokenExpr == "" {
		return `
		SELECT code, original, created_at
		FROM urls
		WHERE code = $1
	`
	}
	return fmt.Sprintf(`
		SELECT code, original, created_at, %s
		FROM urls
		WHERE code = $1
	`, tokenExpr)
}

func urlExistsQuery(tokenExpr string) string {
	if tokenExpr == "" {
		return `
		SELECT EXISTS(
			SELECT 1 FROM urls WHERE code = $1
		)
	`
	}
	return fmt.Sprintf(`
		SELECT EXISTS(
			SELECT 1 FROM urls WHERE code = $1
		), %s
	`, tokenExpr)
}

func (db *DB) recordWriteToken(ctx context.Context, clientID string) error {
	if db.router == nil || clientID == "" {
		return nil
	}
	var raw string
	if err := db.pool.QueryRow(ctx, "SELECT "+db.engine.WriteTokenExpr()).Scan(&raw); err != nil {
		return err
	}
	tok, err := db.engine.ParseToken(raw)
	if err != nil {
		return err
	}
	db.router.AfterWrite(clientID, tok)
	return nil
}

// InsertURL inserts a row; on primary-key conflict it does nothing and returns inserted=false.
func (db *DB) InsertURL(ctx context.Context, code, original string) (inserted bool, err error) {
	start := time.Now()
	defer func() {
		dbQueryDuration.WithLabelValues("insert_url").Observe(time.Since(start).Seconds())
		trace.LogDB(ctx, "insert_url", start)
	}()
	tag, err := db.pool.Exec(ctx, `
		INSERT INTO urls (code, original)
		VALUES ($1, $2)
		ON CONFLICT (code) DO NOTHING
	`, code, original)
	if err != nil {
		return false, err
	}
	inserted = tag.RowsAffected() > 0
	if inserted {
		if err := db.recordWriteToken(ctx, trace.ClientID(ctx)); err != nil {
			return true, err
		}
	}
	return inserted, nil
}

func (db *DB) GetURL(ctx context.Context, code string) (URLStats, error) {
	start := time.Now()
	defer func() {
		dbQueryDuration.WithLabelValues("get_url").Observe(time.Since(start).Seconds())
		trace.LogDB(ctx, "get_url", start)
	}()

	if db.router == nil {
		return db.getURLLeader(ctx, code)
	}

	clientID := trace.ClientID(ctx)
	var record URLStats
	_, err := db.router.QueryRead(ctx, clientID,
		func(tokenExpr string) (string, []any) {
			return getURLQuery(tokenExpr), []any{code}
		},
		func(row pgx.Row, replayToken *string) error {
			if replayToken != nil {
				return row.Scan(&record.Code, &record.Original, &record.CreatedAt, replayToken)
			}
			return row.Scan(&record.Code, &record.Original, &record.CreatedAt)
		},
	)
	if err != nil {
		return URLStats{}, err
	}
	return record, nil
}

func (db *DB) getURLLeader(ctx context.Context, code string) (URLStats, error) {
	row := db.pool.QueryRow(ctx, getURLQuery(""), code)
	var record URLStats
	if err := row.Scan(&record.Code, &record.Original, &record.CreatedAt); err != nil {
		return URLStats{}, err
	}
	return record, nil
}

func (db *DB) URLExists(ctx context.Context, code string) (bool, error) {
	start := time.Now()
	defer func() {
		dbQueryDuration.WithLabelValues("url_exists").Observe(time.Since(start).Seconds())
		trace.LogDB(ctx, "url_exists", start)
	}()

	if db.router == nil {
		return db.urlExistsLeader(ctx, code)
	}

	clientID := trace.ClientID(ctx)
	var exists bool
	_, err := db.router.QueryRead(ctx, clientID,
		func(tokenExpr string) (string, []any) {
			return urlExistsQuery(tokenExpr), []any{code}
		},
		func(row pgx.Row, replayToken *string) error {
			if replayToken != nil {
				return row.Scan(&exists, replayToken)
			}
			return row.Scan(&exists)
		},
	)
	return exists, err
}

func (db *DB) urlExistsLeader(ctx context.Context, code string) (bool, error) {
	var exists bool
	err := db.pool.QueryRow(ctx, urlExistsQuery(""), code).Scan(&exists)
	return exists, err
}
