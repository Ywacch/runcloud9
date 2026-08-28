package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ywacch/cloud9/pkg/templateapp/postgres"
	"github.com/ywacch/cloud9/pkg/templateapp/readrouter"
	"github.com/ywacch/cloud9/pkg/templateapp/trace"
)

type URLRecord struct {
	Code      string
	Original  string
	CreatedAt time.Time
}

type readQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type readSplitRouter interface {
	QueryRead(
		ctx context.Context,
		clientID string,
		build func(readTokenExpr string) (sql string, args []any),
		scan func(row pgx.Row, replayToken *string) error,
	) (readrouter.Route, string, error)
	AfterWrite(clientID string, t readrouter.ConsistencyToken)
}

type DB struct {
	pool      *pgxpool.Pool
	readPools []*pgxpool.Pool
	router    readSplitRouter
	engine    readrouter.Engine
}

func newDB(pool *pgxpool.Pool, readPools []*pgxpool.Pool, router readSplitRouter) *DB {
	return &DB{
		pool:      pool,
		readPools: readPools,
		router:    router,
		engine:    readrouter.PostgresEngine{},
	}
}

func (db *DB) Close() {
	for _, p := range db.readPools {
		if p != nil {
			p.Close()
		}
	}
}

func routerActive(r readSplitRouter) bool {
	if r == nil {
		return false
	}
	if ptr, ok := r.(*readrouter.ReadRouter); ok {
		return ptr != nil
	}
	return true
}

func (db *DB) readQuerier(ctx context.Context) (readQuerier, string) {
	if !routerActive(db.router) {
		return db.pool, "postgres"
	}
	if r, ok := db.router.(*readrouter.ReadRouter); ok {
		return r.ReadTarget(trace.ClientID(ctx))
	}
	return db.pool, "postgres"
}

func (db *DB) recordWriteToken(ctx context.Context, clientID string) error {
	if !routerActive(db.router) || clientID == "" {
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

func (db *DB) SaveURL(ctx context.Context, code, original string) error {
	return postgres.Observe(ctx, "save_url", "postgres", func() error {
		_, err := db.pool.Exec(ctx, `
			INSERT INTO urls (code, original)
			VALUES ($1, $2)
			ON CONFLICT (code) DO UPDATE
			SET original = EXCLUDED.original
		`, code, original)
		if err != nil {
			return err
		}
		return db.recordWriteToken(ctx, trace.ClientID(ctx))
	})
}

func (db *DB) GetURL(ctx context.Context, code string) (URLRecord, error) {
	if !routerActive(db.router) {
		var rec URLRecord
		err := postgres.Observe(ctx, "get_url", "postgres", func() error {
			var err error
			rec, err = db.getURLLeader(ctx, code)
			return err
		})
		return rec, err
	}

	clientID := trace.ClientID(ctx)
	var rec URLRecord
	err := postgres.ObserveTarget(ctx, "get_url", func() (string, error) {
		_, target, err := db.router.QueryRead(ctx, clientID,
			func(tokenExpr string) (string, []any) {
				return getURLQuery(tokenExpr), []any{code}
			},
			func(row pgx.Row, replayToken *string) error {
				if replayToken != nil {
					return row.Scan(&rec.Code, &rec.Original, &rec.CreatedAt, replayToken)
				}
				return row.Scan(&rec.Code, &rec.Original, &rec.CreatedAt)
			},
		)
		return target, err
	})
	if err != nil {
		return URLRecord{}, err
	}
	return rec, nil
}

func (db *DB) getURLLeader(ctx context.Context, code string) (URLRecord, error) {
	row := db.pool.QueryRow(ctx, getURLQuery(""), code)
	var rec URLRecord
	if err := row.Scan(&rec.Code, &rec.Original, &rec.CreatedAt); err != nil {
		return URLRecord{}, err
	}
	return rec, nil
}

func (db *DB) URLExists(ctx context.Context, code string) (bool, error) {
	if !routerActive(db.router) {
		var exists bool
		err := postgres.Observe(ctx, "url_exists", "postgres", func() error {
			var err error
			exists, err = db.urlExistsLeader(ctx, code)
			return err
		})
		return exists, err
	}

	clientID := trace.ClientID(ctx)
	var exists bool
	err := postgres.ObserveTarget(ctx, "url_exists", func() (string, error) {
		_, target, err := db.router.QueryRead(ctx, clientID,
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
		return target, err
	})
	return exists, err
}

func (db *DB) urlExistsLeader(ctx context.Context, code string) (bool, error) {
	var exists bool
	err := db.pool.QueryRow(ctx, urlExistsQuery(""), code).Scan(&exists)
	return exists, err
}

func (db *DB) ReadThrough(ctx context.Context, cacheKey string, rules []compiledProxyRule) (string, error) {
	var payload string
	err := postgres.ObserveTarget(ctx, "db_read_through", func() (string, error) {
		q, target := db.readQuerier(ctx)
		p, err := proxyReadThrough(ctx, q, rules, cacheKey)
		payload = p
		return target, err
	})
	return payload, err
}

func (db *DB) ListCodes(ctx context.Context, limit int) ([]string, error) {
	var codes []string
	err := postgres.ObserveTarget(ctx, "list_codes", func() (string, error) {
		q, target := db.readQuerier(ctx)
		rows, err := q.Query(ctx, `SELECT code FROM urls ORDER BY code LIMIT $1`, limit)
		if err != nil {
			return target, err
		}
		defer rows.Close()
		codes = codes[:0]
		for rows.Next() {
			var code string
			if err := rows.Scan(&code); err != nil {
				return target, err
			}
			codes = append(codes, code)
		}
		return target, rows.Err()
	})
	return codes, err
}

func (db *DB) Ping(ctx context.Context) error {
	return postgres.Observe(ctx, "ping", "postgres", func() error {
		var n int
		return db.pool.QueryRow(ctx, "SELECT 1").Scan(&n)
	})
}
