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
	eng := readrouter.PostgresEngine{}
	return &DB{
		pool:      pool,
		readPools: readPools,
		router:    router,
		engine:    eng,
	}
}

func (db *DB) Close() {
	for _, p := range db.readPools {
		if p != nil {
			p.Close()
		}
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

func routerActive(r readSplitRouter) bool {
	if r == nil {
		return false
	}
	if ptr, ok := r.(*readrouter.ReadRouter); ok {
		return ptr != nil
	}
	return true
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

// InsertURL inserts a row; on primary-key conflict it does nothing and returns inserted=false.
func (db *DB) InsertURL(ctx context.Context, code, original string) (inserted bool, err error) {
	err = postgres.Observe(ctx, "insert_url", "postgres", func() error {
		tag, execErr := db.pool.Exec(ctx, `
			INSERT INTO urls (code, original)
			VALUES ($1, $2)
			ON CONFLICT (code) DO NOTHING
		`, code, original)
		if execErr != nil {
			return execErr
		}
		inserted = tag.RowsAffected() > 0
		if inserted {
			if tokenErr := db.recordWriteToken(ctx, trace.ClientID(ctx)); tokenErr != nil {
				return tokenErr
			}
		}
		return nil
	})
	return inserted, err
}

func (db *DB) GetURL(ctx context.Context, code string) (URLStats, error) {
	if !routerActive(db.router) {
		var record URLStats
		err := postgres.Observe(ctx, "get_url", "postgres", func() error {
			var err error
			record, err = db.getURLLeader(ctx, code)
			return err
		})
		return record, err
	}

	clientID := trace.ClientID(ctx)
	var record URLStats
	err := postgres.ObserveTarget(ctx, "get_url", func() (string, error) {
		_, target, err := db.router.QueryRead(ctx, clientID,
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
		return target, err
	})
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
