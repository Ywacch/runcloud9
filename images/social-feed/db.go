package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ywacch/cloud9/pkg/templateapp/postgres"
	"github.com/ywacch/cloud9/pkg/templateapp/readrouter"
	"github.com/ywacch/cloud9/pkg/templateapp/trace"
)

// ErrUsernameTaken is returned when CreateUser hits the unique username constraint.
var ErrUsernameTaken = errors.New("username taken")

var ErrNotFound = errors.New("not found")

type User struct {
	ID       int    `json:"id"`
	Username string `json:"username"`
	Bio      string `json:"bio"`
}

type Post struct {
	ID           int       `json:"id"`
	UserID       int       `json:"user_id"`
	Content      string    `json:"content"`
	CreatedAt    time.Time `json:"created_at"`
	ReplyToID    *int      `json:"reply_to_id,omitempty"`
	ThreadRootID *int      `json:"thread_root_id,omitempty"`
}

// Thread bundles replies and parent posts for a thread root read.
type Thread struct {
	RootID   int
	Replies  []Post
	Parents  []Post
	ServedBy struct {
		Replies string `json:"replies"`
		Parents string `json:"parents"`
	}
}

type FeedItem struct {
	PostID    int       `json:"post_id"`
	AuthorID  int       `json:"author_id"`
	Username  string    `json:"username"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

type readSplitRouter interface {
	QueryRead(
		ctx context.Context,
		clientID string,
		build func(readTokenExpr string) (sql string, args []any),
		scan func(row pgx.Row, replayToken *string) error,
	) (readrouter.Route, string, error)
	QueryRows(
		ctx context.Context,
		clientID string,
		run func(ctx context.Context, q readrouter.Querier) error,
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

func (db *DB) queryRows(ctx context.Context, run func(context.Context, readrouter.Querier) error) (readrouter.Route, string, error) {
	if !routerActive(db.router) {
		err := run(ctx, db.pool)
		return readrouter.RouteLeader, "postgres", err
	}
	return db.router.QueryRows(ctx, trace.ClientID(ctx), run)
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

func getUserQuery(tokenExpr string) string {
	if tokenExpr == "" {
		return `SELECT id, username, bio FROM users WHERE id=$1`
	}
	return fmt.Sprintf(`SELECT id, username, bio, %s FROM users WHERE id=$1`, tokenExpr)
}

func (db *DB) InitSchema(ctx context.Context) error {
	return postgres.Observe(ctx, "init_schema", "postgres", func() error {
		_, err := db.pool.Exec(ctx, `
			CREATE TABLE IF NOT EXISTS users (
				id       SERIAL PRIMARY KEY,
				username TEXT NOT NULL UNIQUE,
				bio      TEXT DEFAULT ''
			);

			CREATE TABLE IF NOT EXISTS posts (
				id         SERIAL PRIMARY KEY,
				user_id    INT NOT NULL REFERENCES users(id),
				content    TEXT NOT NULL,
				created_at TIMESTAMPTZ DEFAULT NOW()
			);

			CREATE TABLE IF NOT EXISTS follows (
				follower_id  INT NOT NULL REFERENCES users(id),
				following_id INT NOT NULL REFERENCES users(id),
				PRIMARY KEY (follower_id, following_id)
			);

			CREATE INDEX IF NOT EXISTS idx_posts_user_id ON posts(user_id);
			CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at DESC);
			CREATE INDEX IF NOT EXISTS idx_follows_follower ON follows(follower_id);
			CREATE INDEX IF NOT EXISTS idx_follows_following ON follows(following_id);
		`)
		if err != nil {
			return err
		}
		if _, err := db.pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS pg_stat_statements`); err != nil {
			return fmt.Errorf("create extension pg_stat_statements: %w", err)
		}
		if _, err := db.pool.Exec(ctx, `
			ALTER TABLE posts ADD COLUMN IF NOT EXISTS reply_to_id INT REFERENCES posts(id);
			ALTER TABLE posts ADD COLUMN IF NOT EXISTS thread_root_id INT;
			CREATE INDEX IF NOT EXISTS idx_posts_thread_root ON posts (thread_root_id);
		`); err != nil {
			return fmt.Errorf("migrate posts thread columns: %w", err)
		}
		return nil
	})
}

func (db *DB) SeedIfEmpty(ctx context.Context) error {
	return postgres.Observe(ctx, "seed_if_empty", "postgres", func() error {
		var count int
		if err := db.pool.QueryRow(ctx, `SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
			return err
		}
		if count > 0 {
			return nil
		}

		start := time.Now()
		slog.Warn("seed dump not found — running fallback self-seed")

		if _, err := db.pool.Exec(ctx, `
			INSERT INTO users (username, bio)
			SELECT 'user_' || i, 'Bio for user ' || i
			FROM generate_series(1, 5000) AS i
		`); err != nil {
			return fmt.Errorf("seed users: %w", err)
		}

		if _, err := db.pool.Exec(ctx, `
			INSERT INTO follows (follower_id, following_id)
			SELECT u.id, c.id
			FROM generate_series(11, 5000) AS u(id),
			     generate_series(1, 10) AS c(id)
		`); err != nil {
			return fmt.Errorf("seed celebrity follows: %w", err)
		}

		if _, err := db.pool.Exec(ctx, `
			INSERT INTO follows (follower_id, following_id)
			SELECT DISTINCT
				(11 + (random() * 3989)::int),
				(11 + (random() * 39)::int)
			FROM generate_series(1, 100000)
			ON CONFLICT DO NOTHING
		`); err != nil {
			return fmt.Errorf("seed influencer follows: %w", err)
		}

		if _, err := db.pool.Exec(ctx, `
			INSERT INTO follows (follower_id, following_id)
			SELECT DISTINCT sub.follower, sub.following
			FROM (
				SELECT
					(1 + (random() * 1999)::int) AS follower,
					(51 + (random() * 449)::int) AS following
				FROM generate_series(1, 80000)
			) sub
			WHERE sub.follower != sub.following
			ON CONFLICT DO NOTHING
		`); err != nil {
			return fmt.Errorf("seed power user follows: %w", err)
		}

		if _, err := db.pool.Exec(ctx, `
			INSERT INTO follows (follower_id, following_id)
			SELECT DISTINCT sub.follower, sub.following
			FROM (
				SELECT
					(1 + (random() * 4999)::int) AS follower,
					(1 + (random() * 4999)::int) AS following
				FROM generate_series(1, 35000)
			) sub
			WHERE sub.follower != sub.following
			ON CONFLICT DO NOTHING
		`); err != nil {
			return fmt.Errorf("seed cross follows: %w", err)
		}

		if _, err := db.pool.Exec(ctx, `
			INSERT INTO posts (user_id, content, created_at)
			SELECT
				CASE
					WHEN random() < 0.15 THEN 1 + (random() * 9)::int
					WHEN random() < 0.40 THEN 11 + (random() * 39)::int
					WHEN random() < 0.65 THEN 51 + (random() * 449)::int
					ELSE 501 + (random() * 4499)::int
				END,
				(ARRAY[
					'Just deployed a new microservice architecture',
					'Cache invalidation really is the hardest problem',
					'Latency vs throughput — there is always a tradeoff',
					'Fan-out on write is expensive but reads are dirt cheap',
					'PostgreSQL can handle way more than people think',
					'Monitoring everything in production today — observability matters',
					'Redis saved us 200ms per request on the hot path',
					'Finally grokked Raft consensus after reading the paper twice',
					'Load testing revealed our real bottleneck was the connection pool',
					'Horizontal scaling is not always the answer — profile first',
					'Switched from polling to WebSockets and p99 dropped 10x',
					'Database indexing made a bigger difference than adding replicas',
					'Our queue depth metric predicted the outage 5 minutes early',
					'Eventual consistency is fine until it is not',
					'The CAP theorem is more nuanced than most blog posts suggest',
					'Sharding by user_id solved our write contention immediately',
					'Circuit breakers prevented a cascade failure today',
					'Rate limiting at the edge saved our downstream services',
					'Migrating from monolith to services — week 3 update',
					'Benchmarked our API: 99th percentile is 145ms under 1k RPS'
				])[1 + (random() * 19)::int],
				NOW() - (random() * interval '30 days')
			FROM generate_series(1, 100000)
		`); err != nil {
			return fmt.Errorf("seed posts: %w", err)
		}

		slog.Info("seeded social graph core data", "duration", time.Since(start).String())
		return nil
	})
}

func (db *DB) GetUser(ctx context.Context, id int) (User, error) {
	if !routerActive(db.router) {
		var u User
		err := postgres.Observe(ctx, "get_user", "postgres", func() error {
			var err error
			u, err = db.getUserLeader(ctx, id)
			return err
		})
		return u, err
	}

	clientID := trace.ClientID(ctx)
	var u User
	err := postgres.ObserveTarget(ctx, "get_user", func() (string, error) {
		_, target, err := db.router.QueryRead(ctx, clientID,
			func(tokenExpr string) (string, []any) {
				return getUserQuery(tokenExpr), []any{id}
			},
			func(row pgx.Row, replayToken *string) error {
				if replayToken != nil {
					return row.Scan(&u.ID, &u.Username, &u.Bio, replayToken)
				}
				return row.Scan(&u.ID, &u.Username, &u.Bio)
			},
		)
		if errors.Is(err, pgx.ErrNoRows) {
			return target, ErrNotFound
		}
		return target, err
	})
	if errors.Is(err, ErrNotFound) {
		return User{}, ErrNotFound
	}
	return u, err
}

func (db *DB) getUserLeader(ctx context.Context, id int) (User, error) {
	var u User
	err := db.pool.QueryRow(ctx, getUserQuery(""), id).Scan(&u.ID, &u.Username, &u.Bio)
	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, ErrNotFound
	}
	return u, err
}

func (db *DB) getPostLeader(ctx context.Context, id int) (Post, error) {
	var p Post
	var replyTo, threadRoot *int
	err := db.pool.QueryRow(ctx, `
		SELECT id, user_id, content, created_at, reply_to_id, thread_root_id
		FROM posts WHERE id = $1
	`, id).Scan(&p.ID, &p.UserID, &p.Content, &p.CreatedAt, &replyTo, &threadRoot)
	if errors.Is(err, pgx.ErrNoRows) {
		return Post{}, ErrNotFound
	}
	if err != nil {
		return Post{}, err
	}
	p.ReplyToID = replyTo
	p.ThreadRootID = threadRoot
	return p, nil
}

type postRowScanner interface {
	Scan(dest ...any) error
}

func scanPost(row postRowScanner) (Post, error) {
	var p Post
	var replyTo, threadRoot *int
	if err := row.Scan(&p.ID, &p.UserID, &p.Content, &p.CreatedAt, &replyTo, &threadRoot); err != nil {
		return Post{}, err
	}
	p.ReplyToID = replyTo
	p.ThreadRootID = threadRoot
	return p, nil
}

func (db *DB) InsertPost(ctx context.Context, userID int, content string, replyToID int) (Post, error) {
	var p Post
	err := postgres.Observe(ctx, "insert_post", "postgres", func() error {
		if replyToID <= 0 {
			err := db.pool.QueryRow(ctx,
				`INSERT INTO posts (user_id, content) VALUES ($1, $2)
				 RETURNING id, user_id, content, created_at, reply_to_id, thread_root_id`,
				userID, content,
			).Scan(&p.ID, &p.UserID, &p.Content, &p.CreatedAt, &p.ReplyToID, &p.ThreadRootID)
			if err != nil {
				return err
			}
			if _, err := db.pool.Exec(ctx, `UPDATE posts SET thread_root_id = id WHERE id = $1`, p.ID); err != nil {
				return err
			}
			root := p.ID
			p.ThreadRootID = &root
			return db.recordWriteToken(ctx, trace.ClientID(ctx))
		}

		parent, err := db.getPostLeader(ctx, replyToID)
		if err != nil {
			return err
		}
		threadRoot := parent.ID
		if parent.ThreadRootID != nil && *parent.ThreadRootID > 0 {
			threadRoot = *parent.ThreadRootID
		}
		err = db.pool.QueryRow(ctx, `
			INSERT INTO posts (user_id, content, reply_to_id, thread_root_id)
			VALUES ($1, $2, $3, $4)
			RETURNING id, user_id, content, created_at, reply_to_id, thread_root_id
		`, userID, content, replyToID, threadRoot).Scan(
			&p.ID, &p.UserID, &p.Content, &p.CreatedAt, &p.ReplyToID, &p.ThreadRootID,
		)
		if err != nil {
			return err
		}
		return db.recordWriteToken(ctx, trace.ClientID(ctx))
	})
	return p, err
}

func (db *DB) GetThread(ctx context.Context, rootID int) (Thread, error) {
	out := Thread{RootID: rootID}
	var replies []Post

	err := postgres.ObserveTarget(ctx, "get_thread_replies", func() (string, error) {
		_, target, err := db.queryRows(ctx, func(ctx context.Context, q readrouter.Querier) error {
			rows, err := q.Query(ctx, `
				SELECT id, user_id, content, created_at, reply_to_id, thread_root_id
				FROM posts
				WHERE thread_root_id = $1 AND reply_to_id IS NOT NULL
				ORDER BY created_at ASC
			`, rootID)
			if err != nil {
				return err
			}
			defer rows.Close()
			replies = replies[:0]
			for rows.Next() {
				p, err := scanPost(rows)
				if err != nil {
					return err
				}
				replies = append(replies, p)
			}
			return rows.Err()
		})
		out.ServedBy.Replies = target
		return target, err
	})
	if err != nil {
		return Thread{}, err
	}
	out.Replies = replies

	parentIDs := make([]int, 0, len(replies))
	seen := make(map[int]struct{}, len(replies))
	for _, r := range replies {
		if r.ReplyToID == nil {
			continue
		}
		id := *r.ReplyToID
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		parentIDs = append(parentIDs, id)
	}

	var parents []Post
	err = postgres.ObserveTarget(ctx, "get_thread_parents", func() (string, error) {
		_, target, err := db.queryRows(ctx, func(ctx context.Context, q readrouter.Querier) error {
			var rows pgx.Rows
			var err error
			if len(parentIDs) == 0 {
				rows, err = q.Query(ctx, `
					SELECT id, user_id, content, created_at, reply_to_id, thread_root_id
					FROM posts WHERE id = $1
				`, rootID)
			} else {
				rows, err = q.Query(ctx, `
					SELECT id, user_id, content, created_at, reply_to_id, thread_root_id
					FROM posts WHERE id = ANY($1::int[])
					ORDER BY id ASC
				`, parentIDs)
			}
			if err != nil {
				return err
			}
			defer rows.Close()
			parents = parents[:0]
			for rows.Next() {
				p, err := scanPost(rows)
				if err != nil {
					return err
				}
				parents = append(parents, p)
			}
			return rows.Err()
		})
		out.ServedBy.Parents = target
		return target, err
	})
	if err != nil {
		return Thread{}, err
	}
	out.Parents = parents
	return out, nil
}

func (db *DB) GetFeedPull(ctx context.Context, userID, limit int) ([]FeedItem, error) {
	var out []FeedItem
	err := postgres.ObserveTarget(ctx, "get_feed_pull", func() (string, error) {
		_, target, err := db.queryRows(ctx, func(ctx context.Context, q readrouter.Querier) error {
			rows, err := q.Query(ctx, `
				SELECT p.id, p.user_id, u.username, p.content, p.created_at
				FROM posts p
				JOIN follows f ON f.following_id = p.user_id
				JOIN users u ON u.id = p.user_id
				WHERE f.follower_id = $1
				ORDER BY p.created_at DESC
				LIMIT $2
			`, userID, limit)
			if err != nil {
				return err
			}
			defer rows.Close()
			out = out[:0]
			for rows.Next() {
				var item FeedItem
				if err := rows.Scan(&item.PostID, &item.AuthorID, &item.Username, &item.Content, &item.CreatedAt); err != nil {
					return err
				}
				out = append(out, item)
			}
			return rows.Err()
		})
		return target, err
	})
	return out, err
}

func (db *DB) Follow(ctx context.Context, followerID, followingID int) error {
	return postgres.Observe(ctx, "follow", "postgres", func() error {
		_, err := db.pool.Exec(ctx,
			`INSERT INTO follows (follower_id, following_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
			followerID, followingID,
		)
		if err != nil {
			return err
		}
		return db.recordWriteToken(ctx, trace.ClientID(ctx))
	})
}

func (db *DB) Unfollow(ctx context.Context, followerID, followingID int) error {
	return postgres.Observe(ctx, "unfollow", "postgres", func() error {
		_, err := db.pool.Exec(ctx,
			`DELETE FROM follows WHERE follower_id = $1 AND following_id = $2`,
			followerID, followingID,
		)
		return err
	})
}

func (db *DB) CreateUser(ctx context.Context, username string) (User, error) {
	var u User
	err := postgres.Observe(ctx, "create_user", "postgres", func() error {
		err := db.pool.QueryRow(ctx,
			`INSERT INTO users (username) VALUES ($1) RETURNING id, username`,
			username,
		).Scan(&u.ID, &u.Username)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return ErrUsernameTaken
			}
			return err
		}
		return nil
	})
	if errors.Is(err, ErrUsernameTaken) {
		return User{}, ErrUsernameTaken
	}
	return u, err
}

func escapeLikePattern(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `%`, `\%`)
	s = strings.ReplaceAll(s, `_`, `\_`)
	return s
}

func (db *DB) SearchUsersByUsername(ctx context.Context, query string, limit int) ([]User, error) {
	if limit <= 0 {
		limit = 20
	}
	var out []User
	err := postgres.ObserveTarget(ctx, "search_users", func() (string, error) {
		pattern := "%" + escapeLikePattern(query) + "%"
		_, target, err := db.queryRows(ctx, func(ctx context.Context, q readrouter.Querier) error {
			rows, err := q.Query(ctx,
				`SELECT id, username, bio FROM users WHERE username ILIKE $1 ESCAPE '\' ORDER BY username LIMIT $2`,
				pattern, limit,
			)
			if err != nil {
				return err
			}
			defer rows.Close()
			out = out[:0]
			for rows.Next() {
				var u User
				if err := rows.Scan(&u.ID, &u.Username, &u.Bio); err != nil {
					return err
				}
				out = append(out, u)
			}
			return rows.Err()
		})
		return target, err
	})
	return out, err
}

func (db *DB) ListUsers(ctx context.Context, limit int) ([]User, error) {
	if limit <= 0 {
		limit = 20
	}
	var out []User
	err := postgres.ObserveTarget(ctx, "list_users", func() (string, error) {
		_, target, err := db.queryRows(ctx, func(ctx context.Context, q readrouter.Querier) error {
			rows, err := q.Query(ctx,
				`SELECT id, username, bio FROM users ORDER BY id LIMIT $1`,
				limit,
			)
			if err != nil {
				return err
			}
			defer rows.Close()
			out = out[:0]
			for rows.Next() {
				var u User
				if err := rows.Scan(&u.ID, &u.Username, &u.Bio); err != nil {
					return err
				}
				out = append(out, u)
			}
			return rows.Err()
		})
		return target, err
	})
	return out, err
}

func (db *DB) GetFollowers(ctx context.Context, userID int) ([]int, error) {
	var ids []int
	err := postgres.ObserveTarget(ctx, "get_followers", func() (string, error) {
		_, target, err := db.queryRows(ctx, func(ctx context.Context, q readrouter.Querier) error {
			rows, err := q.Query(ctx,
				`SELECT DISTINCT follower_id FROM follows WHERE following_id = $1`,
				userID,
			)
			if err != nil {
				return err
			}
			defer rows.Close()
			ids = ids[:0]
			for rows.Next() {
				var id int
				if err := rows.Scan(&id); err != nil {
					return err
				}
				ids = append(ids, id)
			}
			return rows.Err()
		})
		return target, err
	})
	return ids, err
}

func (db *DB) QueryUserPool(ctx context.Context, sql string) ([]int, error) {
	var ids []int
	err := postgres.ObserveTarget(ctx, "list_user_ids", func() (string, error) {
		_, target, err := db.queryRows(ctx, func(ctx context.Context, q readrouter.Querier) error {
			rows, err := q.Query(ctx, sql)
			if err != nil {
				return err
			}
			defer rows.Close()
			ids = ids[:0]
			for rows.Next() {
				var id int
				if err := rows.Scan(&id); err != nil {
					return err
				}
				ids = append(ids, id)
			}
			return rows.Err()
		})
		return target, err
	})
	return ids, err
}

func (db *DB) Ping(ctx context.Context) error {
	return postgres.Observe(ctx, "ping", "postgres", func() error {
		var n int
		return db.pool.QueryRow(ctx, "SELECT 1").Scan(&n)
	})
}
