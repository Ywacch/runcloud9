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
	ID        int       `json:"id"`
	UserID    int       `json:"user_id"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

type FeedItem struct {
	PostID    int       `json:"post_id"`
	AuthorID  int       `json:"author_id"`
	Username  string    `json:"username"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

type DB struct {
	pool *pgxpool.Pool
}

func newDB(pool *pgxpool.Pool) *DB { return &DB{pool: pool} }

func (db *DB) InitSchema(ctx context.Context) error {
	start := time.Now()
	defer func() { dbQueryDuration.WithLabelValues("init_schema").Observe(time.Since(start).Seconds()) }()
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
	// Requires Postgres started with shared_preload_libraries=pg_stat_statements (see deployer db spec).
	if _, err := db.pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS pg_stat_statements`); err != nil {
		return fmt.Errorf("create extension pg_stat_statements: %w", err)
	}
	return nil
}

func (db *DB) SeedIfEmpty(ctx context.Context) error {
	start := time.Now()
	defer func() { dbQueryDuration.WithLabelValues("seed_if_empty").Observe(time.Since(start).Seconds()) }()

	var count int
	if err := db.pool.QueryRow(ctx, `SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return nil // seeder already populated the database
	}

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

	slog.Info("seeded social graph core data",
		"duration", time.Since(start).String(),
	)
	return nil
}

func (db *DB) GetUser(ctx context.Context, id int) (User, error) {
	start := time.Now()
	defer func() {
		dbQueryDuration.WithLabelValues("get_user").Observe(time.Since(start).Seconds())
		logDBOp(ctx, "get_user", start)
	}()
	var u User
	err := db.pool.QueryRow(ctx, `SELECT id, username, bio FROM users WHERE id=$1`, id).Scan(&u.ID, &u.Username, &u.Bio)
	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, ErrNotFound
	}
	return u, err
}

func (db *DB) InsertPost(ctx context.Context, userID int, content string) (Post, error) {
	start := time.Now()
	defer func() {
		dbQueryDuration.WithLabelValues("insert_post").Observe(time.Since(start).Seconds())
		logDBOp(ctx, "insert_post", start)
	}()
	var p Post
	err := db.pool.QueryRow(ctx,
		`INSERT INTO posts (user_id, content) VALUES ($1, $2) RETURNING id, user_id, content, created_at`,
		userID, content,
	).Scan(&p.ID, &p.UserID, &p.Content, &p.CreatedAt)
	return p, err
}

func (db *DB) GetFeedPull(ctx context.Context, userID, limit int) ([]FeedItem, error) {
	start := time.Now()
	defer func() {
		dbQueryDuration.WithLabelValues("get_feed_pull").Observe(time.Since(start).Seconds())
		logDBOp(ctx, "get_feed_pull", start)
	}()
	// follows has PRIMARY KEY (follower_id, following_id) — no DISTINCT needed.
	rows, err := db.pool.Query(ctx, `
		SELECT p.id, p.user_id, u.username, p.content, p.created_at
		FROM posts p
		JOIN follows f ON f.following_id = p.user_id
		JOIN users u ON u.id = p.user_id
		WHERE f.follower_id = $1
		ORDER BY p.created_at DESC
		LIMIT $2
	`, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]FeedItem, 0, limit)
	for rows.Next() {
		var item FeedItem
		if err := rows.Scan(&item.PostID, &item.AuthorID, &item.Username, &item.Content, &item.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// Follow inserts a follow edge (idempotent via ON CONFLICT DO NOTHING).
func (db *DB) Follow(ctx context.Context, followerID, followingID int) error {
	start := time.Now()
	defer func() {
		dbQueryDuration.WithLabelValues("follow").Observe(time.Since(start).Seconds())
		logDBOp(ctx, "follow", start)
	}()
	_, err := db.pool.Exec(ctx,
		`INSERT INTO follows (follower_id, following_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		followerID, followingID,
	)
	return err
}

// Unfollow removes a follow edge.
func (db *DB) Unfollow(ctx context.Context, followerID, followingID int) error {
	start := time.Now()
	defer func() {
		dbQueryDuration.WithLabelValues("unfollow").Observe(time.Since(start).Seconds())
		logDBOp(ctx, "unfollow", start)
	}()
	_, err := db.pool.Exec(ctx,
		`DELETE FROM follows WHERE follower_id = $1 AND following_id = $2`,
		followerID, followingID,
	)
	return err
}

// CreateUser inserts a user with the given username.
func (db *DB) CreateUser(ctx context.Context, username string) (User, error) {
	start := time.Now()
	defer func() {
		dbQueryDuration.WithLabelValues("create_user").Observe(time.Since(start).Seconds())
		logDBOp(ctx, "create_user", start)
	}()
	var u User
	err := db.pool.QueryRow(ctx,
		`INSERT INTO users (username) VALUES ($1) RETURNING id, username`,
		username,
	).Scan(&u.ID, &u.Username)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return User{}, ErrUsernameTaken
		}
		return User{}, err
	}
	return u, nil
}

// SearchUsersByUsername returns users whose username contains query (case-insensitive).
func (db *DB) SearchUsersByUsername(ctx context.Context, query string, limit int) ([]User, error) {
	start := time.Now()
	defer func() {
		dbQueryDuration.WithLabelValues("search_users").Observe(time.Since(start).Seconds())
		logDBOp(ctx, "search_users", start)
	}()
	if limit <= 0 {
		limit = 20
	}
	pattern := "%" + escapeLikePattern(query) + "%"
	rows, err := db.pool.Query(ctx,
		`SELECT id, username, bio FROM users WHERE username ILIKE $1 ESCAPE '\' ORDER BY username LIMIT $2`,
		pattern, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]User, 0, limit)
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.ID, &u.Username, &u.Bio); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

func escapeLikePattern(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `%`, `\%`)
	s = strings.ReplaceAll(s, `_`, `\_`)
	return s
}

// ListUsers returns up to limit users ordered by id.
func (db *DB) ListUsers(ctx context.Context, limit int) ([]User, error) {
	start := time.Now()
	defer func() {
		dbQueryDuration.WithLabelValues("list_users").Observe(time.Since(start).Seconds())
		logDBOp(ctx, "list_users", start)
	}()
	if limit <= 0 {
		limit = 20
	}
	rows, err := db.pool.Query(ctx,
		`SELECT id, username, bio FROM users ORDER BY id LIMIT $1`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]User, 0, limit)
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.ID, &u.Username, &u.Bio); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

func (db *DB) GetFollowers(ctx context.Context, userID int) ([]int, error) {
	start := time.Now()
	defer func() {
		dbQueryDuration.WithLabelValues("get_followers").Observe(time.Since(start).Seconds())
		logDBOp(ctx, "get_followers", start)
	}()
	rows, err := db.pool.Query(ctx,
		`SELECT DISTINCT follower_id FROM follows WHERE following_id = $1`,
		userID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
