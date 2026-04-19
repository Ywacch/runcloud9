package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

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

		CREATE TABLE IF NOT EXISTS feed_items (
			id         SERIAL PRIMARY KEY,
			user_id    INT NOT NULL REFERENCES users(id),
			post_id    INT NOT NULL REFERENCES posts(id),
			author_id  INT NOT NULL REFERENCES users(id),
			content    TEXT NOT NULL,
			created_at TIMESTAMPTZ DEFAULT NOW()
		);

		CREATE INDEX IF NOT EXISTS idx_posts_user_id ON posts(user_id);
		CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at DESC);
		CREATE INDEX IF NOT EXISTS idx_follows_follower ON follows(follower_id);
		CREATE INDEX IF NOT EXISTS idx_follows_following ON follows(following_id);
		CREATE INDEX IF NOT EXISTS idx_feed_items_user ON feed_items(user_id, created_at DESC);
	`)
	return err
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

	slog.Warn("seed dump not found — running fallback self-seed (feed_items will be empty until worker populates them)")

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
	defer func() { dbQueryDuration.WithLabelValues("get_user").Observe(time.Since(start).Seconds()) }()
	var u User
	err := db.pool.QueryRow(ctx, `SELECT id, username, bio FROM users WHERE id=$1`, id).Scan(&u.ID, &u.Username, &u.Bio)
	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, ErrNotFound
	}
	return u, err
}

func (db *DB) InsertPost(ctx context.Context, userID int, content string) (Post, error) {
	start := time.Now()
	defer func() { dbQueryDuration.WithLabelValues("insert_post").Observe(time.Since(start).Seconds()) }()
	var p Post
	err := db.pool.QueryRow(ctx,
		`INSERT INTO posts (user_id, content) VALUES ($1, $2) RETURNING id, user_id, content, created_at`,
		userID, content,
	).Scan(&p.ID, &p.UserID, &p.Content, &p.CreatedAt)
	return p, err
}

func (db *DB) GetFeedPull(ctx context.Context, userID, limit int) ([]FeedItem, error) {
	start := time.Now()
	defer func() { dbQueryDuration.WithLabelValues("get_feed_pull").Observe(time.Since(start).Seconds()) }()
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

func (db *DB) GetFeedPush(ctx context.Context, userID, limit int) ([]FeedItem, error) {
	start := time.Now()
	defer func() { dbQueryDuration.WithLabelValues("get_feed_push").Observe(time.Since(start).Seconds()) }()
	rows, err := db.pool.Query(ctx, `
		SELECT fi.post_id, fi.author_id, u.username, fi.content, fi.created_at
		FROM feed_items fi
		JOIN users u ON u.id = fi.author_id
		WHERE fi.user_id = $1
		ORDER BY fi.created_at DESC
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

func (db *DB) GetFollowers(ctx context.Context, userID int) ([]int, error) {
	start := time.Now()
	defer func() { dbQueryDuration.WithLabelValues("get_followers").Observe(time.Since(start).Seconds()) }()
	rows, err := db.pool.Query(ctx, `SELECT follower_id FROM follows WHERE following_id = $1`, userID)
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

func (db *DB) InsertFeedItem(ctx context.Context, userID, postID, authorID int, content string, createdAt time.Time) error {
	start := time.Now()
	defer func() { dbQueryDuration.WithLabelValues("insert_feed_item").Observe(time.Since(start).Seconds()) }()
	_, err := db.pool.Exec(ctx,
		`INSERT INTO feed_items (user_id, post_id, author_id, content, created_at) VALUES ($1, $2, $3, $4, $5)`,
		userID, postID, authorID, content, createdAt,
	)
	return err
}
