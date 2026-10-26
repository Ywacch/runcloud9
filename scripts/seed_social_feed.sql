-- Schema
CREATE TABLE IF NOT EXISTS users (
    id       SERIAL PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    bio      TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS posts (
    id             SERIAL PRIMARY KEY,
    user_id        INT NOT NULL REFERENCES users(id),
    content        TEXT NOT NULL,
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    reply_to_id    INT REFERENCES posts(id),
    thread_root_id INT
);

CREATE TABLE IF NOT EXISTS follows (
    follower_id  INT NOT NULL REFERENCES users(id),
    following_id INT NOT NULL REFERENCES users(id),
    PRIMARY KEY (follower_id, following_id)
);

CREATE INDEX IF NOT EXISTS idx_posts_user_id ON posts(user_id);
CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_posts_thread_root ON posts (thread_root_id);
CREATE INDEX IF NOT EXISTS idx_follows_follower ON follows(follower_id);
CREATE INDEX IF NOT EXISTS idx_follows_following ON follows(following_id);

SELECT setseed(0.42);

-- 5,000 users
INSERT INTO users (username, bio)
SELECT 'user_' || i, 'Bio for user ' || i
FROM generate_series(1, 5000) AS i;

-- Celebrity follows: everyone 11-5000 follows celebrities 1-10
INSERT INTO follows (follower_id, following_id)
SELECT u.id, c.id
FROM generate_series(11, 5000) AS u(id),
     generate_series(1, 10) AS c(id);

-- Influencer follows
INSERT INTO follows (follower_id, following_id)
SELECT DISTINCT
    (11 + (random() * 3989)::int),
    (11 + (random() * 39)::int)
FROM generate_series(1, 100000)
ON CONFLICT DO NOTHING;

-- Power user follows
INSERT INTO follows (follower_id, following_id)
SELECT DISTINCT sub.follower, sub.following
FROM (
    SELECT
        (1 + (random() * 1999)::int) AS follower,
        (51 + (random() * 449)::int) AS following
    FROM generate_series(1, 80000)
) sub
WHERE sub.follower != sub.following
ON CONFLICT DO NOTHING;

-- Cross follows
INSERT INTO follows (follower_id, following_id)
SELECT DISTINCT sub.follower, sub.following
FROM (
    SELECT
        (1 + (random() * 4999)::int) AS follower,
        (1 + (random() * 4999)::int) AS following
    FROM generate_series(1, 35000)
) sub
WHERE sub.follower != sub.following
ON CONFLICT DO NOTHING;

-- 100,000 posts over 30 days
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
FROM generate_series(1, 100000);

UPDATE posts
SET thread_root_id = id
WHERE reply_to_id IS NULL AND thread_root_id IS NULL;

WITH phrases AS (
    SELECT ARRAY[
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
    ] AS body
),
level1 AS (
    INSERT INTO posts (user_id, content, created_at, reply_to_id, thread_root_id)
    SELECT
        CASE
            WHEN ((p.id * 17 + n) % 20) < 3 THEN 1 + ((p.id + n) % 10)
            WHEN ((p.id * 17 + n) % 20) < 8 THEN 11 + ((p.id + n) % 40)
            WHEN ((p.id * 17 + n) % 20) < 13 THEN 51 + ((p.id + n) % 450)
            ELSE 501 + ((p.id + n) % 4500)
        END,
        (SELECT body FROM phrases)[1 + ((p.id + n) % 20)],
        p.created_at + ((30 + ((p.id * 13 + n * 7) % 21571)) * interval '1 second'),
        p.id,
        p.id
    FROM posts p
    JOIN generate_series(1, 8) AS n ON n <= 1 + (p.id % 8)
    WHERE p.reply_to_id IS NULL
      AND p.id <= 100000
      AND (p.id % 20) < 3
    RETURNING id, thread_root_id, created_at
),
level2 AS (
    INSERT INTO posts (user_id, content, created_at, reply_to_id, thread_root_id)
    SELECT
        CASE
            WHEN ((l.id * 17) % 20) < 3 THEN 1 + (l.id % 10)
            WHEN ((l.id * 17) % 20) < 8 THEN 11 + (l.id % 40)
            WHEN ((l.id * 17) % 20) < 13 THEN 51 + (l.id % 450)
            ELSE 501 + (l.id % 4500)
        END,
        (SELECT body FROM phrases)[1 + (l.id % 20)],
        l.created_at + ((30 + ((l.id * 13) % 21571)) * interval '1 second'),
        l.id,
        l.thread_root_id
    FROM level1 l
    WHERE (l.id % 3) = 0
    RETURNING id, thread_root_id, created_at
)
INSERT INTO posts (user_id, content, created_at, reply_to_id, thread_root_id)
SELECT
    CASE
        WHEN ((l.id * 17) % 20) < 3 THEN 1 + (l.id % 10)
        WHEN ((l.id * 17) % 20) < 8 THEN 11 + (l.id % 40)
        WHEN ((l.id * 17) % 20) < 13 THEN 51 + (l.id % 450)
        ELSE 501 + (l.id % 4500)
    END,
    (SELECT body FROM phrases)[1 + (l.id % 20)],
    l.created_at + ((30 + ((l.id * 13) % 21571)) * interval '1 second'),
    l.id,
    l.thread_root_id
FROM level2 l
WHERE (l.id % 5) = 0;

-- Timeline materialization moved to Redis sorted sets (timeline:{user_id}).
-- This seed script now only loads canonical entities: users, posts, follows.
