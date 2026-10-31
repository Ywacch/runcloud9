CREATE TABLE IF NOT EXISTS timelines (
  user_id    bigint      NOT NULL,
  post_id    bigint      NOT NULL,
  author_id  bigint      NOT NULL,
  payload    jsonb       NOT NULL,
  created_at timestamptz NOT NULL,
  PRIMARY KEY (user_id, created_at, post_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS timelines_user_post ON timelines (user_id, post_id);
