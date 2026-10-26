DO $$
DECLARE
    missing_parent bigint;
    bad_time bigint;
    bad_root bigint;
    roots_not_self bigint;
    rooted_posts bigint;
    reply_rows bigint;
    roots_with_replies bigint;
    seq_last bigint;
    max_id bigint;
BEGIN
    SELECT COUNT(*) INTO missing_parent
    FROM posts r
    WHERE r.reply_to_id IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM posts p WHERE p.id = r.reply_to_id);
    IF missing_parent > 0 THEN
        RAISE EXCEPTION 'replies with missing parent: %', missing_parent;
    END IF;

    SELECT COUNT(*) INTO bad_time
    FROM posts r
    JOIN posts p ON p.id = r.reply_to_id
    WHERE r.created_at <= p.created_at;
    IF bad_time > 0 THEN
        RAISE EXCEPTION 'replies not after parent: %', bad_time;
    END IF;

    SELECT COUNT(*) INTO bad_root
    FROM posts r
    LEFT JOIN posts root ON root.id = r.thread_root_id
    WHERE r.reply_to_id IS NOT NULL
      AND (root.id IS NULL OR root.reply_to_id IS NOT NULL);
    IF bad_root > 0 THEN
        RAISE EXCEPTION 'invalid thread_root_id on replies: %', bad_root;
    END IF;

    SELECT COUNT(*) INTO roots_not_self
    FROM posts p
    WHERE p.reply_to_id IS NULL
      AND (p.thread_root_id IS NULL OR p.thread_root_id <> p.id);
    IF roots_not_self > 0 THEN
        RAISE EXCEPTION 'root posts without thread_root_id = id: %', roots_not_self;
    END IF;

    SELECT COUNT(*) INTO rooted_posts
    FROM posts p
    WHERE p.reply_to_id IS NULL AND p.thread_root_id = p.id;
    IF rooted_posts = 0 THEN
        RAISE EXCEPTION 'no rooted posts';
    END IF;

    SELECT COUNT(*) INTO roots_with_replies
    FROM posts p
    WHERE p.reply_to_id IS NULL
      AND p.id <= 100000
      AND (p.id % 20) < 3
      AND EXISTS (SELECT 1 FROM posts r WHERE r.reply_to_id = p.id);
    IF roots_with_replies <> 15000 THEN
        RAISE EXCEPTION 'expected 15000 seeded roots with replies, got %', roots_with_replies;
    END IF;

    SELECT COUNT(*) INTO reply_rows FROM posts WHERE reply_to_id IS NOT NULL;
    IF reply_rows < 60000 OR reply_rows > 120000 THEN
        RAISE EXCEPTION 'reply row count out of range: %', reply_rows;
    END IF;

    SELECT last_value INTO seq_last FROM posts_id_seq;
    SELECT COALESCE(MAX(id), 0) INTO max_id FROM posts;
    IF seq_last < max_id THEN
        RAISE EXCEPTION 'posts_id_seq behind max id: seq=% max=%', seq_last, max_id;
    END IF;
END $$;
