package main

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	defaultWriteBehindIntervalSeconds = 30
	minWriteBehindIntervalSeconds     = 1
	maxWriteBehindIntervalSeconds     = 300
)

// parseWriteBehindIntervalSeconds interprets WRITE_BEHIND_INTERVAL.
// Valid values are clamped to 1–300 seconds. Missing, malformed, or
// non-positive values fall back to 30 so time.NewTicker never receives 0.
func parseWriteBehindIntervalSeconds(raw string) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return defaultWriteBehindIntervalSeconds
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < minWriteBehindIntervalSeconds {
		return defaultWriteBehindIntervalSeconds
	}
	if n > maxWriteBehindIntervalSeconds {
		return maxWriteBehindIntervalSeconds
	}
	return n
}

func writeBehindInterval(d time.Duration) time.Duration {
	// Panic guard only: loadConfig already clamps env to 1–300.
	// Non-positive durations (misconfiguration or tests that inject zero)
	// must never reach time.NewTicker.
	if d <= 0 {
		return time.Duration(defaultWriteBehindIntervalSeconds) * time.Second
	}
	return d
}

func shouldStartWriteBehindFlusher(strategy string) bool {
	return strategy == "write-behind"
}

func pendingWriteKey(code string) string {
	return "wb:" + code
}

// storePendingWrite records a durable write-behind payload under wb:{code}.
// The key has no Redis TTL: the flusher deletes it after a successful DB
// write, so a short cache-entry TTL cannot silently drop unflushed writes.
func storePendingWrite(ctx context.Context, rdb *redis.Client, code, value string) error {
	return rdb.Set(ctx, pendingWriteKey(code), value, 0).Err()
}

func runWriteBehindFlusher(ctx context.Context, interval time.Duration, flush func(context.Context)) {
	interval = writeBehindInterval(interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			flush(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func flushPendingWrites(ctx context.Context, rdb *redis.Client, store urlPersister) {
	var cursor uint64
	for {
		keys, nextCursor, err := rdb.Scan(ctx, cursor, "wb:*", 100).Result()
		if err != nil {
			slog.Error("write-behind scan failed", "error", err)
			return
		}
		for _, key := range keys {
			code := strings.TrimPrefix(key, "wb:")
			original, err := rdb.Get(ctx, key).Result()
			if err != nil {
				continue
			}
			if err := store.SaveURL(ctx, code, original); err != nil {
				slog.Error("write-behind flush failed", "code", code, "error", err)
				continue
			}
			if err := rdb.Del(ctx, key).Err(); err != nil {
				slog.Error("write-behind pending-key delete failed", "code", code, "error", err)
			}
		}
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
}
