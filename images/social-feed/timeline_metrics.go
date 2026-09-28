package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

const timelineKeyScanPattern = "timeline:*"
const timelineKeyReportInterval = 15 * time.Second

func countTimelineKeys(ctx context.Context, client *redis.Client) (int64, error) {
	if client == nil {
		return 0, nil
	}
	var count int64
	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, timelineKeyScanPattern, 100).Result()
		if err != nil {
			return 0, err
		}
		count += int64(len(keys))
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return count, nil
}

// runTimelineKeyReporter periodically SCANs timeline:* keys and updates social_timeline_keys.
func runTimelineKeyReporter(ctx context.Context, client *redis.Client) {
	if client == nil {
		return
	}
	ticker := time.NewTicker(timelineKeyReportInterval)
	defer ticker.Stop()

	report := func() {
		n, err := countTimelineKeys(ctx, client)
		if err != nil {
			slog.Warn("timeline key count failed", "error", err)
			return
		}
		socialTimelineKeys.Set(float64(n))
	}

	report()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			report()
		}
	}
}
