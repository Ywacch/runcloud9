package main

import "context"

// fetchFeedItems resolves the social feed: Redis materialized timeline when cache is present
// (written by fanout-worker with MQ, or by handlePost sync ZADD without MQ), else DB pull.
// When cache is present, push mode is strict: no DB fallback on empty or missing timeline.
func fetchFeedItems(
	ctx context.Context,
	cache *Cache,
	pageSize, userID int,
	stats *feedCacheStats,
	pull func(ctx context.Context, userID, limit int) ([]FeedItem, error),
) (items []FeedItem, mode string, err error) {
	if cache != nil {
		// Redis timeline — MQ is not required to read from Redis.
		mode = "push-redis"
		items, err = cache.GetTimeline(ctx, userID, pageSize)
		if err != nil {
			return nil, mode, err
		}
		if len(items) == 0 {
			stats.recordTimelineMiss()
			feedRequestTotal.WithLabelValues("push-redis").Inc()
			return []FeedItem{}, mode, nil
		}
		stats.recordTimelineHit()
		feedRequestTotal.WithLabelValues("push-redis").Inc()
		return items, mode, nil
	}
	feedRequestTotal.WithLabelValues("pull").Inc()
	items, err = pull(ctx, userID, pageSize)
	return items, "pull", err
}
