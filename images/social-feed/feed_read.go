package main

import "context"

// fetchFeedItems resolves the social feed: Redis materialized timeline when cache is present
// (written by fanout-worker with MQ, or by handlePost sync ZADD without MQ), else DB pull.
func fetchFeedItems(ctx context.Context, cache *Cache, pageSize, userID int, pull func(ctx context.Context, userID, limit int) ([]FeedItem, error)) (items []FeedItem, mode string, err error) {
	if cache != nil {
		// Redis timeline — MQ is not required to read from Redis.
		mode = "push-redis"
		items, err = cache.GetTimeline(ctx, userID, pageSize)
		if err != nil || len(items) == 0 {
			mode = "push-fallback"
			feedRequestTotal.WithLabelValues("push-fallback").Inc()
			items, err = pull(ctx, userID, pageSize)
			return items, mode, err
		}
		feedRequestTotal.WithLabelValues("push-redis").Inc()
		return items, mode, nil
	}
	feedRequestTotal.WithLabelValues("pull").Inc()
	items, err = pull(ctx, userID, pageSize)
	return items, "pull", err
}
