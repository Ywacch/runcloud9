package main

import (
	"context"

	"github.com/ywacch/cloud9/pkg/templateapp/jobs/timeline"
)

// fetchFeedItems resolves the social feed: Redis materialized timeline when timelineStore is set
// (written by fanout-worker with MQ, or by handlePost sync append without MQ), else DB pull.
// When timelineStore is set, push mode is strict: no DB fallback on empty or missing timeline.
func fetchFeedItems(
	ctx context.Context,
	timelineStore timeline.Store,
	pageSize, userID int,
	stats *feedCacheStats,
	pull func(ctx context.Context, userID, limit int) ([]FeedItem, error),
) (items []FeedItem, mode string, err error) {
	if timelineStore != nil {
		mode = "push-redis"
		raw, err := timelineStore.Range(ctx, int64(userID), pageSize)
		if err != nil {
			return nil, mode, err
		}
		items = feedItemsFromTimeline(raw)
		if len(items) == 0 {
			if stats != nil {
				stats.recordTimelineMiss()
			}
			feedRequestTotal.WithLabelValues("push-redis").Inc()
			return []FeedItem{}, mode, nil
		}
		if stats != nil {
			stats.recordTimelineHit()
		}
		feedRequestTotal.WithLabelValues("push-redis").Inc()
		return items, mode, nil
	}
	feedRequestTotal.WithLabelValues("pull").Inc()
	items, err = pull(ctx, userID, pageSize)
	return items, "pull", err
}
