package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// timelineMaxEntries caps each follower timeline; entries beyond the cap are
// trimmed on every fan-out write. Must match the social-feed app's cap.
const timelineMaxEntries = 1000

// appendTimelineWrite queues the ZADD, trim, and optional EXPIRE commands for
// one follower timeline onto pipe. A ttl of 0 leaves the timeline persistent
// for the pod lifetime (the template's no-expiry default).
func appendTimelineWrite(ctx context.Context, pipe redis.Pipeliner, followerID int, payload []byte, createdAt time.Time, ttl time.Duration) {
	key := fmt.Sprintf("timeline:%d", followerID)
	pipe.ZAdd(ctx, key, redis.Z{
		Score:  float64(createdAt.UnixMilli()),
		Member: string(payload),
	})
	pipe.ZRemRangeByRank(ctx, key, 0, -(timelineMaxEntries + 1))
	if ttl > 0 {
		pipe.Expire(ctx, key, ttl)
	}
}
