package introspect

import (
	"context"
	"fmt"
	"strings"
)

// StreamsWorkerGroup is the consumer group templates use for Redis Streams work queues.
const StreamsWorkerGroup = "cloud9-workers"

// StreamPeekExtra carries Redis Streams backlog fields for the Data tab.
type StreamPeekExtra struct {
	Pending int64 `json:"pending"`
	Lag     int64 `json:"lag"`
}

// PeekStream returns a non-destructive peek of the latest stream entry.
func (d *RedisDriver) PeekStream(ctx context.Context, stream string) (PeekResult, StreamPeekExtra, error) {
	if d == nil || d.client == nil {
		return PeekResult{}, StreamPeekExtra{}, fmt.Errorf("introspect: redis client is required")
	}
	stream = strings.TrimSpace(stream)
	if stream == "" {
		return PeekResult{}, StreamPeekExtra{}, fmt.Errorf("introspect: stream name is required")
	}

	length, err := d.client.XLen(ctx, stream).Result()
	if err != nil {
		return PeekResult{}, StreamPeekExtra{}, fmt.Errorf("redis xlen %q: %w", stream, err)
	}
	if length == 0 {
		return PeekResult{Queue: stream, Empty: true}, StreamPeekExtra{}, nil
	}

	msgs, err := d.client.XRevRangeN(ctx, stream, "+", "-", 1).Result()
	if err != nil {
		return PeekResult{}, StreamPeekExtra{}, fmt.Errorf("redis xrange %q: %w", stream, err)
	}
	var body string
	contentType := "application/octet-stream"
	if len(msgs) > 0 {
		body = streamFieldBody(msgs[0].Values)
		if ct, ok := msgs[0].Values["contentType"].(string); ok && ct != "" {
			contentType = ct
		}
	}

	extra := StreamPeekExtra{}
	if pend, err := d.client.XPending(ctx, stream, StreamsWorkerGroup).Result(); err == nil {
		extra.Pending = pend.Count
	}
	if groups, err := d.client.XInfoGroups(ctx, stream).Result(); err == nil {
		for _, g := range groups {
			if g.Name == StreamsWorkerGroup {
				extra.Lag = g.Lag
				break
			}
		}
	}

	return PeekResult{
		Queue:       stream,
		Empty:       false,
		Body:        body,
		ContentType: contentType,
		Requeued:    false,
	}, extra, nil
}

func streamFieldBody(values map[string]any) string {
	if values == nil {
		return ""
	}
	if b, ok := values["body"]; ok {
		return fmt.Sprint(b)
	}
	for _, v := range values {
		return fmt.Sprint(v)
	}
	return ""
}
