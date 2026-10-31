// Package timeline defines the timeline job interface shared by template apps.
package timeline

import (
	"context"
	"errors"
	"time"
)

// Prometheus metric names for timeline store operations (referenced by collectors and template YAML).
const (
	MetricAppendTotal     = "timeline_append_total"
	MetricAppendDuration  = "timeline_append_duration_seconds"
	MetricRangeDuration   = "timeline_range_duration_seconds"
	MetricTrimDuration    = "timeline_trim_duration_seconds"
	MetricOpErrorsTotal   = "timeline_op_errors_total"
	MetricOpErrorsLabelOp = "op"
	MetricEngineLabel     = "engine"
)

// Item is one timeline entry. Payload is opaque to adapters.
type Item struct {
	PostID    int64
	AuthorID  int64
	Payload   []byte
	CreatedAt time.Time
}

// Store is the timeline job. Implementations must be safe for concurrent use.
type Store interface {
	// Append adds item to every user's timeline in one batch and trims to the adapter's cap.
	// Re-appending the same PostID for a user must not create a duplicate.
	Append(ctx context.Context, userIDs []int64, item Item) error
	// Range returns up to limit items, newest first.
	Range(ctx context.Context, userID int64, limit int) ([]Item, error)
	// Trim keeps the newest keep items.
	Trim(ctx context.Context, userID int64, keep int) error
	Engine() string
}

// ErrInvalidLimit is returned when Range or Trim is called with a non-positive limit or keep.
var ErrInvalidLimit = errors.New("timeline: limit must be > 0")
