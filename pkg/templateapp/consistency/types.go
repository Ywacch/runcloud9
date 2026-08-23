// Package consistency provides out-of-band cache-vs-database verification for template apps.
package consistency

import (
	"context"
	"strings"
	"time"
)

// ReadObservation captures a sampled cache hit on the request path.
type ReadObservation struct {
	Key, Source, Served, RequestID string
}

// Sample is a stale-read evidence row retained for /consistency/samples and logs.
type Sample struct {
	Key       string    `json:"key"`
	Source    string    `json:"source"`
	Served    string    `json:"served"`
	Truth     string    `json:"truth"`
	RequestID string    `json:"requestId"`
	At        time.Time `json:"at"`
}

// TruthReader loads the source-of-truth value for a cache key (primary DB in cache-aside).
type TruthReader func(ctx context.Context, key string) (truth string, err error)

// Config tunes the bounded verifier worker and Prometheus labels.
type Config struct {
	Template      string        // prometheus label, e.g. "ecommerce"
	Source        string        // prometheus label, e.g. "cache"
	Strategy      string        // CACHE_STRATEGY; skip verify when write-behind
	QueueSize     int           // default 32
	SampleCap     int           // default 8
	VerifyTimeout time.Duration // default 1s
	Equal         func(served, truth string) bool
}

// Enabled reports whether cache-vs-DB verification should run for the cache strategy.
func Enabled(strategy string) bool {
	return !strings.EqualFold(strings.TrimSpace(strategy), "write-behind")
}
