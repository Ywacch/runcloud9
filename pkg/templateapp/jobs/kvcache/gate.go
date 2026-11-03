package kvcache

import (
	"sync"
	"time"
)

const (
	cacheFailLimit = 3
	cacheSkipFor   = 3 * time.Second
)

// FailGate skips cache calls after repeated failures.
// The next call after the skip window is a probe.
type FailGate struct {
	mu        sync.Mutex
	fails     int
	skipUntil time.Time
	now       func() time.Time
	limit     int
	skipFor   time.Duration
}

// NewFailGate returns a gate that skips after 3 failures for 3 seconds.
func NewFailGate(now func() time.Time) *FailGate {
	if now == nil {
		now = time.Now
	}
	return &FailGate{
		now:     now,
		limit:   cacheFailLimit,
		skipFor: cacheSkipFor,
	}
}

// Allow reports whether a cache call should run.
func (g *FailGate) Allow() bool {
	if g == nil {
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return !g.now().Before(g.skipUntil)
}

// OK clears the failure count after a successful call.
func (g *FailGate) OK() {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.fails = 0
	g.mu.Unlock()
}

// Fail records one failed call. The third failure opens the skip window.
func (g *FailGate) Fail() {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.fails++
	if g.fails >= g.limit {
		g.skipUntil = g.now().Add(g.skipFor)
		g.fails = 0
	}
}
