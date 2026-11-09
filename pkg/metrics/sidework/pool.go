// Package sidework limits concurrent direct HTTP scrapes and shared forensic work.
package sidework

import (
	"context"
	"sync/atomic"
	"time"
)

// Pool caps how many side-work tasks may run at once process-wide.
type Pool struct {
	sem        chan struct{}
	slots      int
	now        func() time.Time
	inflight   atomic.Int32
	onInflight func(int)
}

// New builds a pool with the given slot count. now is used for tests; nil uses time.Now.
func New(slots int, now func() time.Time) *Pool {
	if slots < 1 {
		slots = 1
	}
	if now == nil {
		now = time.Now
	}
	return &Pool{
		sem:   make(chan struct{}, slots),
		slots: slots,
		now:   now,
	}
}

// Slots returns the configured capacity.
func (p *Pool) Slots() int {
	if p == nil {
		return 0
	}
	return p.slots
}

// SetInflightObserver is invoked whenever inflight changes (for Prometheus gauges).
func (p *Pool) SetInflightObserver(fn func(int)) {
	if p == nil {
		return
	}
	p.onInflight = fn
}

// Inflight returns the number of held slots.
func (p *Pool) Inflight() int {
	if p == nil {
		return 0
	}
	return int(p.inflight.Load())
}

// Acquire waits until a slot is free, ctx ends, or wait elapses.
// When ok is false the caller must skip the work; release is a no-op.
func (p *Pool) Acquire(ctx context.Context, wait time.Duration) (release func(), ok bool) {
	noop := func() {}
	if p == nil {
		return noop, true
	}
	if ctx == nil {
		ctx = context.Background()
	}

	acquired := func() (func(), bool) {
		p.inflight.Add(1)
		if p.onInflight != nil {
			p.onInflight(int(p.inflight.Load()))
		}
		return p.releaseFunc(), true
	}

	if wait <= 0 {
		select {
		case <-ctx.Done():
			return noop, false
		case p.sem <- struct{}{}:
			return acquired()
		default:
			return noop, false
		}
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return noop, false
	case p.sem <- struct{}{}:
		return acquired()
	case <-timer.C:
		return noop, false
	}
}

func (p *Pool) releaseFunc() func() {
	return func() {
		p.inflight.Add(-1)
		if p.onInflight != nil {
			p.onInflight(int(p.inflight.Load()))
		}
		select {
		case <-p.sem:
		default:
		}
	}
}
