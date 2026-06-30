// Keep aligned with pkg/loadgen/latency_ring.go.
package main

import (
	"math"
	"sync"
)

const recentLatenciesCap = 120

type latencyRing struct {
	mu   sync.Mutex
	buf  [recentLatenciesCap]float64
	n    int
	head int
	full bool
}

func (r *latencyRing) Push(ms float64) {
	if r == nil || !validLatencyMs(ms) {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.full {
		r.buf[r.n] = ms
		r.n++
		if r.n == recentLatenciesCap {
			r.full = true
			r.head = 0
		}
		return
	}
	r.buf[r.head] = ms
	r.head++
	if r.head == recentLatenciesCap {
		r.head = 0
	}
}

func (r *latencyRing) Snapshot() []float64 {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.n == 0 {
		return nil
	}
	if !r.full {
		out := make([]float64, r.n)
		copy(out, r.buf[:r.n])
		return out
	}
	out := make([]float64, recentLatenciesCap)
	tail := recentLatenciesCap - r.head
	copy(out, r.buf[r.head:])
	copy(out[tail:], r.buf[:r.head])
	return out
}

func validLatencyMs(ms float64) bool {
	return ms >= 0 && !math.IsNaN(ms) && !math.IsInf(ms, 0)
}
