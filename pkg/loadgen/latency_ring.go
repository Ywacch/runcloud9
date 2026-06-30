package loadgen

import (
	"math"
	"sync"
)

// RecentLatenciesCap is how many per-request client latencies LoadStats retains for needle charts.
const RecentLatenciesCap = 120

// latencyRing is a fixed-size FIFO of recent request latencies in milliseconds.
type latencyRing struct {
	mu   sync.Mutex
	buf  [RecentLatenciesCap]float64
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
		if r.n == RecentLatenciesCap {
			r.full = true
			r.head = 0
		}
		return
	}
	r.buf[r.head] = ms
	r.head++
	if r.head == RecentLatenciesCap {
		r.head = 0
	}
}

// Snapshot returns oldest→newest latencies (copy, len ≤ RecentLatenciesCap).
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
	out := make([]float64, RecentLatenciesCap)
	tail := RecentLatenciesCap - r.head
	copy(out, r.buf[r.head:])
	copy(out[tail:], r.buf[:r.head])
	return out
}

func validLatencyMs(ms float64) bool {
	return ms >= 0 && !math.IsNaN(ms) && !math.IsInf(ms, 0)
}
