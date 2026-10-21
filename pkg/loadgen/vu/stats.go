package vu

import (
	"sort"
	"sync"
	"time"
)

const gapRingCap = 1024

// Stats is the JSON snapshot of the VU lane for one run.
type Stats struct {
	Behaviors  map[string]BehaviorStats `json:"behaviors"`
	ReplyGapMs GapSummary               `json:"replyGapMs"`
	Seed       int64                    `json:"seed"`
}

// BehaviorStats counts one behavior's VUs, requests, and errors.
type BehaviorStats struct {
	VUs      int   `json:"vus"`
	Requests int64 `json:"requests"`
	Errors   int64 `json:"errors"`
}

// GapSummary summarizes the time from a root's creation to its reply's creation.
type GapSummary struct {
	Count    int64 `json:"count"`
	P50      int64 `json:"p50"`
	P95      int64 `json:"p95"`
	Under500 int64 `json:"under500"`
}

// Recorder accumulates Stats under a mutex.
type Recorder struct {
	mu        sync.Mutex
	behaviors map[string]BehaviorStats
	seed      int64
	gaps      GapSummary
	ring      [gapRingCap]int64
	ringHead  int
	ringLen   int
}

// NewRecorder returns a recorder that reports seed.
func NewRecorder(seed int64) *Recorder {
	return &Recorder{behaviors: make(map[string]BehaviorStats), seed: seed}
}

// AddVUs adds n VUs to behavior name.
func (r *Recorder) AddVUs(name string, n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	b := r.behaviors[name]
	b.VUs += n
	r.behaviors[name] = b
}

// IncRequest counts one request by behavior name.
func (r *Recorder) IncRequest(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	b := r.behaviors[name]
	b.Requests++
	r.behaviors[name] = b
}

// IncError counts one failed request by behavior name.
func (r *Recorder) IncError(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	b := r.behaviors[name]
	b.Errors++
	r.behaviors[name] = b
}

// ObserveGap records one root-to-reply gap.
func (r *Recorder) ObserveGap(d time.Duration) {
	ms := d.Milliseconds()
	if ms < 0 {
		ms = 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gaps.Count++
	if ms < 500 {
		r.gaps.Under500++
	}
	r.ring[r.ringHead] = ms
	r.ringHead = (r.ringHead + 1) % gapRingCap
	if r.ringLen < gapRingCap {
		r.ringLen++
	}
}

// Snapshot returns a copy safe to marshal.
func (r *Recorder) Snapshot() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := Stats{
		Behaviors:  make(map[string]BehaviorStats, len(r.behaviors)),
		ReplyGapMs: r.gaps,
		Seed:       r.seed,
	}
	for k, v := range r.behaviors {
		out.Behaviors[k] = v
	}
	if r.ringLen > 0 {
		gaps := make([]int64, r.ringLen)
		start := (r.ringHead - r.ringLen + gapRingCap) % gapRingCap
		for i := 0; i < r.ringLen; i++ {
			gaps[i] = r.ring[(start+i)%gapRingCap]
		}
		sort.Slice(gaps, func(i, j int) bool { return gaps[i] < gaps[j] })
		out.ReplyGapMs.P50 = percentile(gaps, 50)
		out.ReplyGapMs.P95 = percentile(gaps, 95)
	}
	return out
}

func percentile(sorted []int64, p int) int64 {
	rank := (p*len(sorted) + 99) / 100
	if rank < 1 {
		rank = 1
	}
	return sorted[rank-1]
}
