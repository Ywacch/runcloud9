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
	Observer   ObserverStats            `json:"observer"`
	Seed       int64                    `json:"seed"`
}

// ObserverStats counts thread_observer reads and reply-before-parent sightings.
type ObserverStats struct {
	ThreadReads int64                `json:"threadReads"`
	WindowReads int64                `json:"windowReads"`
	Violations  int64                `json:"violations"`
	Examples    []ObservationExample `json:"examples,omitempty"`
	Errors      int64                `json:"errors"`
}

// ObservationExample is one orphan reply sighting (newest first in stats).
type ObservationExample struct {
	At       time.Time `json:"at"`
	RootID   int       `json:"rootId"`
	ReplyID  int       `json:"replyId"`
	ParentID int       `json:"parentId"`
	ServedBy struct {
		Replies string `json:"replies"`
		Parents string `json:"parents"`
	} `json:"servedBy"`
	RequestID string `json:"requestId"`
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
	observer  ObserverStats
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

// IncObserverRead counts one thread read; inWindow is true when the root was young enough.
func (r *Recorder) IncObserverRead(inWindow bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.observer.ThreadReads++
	if inWindow {
		r.observer.WindowReads++
	}
}

// IncObserverError counts a failed observer read.
func (r *Recorder) IncObserverError() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.observer.Errors++
}

// IncObserverViolation counts a read that saw at least one orphan reply.
func (r *Recorder) IncObserverViolation() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.observer.Violations++
}

// PrependObserverExample keeps the newest examples first (at most five).
func (r *Recorder) PrependObserverExample(ex ObservationExample) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.observer.Examples = append([]ObservationExample{ex}, r.observer.Examples...)
	if len(r.observer.Examples) > maxObservationExamples {
		r.observer.Examples = r.observer.Examples[:maxObservationExamples]
	}
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
		Observer:   r.observer,
		Seed:       r.seed,
	}
	if len(r.observer.Examples) > 0 {
		out.Observer.Examples = append([]ObservationExample(nil), r.observer.Examples...)
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
