package metrics

import (
	"testing"
	"time"
)

func TestSessionAccumulator_Observe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		scrapes []map[string]float64
		want    float64
	}{
		{
			name:    "delta",
			scrapes: []map[string]float64{{"A": 10}, {"A": 20}},
			want:    20,
		},
		{
			name:    "podVanish",
			scrapes: []map[string]float64{{"A": 10, "B": 5}, {"B": 5}},
			want:    15,
		},
		{
			name:    "newPodAfterKill",
			scrapes: []map[string]float64{{"A": 10}, {"B": 0}, {"B": 7}},
			want:    17,
		},
		{
			name:    "resetSameName",
			scrapes: []map[string]float64{{"A": 10}, {"A": 0}, {"A": 3}},
			want:    13,
		},
		{
			name:    "resetNewName",
			scrapes: []map[string]float64{{"A": 10}, {"B": 0}},
			want:    10,
		},
		{
			name:    "emptyScrapeNoop",
			scrapes: []map[string]float64{{"A": 10}, {}, {"A": 12}},
			want:    12,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			acc := newSessionAccumulator()
			const ns = "ns-test"
			var got float64
			for _, scrape := range tc.scrapes {
				got = acc.Observe(ns, seriesHTTP, scrape)
			}
			if got != tc.want {
				t.Fatalf("Observe: want %v, got %v", tc.want, got)
			}
		})
	}
}

func TestSessionAccumulator_TwoNamespaces(t *testing.T) {
	t.Parallel()

	acc := newSessionAccumulator()
	if got := acc.Observe("ns1", seriesHTTP, map[string]float64{"A": 5}); got != 5 {
		t.Fatalf("ns1 first: want 5, got %v", got)
	}
	if got := acc.Observe("ns2", seriesHTTP, map[string]float64{"A": 3}); got != 3 {
		t.Fatalf("ns2 first: want 3, got %v", got)
	}
	if got := acc.Observe("ns1", seriesHTTP, map[string]float64{"A": 8}); got != 8 {
		t.Fatalf("ns1 second: want 8, got %v", got)
	}
	if got := acc.Observe("ns2", seriesHTTP, map[string]float64{"A": 3}); got != 3 {
		t.Fatalf("ns2 unchanged: want 3, got %v", got)
	}
}

func TestSessionAccumulator_Cleanup(t *testing.T) {
	t.Parallel()

	acc := newSessionAccumulator()
	const ns = "ns-cleanup"
	if got := acc.Observe(ns, seriesHTTP, map[string]float64{"A": 10}); got != 10 {
		t.Fatalf("before cleanup: want 10, got %v", got)
	}
	acc.Cleanup(ns)
	if got := acc.Observe(ns, seriesHTTP, map[string]float64{"A": 4}); got != 4 {
		t.Fatalf("after cleanup: want 4 (fresh), got %v", got)
	}
}

func TestSessionAccumulator_NilSafe(t *testing.T) {
	t.Parallel()

	var acc *sessionAccumulator
	if got := acc.Observe("ns", seriesHTTP, map[string]float64{"A": 3}); got != 3 {
		t.Fatalf("nil acc: want 3, got %v", got)
	}
	if got := acc.ObserveDeltas("ns", seriesPgWrite, map[string]float64{"A": 3}); got != 0 {
		t.Fatalf("nil acc ObserveDeltas: want 0, got %v", got)
	}
	acc.Cleanup("ns") // must not panic
	acc.rebaseline("ns", seriesPgWrite, map[string]float64{"postgres": 1})
}

func TestSessionAccumulator_ObserveDeltas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		scrapes []map[string]float64
		want    float64
	}{
		{
			name:    "baselinesFirstScrape",
			scrapes: []map[string]float64{{"postgres": 1_000_000}, {"postgres": 1_000_008}},
			want:    8,
		},
		{
			name:    "redisDeltaAfterBaseline",
			scrapes: []map[string]float64{{"redis-0": 500}, {"redis-0": 550}},
			want:    50,
		},
		{
			name:    "podVanishKeepsPostBaseline",
			scrapes: []map[string]float64{{"A": 100}, {"A": 150}, {"B": 0}, {"B": 20}},
			want:    70,
		},
		{
			name:    "resetAddsNow",
			scrapes: []map[string]float64{{"A": 100}, {"A": 10}},
			want:    10,
		},
		{
			name:    "emptyScrapeNoop",
			scrapes: []map[string]float64{{"A": 10}, {}, {"A": 12}},
			want:    2,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			acc := newSessionAccumulator()
			const ns = "ns-delta"
			var got float64
			for _, scrape := range tc.scrapes {
				got = acc.ObserveDeltas(ns, seriesPgWrite, scrape)
			}
			if got != tc.want {
				t.Fatalf("ObserveDeltas: want %v, got %v", tc.want, got)
			}
		})
	}
}

func TestSessionAccumulator_Rebaseline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		before   []map[string]float64
		snapshot map[string]float64
		after    []map[string]float64
		want     float64
	}{
		{
			name:     "dropsSeedThenCountsDelta",
			before:   []map[string]float64{{"postgres": 0}, {"postgres": 1_000_000}},
			snapshot: map[string]float64{"postgres": 1_000_000},
			after:    []map[string]float64{{"postgres": 1_000_008}},
			want:     8,
		},
		{
			name:     "emptySnapshotRebaselinesNextScrape",
			before:   []map[string]float64{{"postgres": 0}, {"postgres": 1_000_000}},
			snapshot: nil,
			after:    []map[string]float64{{"postgres": 1_000_000}, {"postgres": 1_000_008}},
			want:     8,
		},
		{
			name:     "idempotentWhenCountersUnchanged",
			before:   []map[string]float64{{"postgres": 1_000_000}},
			snapshot: map[string]float64{"postgres": 1_000_000},
			after:    []map[string]float64{{"postgres": 1_000_000}},
			want:     0,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			acc := newSessionAccumulator()
			const ns = "ns-rebaseline"
			for _, scrape := range tc.before {
				_ = acc.ObserveDeltas(ns, seriesPgWrite, scrape)
			}
			acc.rebaseline(ns, seriesPgWrite, tc.snapshot)
			var got float64
			for _, scrape := range tc.after {
				got = acc.ObserveDeltas(ns, seriesPgWrite, scrape)
			}
			if got != tc.want {
				t.Fatalf("after Rebaseline: want %v, got %v", tc.want, got)
			}
		})
	}
}

func TestSessionAccumulator_RebaselineDoesNotTouchHTTP(t *testing.T) {
	t.Parallel()

	acc := newSessionAccumulator()
	const ns = "ns-rebaseline-http"
	if got := acc.Observe(ns, seriesHTTP, map[string]float64{"app": 40}); got != 40 {
		t.Fatalf("http before: want 40, got %v", got)
	}
	_ = acc.ObserveDeltas(ns, seriesPgWrite, map[string]float64{"postgres": 0})
	_ = acc.ObserveDeltas(ns, seriesPgWrite, map[string]float64{"postgres": 1_000_000})
	acc.rebaseline(ns, seriesPgWrite, map[string]float64{"postgres": 1_000_000})
	if got := acc.Observe(ns, seriesHTTP, map[string]float64{"app": 45}); got != 45 {
		t.Fatalf("http after pg rebaseline: want 45, got %v", got)
	}
	if got := acc.ObserveDeltas(ns, seriesPgWrite, map[string]float64{"postgres": 1_000_008}); got != 8 {
		t.Fatalf("pg after rebaseline: want 8, got %v", got)
	}
}

func TestSessionAccumulator_FirstObserve(t *testing.T) {
	t.Parallel()

	acc := newSessionAccumulator()
	const ns = "ns-first"
	before := time.Now()
	acc.Observe(ns, seriesHTTP, map[string]float64{"A": 1})

	acc.mu.Lock()
	fo := acc.ns[ns].firstObserve
	acc.mu.Unlock()
	if fo.IsZero() || fo.Before(before) {
		t.Fatalf("firstObserve not set on Observe: %v", fo)
	}

	first := fo
	acc.ObserveDeltas(ns, seriesPgRead, map[string]float64{"pg": 1})
	acc.mu.Lock()
	fo2 := acc.ns[ns].firstObserve
	acc.mu.Unlock()
	if !fo2.Equal(first) {
		t.Fatalf("firstObserve changed on second series: %v -> %v", first, fo2)
	}

	acc.Cleanup(ns)
	if got := acc.Elapsed(ns, time.Now()); got != 0 {
		t.Fatalf("elapsed after cleanup: want 0, got %v", got)
	}
	acc.Observe(ns, seriesHTTP, map[string]float64{"A": 2})
	acc.mu.Lock()
	fo3 := acc.ns[ns].firstObserve
	acc.mu.Unlock()
	if fo3.IsZero() {
		t.Fatal("firstObserve not set after cleanup+observe")
	}
}

func TestSessionAccumulator_Elapsed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		elapsed  time.Duration
		now      time.Time
		wantZero bool
		want     time.Duration
	}{
		{
			name:     "unset",
			wantZero: true,
		},
		{
			name:    "fiveMinutes",
			elapsed: 5 * time.Minute,
			now:     time.Date(2026, 1, 1, 12, 15, 0, 0, time.UTC),
			want:    5 * time.Minute,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			acc := newSessionAccumulator()
			const ns = "ns-elapsed"
			if tc.wantZero {
				if got := acc.Elapsed(ns, time.Now()); got != 0 {
					t.Fatalf("unset: want 0, got %v", got)
				}
				return
			}
			acc.mu.Lock()
			acc.ns[ns] = &nsSession{
				firstObserve: tc.now.Add(-tc.elapsed),
				series:       make(map[string]*seriesAcc),
			}
			acc.mu.Unlock()
			if got := acc.Elapsed(ns, tc.now); got != tc.want {
				t.Fatalf("Elapsed: want %v, got %v", tc.want, got)
			}
		})
	}
}

func TestSessionP99PromRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input time.Duration
		want  string
	}{
		{name: "zeroUsesMin", input: 0, want: "[1m]"},
		{name: "subMinuteUsesMin", input: 30 * time.Second, want: "[1m]"},
		{name: "fiveMinutes", input: 5 * time.Minute, want: "[5m]"},
		{name: "fifteenMinutes", input: 15 * time.Minute, want: "[15m]"},
		{name: "maxCap", input: 45 * time.Minute, want: "[30m]"},
		{name: "exactMax", input: 30 * time.Minute, want: "[30m]"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := sessionP99PromRange(tc.input); got != tc.want {
				t.Fatalf("sessionP99PromRange(%v): want %q, got %q", tc.input, tc.want, got)
			}
		})
	}
}
