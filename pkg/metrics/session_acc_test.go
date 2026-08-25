package metrics

import "testing"

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
	acc.Cleanup("ns") // must not panic
}
