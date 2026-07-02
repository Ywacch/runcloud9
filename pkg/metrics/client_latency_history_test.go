package metrics

import (
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/loadgen"
)

func TestClientLatencyAccumulator_mergeDedupeAndOrder(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	acc := newClientLatencyAccumulator()

	acc.merge([]loadgen.LoadIntervalStats{
		{At: base.Add(5 * time.Second), P99Ms: 40, ActualRps: 48},
		{At: base, P99Ms: 30, ActualRps: 50},
	})
	acc.merge([]loadgen.LoadIntervalStats{
		{At: base.Add(5 * time.Second), P99Ms: 42, ActualRps: 47}, // same t, overwrite
		{At: base.Add(10 * time.Second), P99Ms: 55, ActualRps: 49},
	})

	snap := acc.snapshot()
	if len(snap) != 3 {
		t.Fatalf("len(snapshot) = %d, want 3", len(snap))
	}
	if snap[0]["p99Ms"] != float64(30) {
		t.Fatalf("oldest p99Ms = %v, want 30", snap[0]["p99Ms"])
	}
	if snap[1]["p99Ms"] != float64(42) {
		t.Fatalf("middle p99Ms = %v, want 42 (deduped overwrite)", snap[1]["p99Ms"])
	}
	if snap[2]["p99Ms"] != float64(55) {
		t.Fatalf("newest p99Ms = %v, want 55", snap[2]["p99Ms"])
	}
}

func TestClientLatencyAccumulator_skipsInvalid(t *testing.T) {
	t.Parallel()

	acc := newClientLatencyAccumulator()
	acc.merge([]loadgen.LoadIntervalStats{
		{At: time.Time{}, P99Ms: 10, ActualRps: 1},
		{At: time.Now().UTC(), P99Ms: -1, ActualRps: 1},
		{At: time.Now().UTC(), P99Ms: 20, ActualRps: 45},
	})
	if len(acc.snapshot()) != 1 {
		t.Fatalf("want 1 valid interval, got %d", len(acc.snapshot()))
	}
}

func TestMetricsCollector_ClientLatencyHistoryFor(t *testing.T) {
	t.Parallel()

	c := NewMetricsCollector("", nil, nil, nil, nil)
	ns := "cloud9-test"
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	c.mergeClientLatencyHistory(ns, []loadgen.LoadIntervalStats{
		{At: t0, P99Ms: 12, ActualRps: 40},
	})
	c.mergeClientLatencyHistory(ns, []loadgen.LoadIntervalStats{
		{At: t0.Add(5 * time.Second), P99Ms: 18, ActualRps: 41},
	})

	hist := c.ClientLatencyHistoryFor(ns)
	if len(hist) != 2 {
		t.Fatalf("len(history) = %d, want 2", len(hist))
	}

	c.Cleanup(ns)
	if got := c.ClientLatencyHistoryFor(ns); got != nil {
		t.Fatalf("after Cleanup expected nil, got %#v", got)
	}
}
