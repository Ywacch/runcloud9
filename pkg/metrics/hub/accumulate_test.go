package hub

import (
	"testing"
	"time"
)

func TestAccumulate_RateTimesDt(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	now := start
	a := NewWindowAccumulator(func() time.Time { return now })
	ns := "ns-a"

	a.integrateRate(ns, now, StreamHTTPRequests, 2, true)
	now = start.Add(5 * time.Second)
	a.integrateRate(ns, now, StreamHTTPRequests, 2, true)

	total, ok := a.Total(ns, StreamHTTPRequests)
	if !ok {
		t.Fatal("expected total present")
	}
	if total != 10 {
		t.Fatalf("total: want 10, got %v", total)
	}
}

func TestAccumulate_ResetOnNewHub(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	a := NewWindowAccumulator(func() time.Time { return now })
	a.integrateRate("ns-a", now, StreamHTTPRequests, 5, true)
	b := NewWindowAccumulator(func() time.Time { return now })
	if v, ok := b.Total("ns-a", StreamHTTPRequests); ok || v != 0 {
		t.Fatalf("new hub must not inherit totals: ok=%v v=%v", ok, v)
	}
}

func TestQuantile_MatchesPrometheusInterpolation(t *testing.T) {
	t.Parallel()
	// Cumulative counts after integration; p0.5 should interpolate between 0.1 and 0.5.
	buckets := map[string]float64{
		"0.1":  10,
		"0.5":  40,
		"1.0":  50,
		"+Inf": 50,
	}
	got, ok := histogramQuantile(0.5, buckets)
	if !ok {
		t.Fatal("expected quantile ok")
	}
	// rank = 0.5 * 50 = 25; bucket 0.1 has 10, bucket 0.5 has 40 cumulative at upper 0.5.
	// rank 25 lies in second bucket: 0.1 + (25-10)/30*(0.5-0.1) = 0.1 + 0.2 = 0.3
	want := 0.3
	if got < want-0.001 || got > want+0.001 {
		t.Fatalf("quantile: want ~%v, got %v", want, got)
	}
}
