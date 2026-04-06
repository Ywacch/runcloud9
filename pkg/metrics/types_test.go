package metrics

import (
	"testing"
)

func TestLifetimeHitRate(t *testing.T) {
	t.Parallel()
	if got := lifetimeHitRate(ComponentMetrics{}); got != 0 {
		t.Fatalf("empty: want 0, got %v", got)
	}
	if got := lifetimeHitRate(ComponentMetrics{CacheHitsLast9m: 80, CacheMissesLast9m: 20}); got != 80 {
		t.Fatalf("80/100: want 80, got %v", got)
	}
}

func TestPeakSummary(t *testing.T) {
	t.Parallel()
	if got := PeakSummary(nil); got["peak_rps"] != 0.0 {
		t.Fatalf("nil dm: want empty summary, got %#v", got)
	}

	dm := &DeploymentMetrics{
		Components: map[string]ComponentMetrics{
			"app-server": {
				RequestRate:   42,
				P99Latency:    0.0123,
				ErrorRate:     0.5,
				TotalRequests: 9000,
			},
			"cache": {
				CacheHitsLast9m:   700,
				CacheMissesLast9m: 300,
			},
			"db": {
				DbQueryRate:    11,
				DbTotalQueries: 5000,
			},
		},
	}
	s := PeakSummary(dm)
	if s["peak_rps"] != 42.0 {
		t.Fatalf("peak_rps: want 42, got %v", s["peak_rps"])
	}
	// P99 is stored in seconds; summary exposes ms.
	if s["peak_p99_ms"] != 12.3 {
		t.Fatalf("peak_p99_ms: want 12.3, got %v", s["peak_p99_ms"])
	}
	if s["cache_hit_rate"] != 70.0 {
		t.Fatalf("cache_hit_rate: want 70, got %v", s["cache_hit_rate"])
	}
	if s["db_query_rate"] != 11.0 {
		t.Fatalf("db_query_rate: want 11, got %v", s["db_query_rate"])
	}
}

func TestPeakSummary_MissingAppServer(t *testing.T) {
	t.Parallel()
	dm := &DeploymentMetrics{
		Components: map[string]ComponentMetrics{
			"db": {DbQueryRate: 1},
		},
	}
	s := PeakSummary(dm)
	if s["peak_rps"] != 0.0 {
		t.Fatalf("want empty defaults without app-server, got %#v", s)
	}
}
