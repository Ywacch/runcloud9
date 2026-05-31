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
	if _, ok := s["storage_read_rate"]; !ok {
		t.Fatal("expected storage_read_rate in peak summary")
	}
}

func TestPeakSummary_StoragePeaks(t *testing.T) {
	t.Parallel()
	dm := &DeploymentMetrics{
		Components: map[string]ComponentMetrics{
			"app-server": {RequestRate: 10, P99Latency: 0.01},
			"cache":      {},
			"db": {
				StorageIO: &StorageIO{
					ReadRate:     200,
					WriteRate:    50,
					ReadLatency:  0.002,
					BytesUsed:    900,
					BytesLimit:   1000,
					ErrorRate:    1,
				},
			},
		},
	}
	s := PeakSummary(dm)
	if s["storage_read_rate"] != 200.0 || s["storage_write_rate"] != 50.0 {
		t.Fatalf("rates: %#v", s)
	}
	if s["storage_read_latency_ms"] != 2.0 {
		t.Fatalf("read latency ms: %v", s["storage_read_latency_ms"])
	}
	if s["storage_utilization_pct"] != 90.0 {
		t.Fatalf("util: %v", s["storage_utilization_pct"])
	}
	if s["storage_error_rate"] != 1.0 {
		t.Fatalf("storage err: %v", s["storage_error_rate"])
	}
}

func TestPeakSummaryFromHistory(t *testing.T) {
	t.Parallel()
	var rb RingBuffer
	rb.Add(DeploymentMetrics{
		Components: map[string]ComponentMetrics{
			"app-server": {RequestRate: 10, P99Latency: 0.01, ErrorRate: 0.1, TotalRequests: 100},
			"cache":      {CacheHitsLast9m: 90, CacheMissesLast9m: 10},
			"db":         {DbQueryRate: 5, DbTotalQueries: 50},
		},
	})
	rb.Add(DeploymentMetrics{
		Components: map[string]ComponentMetrics{
			"app-server": {RequestRate: 50, P99Latency: 0.05, ErrorRate: 2, TotalRequests: 500},
			"cache":      {CacheHitsLast9m: 50, CacheMissesLast9m: 50},
			"db":         {DbQueryRate: 20, DbTotalQueries: 200},
		},
	})
	s := PeakSummaryFromHistory(&rb)
	if s["peak_rps"] != 50.0 {
		t.Fatalf("peak_rps: want 50, got %v", s["peak_rps"])
	}
	if s["peak_p99_ms"] != 50.0 {
		t.Fatalf("peak_p99_ms: want 50, got %v", s["peak_p99_ms"])
	}
	if s["error_rate"] != 2.0 {
		t.Fatalf("error_rate: want 2, got %v", s["error_rate"])
	}
	// Worst hit rate: second snap 50%, first 90% → min 50
	if s["cache_hit_rate"] != 50.0 {
		t.Fatalf("cache_hit_rate: want 50 (min), got %v", s["cache_hit_rate"])
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
