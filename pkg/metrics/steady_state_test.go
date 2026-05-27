package metrics

import (
	"testing"
	"time"
)

func TestSteadyStateSummary_SkipsWarmup(t *testing.T) {
	t.Parallel()
	var rb RingBuffer
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 20; i++ {
		rb.Add(DeploymentMetrics{
			Timestamp: base.Add(time.Duration(i*2) * time.Second),
			Components: map[string]ComponentMetrics{
				"app-server": {
					P99Latency:  float64(i) * 0.001,
					RequestRate: float64(i),
				},
				"db": {DbQueryRate: float64(i) * 0.1},
				"cache": {
					CacheHitsLast9m:   float64(i * 10),
					CacheMissesLast9m: float64(100 - i),
				},
			},
		})
	}
	s := SteadyStateSummary(&rb)
	if s["snapshot_count"].(int) != 5 {
		t.Fatalf("snapshot_count = %v, want 5 (20 ticks - 15 warmup)", s["snapshot_count"])
	}
	if s["steady_p99_ms"].(float64) <= 0 {
		t.Fatalf("steady_p99_ms should be > 0, got %v", s["steady_p99_ms"])
	}
}

func TestSteadyStateSummary_EmptyBuffer(t *testing.T) {
	t.Parallel()
	s := SteadyStateSummary(nil)
	if s["snapshot_count"].(int) != 0 {
		t.Fatalf("expected 0 snapshots, got %v", s["snapshot_count"])
	}
}

func TestMedian(t *testing.T) {
	t.Parallel()
	if got := median([]float64{1, 3, 9}); got != 3 {
		t.Fatalf("median odd: got %v want 3", got)
	}
	if got := median([]float64{1, 2, 3, 4}); got != 2.5 {
		t.Fatalf("median even: got %v want 2.5", got)
	}
}
