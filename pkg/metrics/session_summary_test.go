package metrics

import (
	"testing"
	"time"
)

func TestSessionSummary_MergesPeakSteadyHistoryAnchors(t *testing.T) {
	t.Parallel()
	c := &MetricsCollector{}
	ns := "ns-summary"
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	c.historyMu.Lock()
	rb := &RingBuffer{}
	for i := 0; i < 5; i++ {
		dm := DeploymentMetrics{
			Timestamp: t0.Add(time.Duration(i) * 10 * time.Second),
			Components: map[string]ComponentMetrics{
				"app-server": {
					RequestRate: float64(10 + i),
					P99Latency:  0.01 * float64(i+1),
					ErrorRate:   0.1,
				},
				"cache": {
					CacheHitsLast9m:   80,
					CacheMissesLast9m: 20,
				},
				"db": {CPUUsage: 0.2, CPULimit: 1.0},
			},
		}
		if i == 1 {
			dm.Events = []Event{{
				Timestamp: t0.Add(10 * time.Second),
				Type:      "action",
				Source:    "loadgen",
				Kind:      "loadgen_start",
				Title:     "start",
				Detail:    map[string]string{"rps": "10"},
			}}
		}
		if i == 2 {
			dm.Events = []Event{{
				Timestamp: t0.Add(20 * time.Second),
				Type:      "threshold",
				RuleID:    "cache_hit_low",
				Title:     "low",
			}}
		}
		rb.Add(dm)
	}
	c.history = map[string]*RingBuffer{ns: rb}
	c.historyMu.Unlock()

	base := map[string]any{"seeded": true}
	last := &DeploymentMetrics{
		Components: map[string]ComponentMetrics{
			"app-server": {RequestRate: 14, P99Latency: 0.05},
		},
	}
	summary := c.SessionSummary(ns, base, last)
	if summary["seeded"] != true {
		t.Fatalf("preserve base: %#v", summary["seeded"])
	}
	if _, ok := summary["peak_p99_ms"]; !ok {
		t.Fatalf("want peak fields: %#v", summary)
	}
	if _, ok := summary["steady_state"]; !ok {
		t.Fatal("want steady_state")
	}
	if _, ok := summary["session_history"]; !ok {
		t.Fatal("want session_history")
	}
	if _, ok := summary["events_summary"]; !ok {
		t.Fatal("want events_summary")
	}
	we, ok := summary["workload_events"].([]map[string]any)
	if !ok || len(we) != 1 || we[0]["kind"] != "loadgen_start" {
		t.Fatalf("workload_events: %#v", summary["workload_events"])
	}
	traj, ok := summary["trajectories"].(map[string]any)
	if !ok || len(traj) == 0 {
		t.Fatalf("trajectories: %#v", summary["trajectories"])
	}
}

func TestSessionSummary_IncludesFreshnessPerComponent(t *testing.T) {
	t.Parallel()
	c := &MetricsCollector{}
	dm := &DeploymentMetrics{
		Components: map[string]ComponentMetrics{
			"app-server": {
				Component: "app-server",
				CPUSource: ResourceSourceMetricsAPI,
				MemSource: ResourceSourceMetricsAPI,
			},
			"mq": {
				Component: "mq",
				CPUSource: ResourceSourceMetricsAPI,
				MemSource: ResourceSourceMetricsAPI,
			},
		},
	}
	summary := c.SessionSummary("ns", nil, dm)
	fresh, ok := summary["freshness"].(map[string]map[string]SignalFreshness)
	if !ok {
		t.Fatalf("freshness type: %T", summary["freshness"])
	}
	if len(fresh) != 2 {
		t.Fatalf("want 2 components, got %d", len(fresh))
	}
	if fresh["app-server"][SignalContainerCPU].Tier != FreshnessMetricsAPI {
		t.Fatalf("app containerCpu: %+v", fresh["app-server"][SignalContainerCPU])
	}
	if fresh["mq"][SignalQueue].Tier != FreshnessDirect {
		t.Fatalf("mq queue: %+v", fresh["mq"][SignalQueue])
	}
}

func TestSessionSummary_NilCollector(t *testing.T) {
	t.Parallel()
	var c *MetricsCollector
	dm := &DeploymentMetrics{
		Components: map[string]ComponentMetrics{
			"app-server": {RequestRate: 1, P99Latency: 0.01},
		},
	}
	summary := c.SessionSummary("ns", map[string]any{"x": 1}, dm)
	if summary["x"] != 1 {
		t.Fatalf("base: %#v", summary)
	}
	if _, ok := summary["peak_p99_ms"]; !ok {
		t.Fatalf("peak fallback: %#v", summary)
	}
	fresh, ok := summary["freshness"].(map[string]map[string]SignalFreshness)
	if !ok || len(fresh) != 1 {
		t.Fatalf("freshness on nil collector: %#v", summary["freshness"])
	}
}
