package metrics

import (
	"testing"
	"time"
)

func TestBuildWorkloadEventsSummary_FiltersDedupCap(t *testing.T) {
	t.Parallel()
	t1 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Minute)
	t3 := t1.Add(2 * time.Minute)
	t4 := t1.Add(3 * time.Minute)

	hist := []DeploymentMetrics{
		{Timestamp: t1, Events: []Event{
			{Timestamp: t1, Type: "action", Source: "loadgen", Kind: "loadgen_start", Title: "start",
				Detail: map[string]string{"rps": "100"}, Audience: "both"},
			{Timestamp: t1, Type: "threshold", RuleID: "cache_hit_low", Title: "ignore"},
		}},
		{Timestamp: t2, Events: []Event{
			// Duplicate of t1 start (same kind+timestamp) should be skipped.
			{Timestamp: t1, Type: "action", Source: "loadgen", Kind: "loadgen_start", Title: "start-dup"},
			{Timestamp: t2, Type: "action", Source: "chaos", Kind: "chaos_inject", Component: "cache",
				Title: "kill", Detail: map[string]string{"scenario": "pod_kill"}, Audience: "ai"},
		}},
		{Timestamp: t3, Events: []Event{
			{Timestamp: t3, Type: "action", Source: "loadgen", Kind: "loadgen_stop", Title: "stop"},
		}},
		{Timestamp: t4, Events: []Event{
			{Timestamp: t4, Type: "action", Source: "runtime", Title: "pod restart"}, // no Kind
		}},
	}

	got := BuildWorkloadEventsSummary(hist, 2)
	if len(got) != 2 {
		t.Fatalf("cap: want 2 most recent, got %d (%#v)", len(got), got)
	}
	// Cap keeps newest: chaos_inject @ t2 and loadgen_stop @ t3
	if got[0]["kind"] != "chaos_inject" {
		t.Fatalf("want chaos_inject first of capped, got %v", got[0]["kind"])
	}
	if got[1]["kind"] != "loadgen_stop" {
		t.Fatalf("want loadgen_stop second, got %v", got[1]["kind"])
	}

	full := BuildWorkloadEventsSummary(hist, 20)
	if len(full) != 3 {
		t.Fatalf("want 3 unique anchors, got %d", len(full))
	}
	if full[0]["kind"] != "loadgen_start" {
		t.Fatalf("oldest first: %v", full[0]["kind"])
	}
	detail, ok := full[0]["detail"].(map[string]string)
	if !ok || detail["rps"] != "100" {
		t.Fatalf("detail passthrough: %#v", full[0]["detail"])
	}
	if full[0]["audience"] != "both" {
		t.Fatalf("audience: %v", full[0]["audience"])
	}
	if full[1]["component"] != "cache" {
		t.Fatalf("component: %v", full[1]["component"])
	}
	if full[2]["audience"] != "both" {
		t.Fatalf("empty audience defaults to both: %v", full[2]["audience"])
	}
}

func TestBuildWorkloadEventsSummary_Empty(t *testing.T) {
	t.Parallel()
	if got := BuildWorkloadEventsSummary(nil, 10); len(got) != 0 {
		t.Fatalf("nil hist: %#v", got)
	}
	if got := BuildWorkloadEventsSummary([]DeploymentMetrics{{Events: []Event{{Type: "action"}}}}, 10); len(got) != 0 {
		t.Fatalf("no kind: %#v", got)
	}
}
