package metrics

import (
	"testing"
	"time"
)

func TestBuildEventsSummary_DedupSortCap(t *testing.T) {
	t.Parallel()
	t1 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Minute)
	t3 := t1.Add(2 * time.Minute)

	hist := []DeploymentMetrics{
		{Timestamp: t1, Events: []Event{
			{Timestamp: t1, Type: "threshold", RuleID: "cache_hit_low", Title: "Low cache", Narrative: "first"},
		}},
		{Timestamp: t2, Events: []Event{
			{Timestamp: t2, Type: "threshold", RuleID: "cache_hit_low", Title: "Low cache", Narrative: "second"},
			{Timestamp: t2, Type: "cascade", RuleID: "cascade_cache_db", Title: "Cascade", Narrative: "c1"},
		}},
		{Timestamp: t3, Events: []Event{
			{Timestamp: t3, Type: "threshold", RuleID: "db_latency", Title: "DB slow", Narrative: "d1"},
			{Timestamp: t3, Type: "cascade", RuleID: "cascade_latency_miss", Title: "Lat", Narrative: "c2"},
		}},
	}
	got := BuildEventsSummary(hist, 2)
	if len(got) != 2 {
		t.Fatalf("cap: want 2, got %d", len(got))
	}
	if got[0]["type"] != "cascade" || got[1]["type"] != "cascade" {
		t.Fatalf("cascades first: %#v", got)
	}
	// Latest cascade among the two kept by cap: t3 > t2 for cascade_latency_miss vs cascade_cache_db
	if got[0]["ruleId"] != "cascade_latency_miss" {
		t.Fatalf("want newest cascade first, got %v", got[0]["ruleId"])
	}
	if got[1]["ruleId"] != "cascade_cache_db" {
		t.Fatalf("second cascade: %v", got[1]["ruleId"])
	}

	// cache_hit_low should use latest narrative
	full := BuildEventsSummary(hist, 20)
	var hit map[string]any
	for _, row := range full {
		if row["ruleId"] == "cache_hit_low" {
			hit = row
			break
		}
	}
	if hit == nil {
		t.Fatal("missing cache_hit_low")
	}
	if hit["narrative"] != "second" {
		t.Fatalf("dedup latest: %v", hit["narrative"])
	}
}

func TestBuildEventsSummary_SkipsActionAndEmptyRule(t *testing.T) {
	t.Parallel()
	ts := time.Now()
	hist := []DeploymentMetrics{{
		Timestamp: ts,
		Events: []Event{
			{Timestamp: ts, Type: "action", Title: "x"},
			{Timestamp: ts, Type: "threshold", Title: "no id"},
		},
	}}
	got := BuildEventsSummary(hist, 10)
	if len(got) != 0 {
		t.Fatalf("want empty, got %#v", got)
	}
}
