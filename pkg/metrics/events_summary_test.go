package metrics

import (
	"strings"
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

func TestBuildEventsSummary_RetainsInvariant(t *testing.T) {
	t.Parallel()
	ts := time.Now()
	hist := []DeploymentMetrics{{
		Timestamp: ts,
		Events: []Event{{
			Timestamp: ts,
			Type:      "invariant",
			RuleID:    "oversell",
			Title:     "Oversell",
			Narrative: "sold > capacity",
		}},
	}}
	got := BuildEventsSummary(hist, 10)
	if len(got) != 1 {
		t.Fatalf("want 1 row, got %d", len(got))
	}
	if got[0]["type"] != "invariant" || got[0]["ruleId"] != "oversell" {
		t.Fatalf("row: %#v", got[0])
	}
}

func TestBuildEventsSummary_InvariantSortOrder(t *testing.T) {
	t.Parallel()
	ts := time.Now()
	hist := []DeploymentMetrics{{
		Timestamp: ts,
		Events: []Event{
			{Timestamp: ts, Type: "threshold", RuleID: "db_latency", Title: "DB"},
			{Timestamp: ts, Type: "invariant", RuleID: "oversell", Title: "Over"},
			{Timestamp: ts, Type: "cascade", RuleID: "cascade_cache_db", Title: "Cas"},
		},
	}}
	got := BuildEventsSummary(hist, 10)
	if len(got) != 3 {
		t.Fatalf("want 3 rows, got %d", len(got))
	}
	if got[0]["type"] != "cascade" || got[1]["type"] != "invariant" || got[2]["type"] != "threshold" {
		t.Fatalf("sort order: %#v", got)
	}
}

func TestBuildEventsSummary_RetainsDetail(t *testing.T) {
	t.Parallel()
	t1 := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Minute)
	hist := []DeploymentMetrics{
		{Timestamp: t1, Events: []Event{{
			Timestamp: t1,
			Type:      "invariant",
			RuleID:    "cache_db_stale",
			Title:     "Stale cache",
			Detail:    map[string]string{"kind": "cache_db_stale", "delta": "12"},
		}}},
		{Timestamp: t2, Events: []Event{{
			Timestamp: t2,
			Type:      "invariant",
			RuleID:    "cache_db_stale",
			Title:     "Stale cache",
			Detail:    map[string]string{"kind": "cache_db_stale", "delta": "99"},
		}, {
			Timestamp: t2,
			Type:      "invariant",
			RuleID:    "failover_write_loss",
			Title:     "Write loss",
			Detail:    map[string]string{"rowsLost": "3"},
		}}},
	}
	got := BuildEventsSummary(hist, 10)
	var stale, failover map[string]any
	for _, row := range got {
		switch row["ruleId"] {
		case "cache_db_stale":
			stale = row
		case "failover_write_loss":
			failover = row
		}
	}
	if stale == nil || failover == nil {
		t.Fatalf("rows: %#v", got)
	}
	staleDetail, ok := stale["detail"].(map[string]string)
	if !ok || staleDetail["delta"] != "99" {
		t.Fatalf("latest detail per rule: %#v", stale["detail"])
	}
	failDetail, ok := failover["detail"].(map[string]string)
	if !ok || failDetail["rowsLost"] != "3" {
		t.Fatalf("failover detail: %#v", failover["detail"])
	}
}

func TestBuildEventsSummary_OmitsEmptyDetail(t *testing.T) {
	t.Parallel()
	ts := time.Now()
	hist := []DeploymentMetrics{{
		Timestamp: ts,
		Events: []Event{{
			Timestamp: ts,
			Type:      "threshold",
			RuleID:    "db_latency",
			Title:     "DB",
		}},
	}}
	got := BuildEventsSummary(hist, 10)
	if len(got) != 1 {
		t.Fatalf("want 1 row, got %d", len(got))
	}
	if _, ok := got[0]["detail"]; ok {
		t.Fatalf("detail key should be omitted: %#v", got[0])
	}
}

func TestBuildEventsSummary_CapsDetailValueLength(t *testing.T) {
	t.Parallel()
	ts := time.Now()
	long := strings.Repeat("a", 300)
	hist := []DeploymentMetrics{{
		Timestamp: ts,
		Events: []Event{{
			Timestamp: ts,
			Type:      "threshold",
			RuleID:    "cache_hit_low",
			Detail:    map[string]string{"note": long},
		}},
	}}
	got := BuildEventsSummary(hist, 10)
	detail, ok := got[0]["detail"].(map[string]string)
	if !ok {
		t.Fatalf("detail: %#v", got[0]["detail"])
	}
	if len(detail["note"]) != eventSummaryDetailMaxLen {
		t.Fatalf("cap: got len %d want %d", len(detail["note"]), eventSummaryDetailMaxLen)
	}
}

func TestBuildEventsSummary_DetailCopyIsolatedFromEvent(t *testing.T) {
	t.Parallel()
	ts := time.Now()
	detail := map[string]string{"delta": "1"}
	ev := Event{
		Timestamp: ts,
		Type:      "invariant",
		RuleID:    "cache_db_stale",
		Detail:    detail,
	}
	hist := []DeploymentMetrics{{Timestamp: ts, Events: []Event{ev}}}
	got := BuildEventsSummary(hist, 10)
	detail["delta"] = "mutated"
	rowDetail, ok := got[0]["detail"].(map[string]string)
	if !ok || rowDetail["delta"] != "1" {
		t.Fatalf("summary should not alias event detail: %#v", got[0]["detail"])
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
