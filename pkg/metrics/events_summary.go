package metrics

import (
	"sort"
	"time"
)

const defaultEventsSummaryMax = 10

// BuildEventsSummary scans downsampled session history (oldest → newest) for threshold and
// cascade events. Each ruleId appears at most once, using the latest snapshot's copy. The slice
// is sorted with cascades before thresholds, then by timestamp descending, capped at maxEntries
// (if maxEntries <= 0, 10 is used). Each map is JSON-friendly for metrics_summary persistence.
func BuildEventsSummary(hist []DeploymentMetrics, maxEntries int) []map[string]any {
	if maxEntries <= 0 {
		maxEntries = defaultEventsSummaryMax
	}
	type acc struct {
		ts         time.Time
		typ        string
		ruleID     string
		title      string
		component  string
		narrative  string
		suggestion string
		related    []string
	}
	byRule := make(map[string]acc)
	for _, snap := range hist {
		for _, ev := range snap.Events {
			if ev.Type != "threshold" && ev.Type != "cascade" {
				continue
			}
			if ev.RuleID == "" {
				continue
			}
			byRule[ev.RuleID] = acc{
				ts:         ev.Timestamp,
				typ:        ev.Type,
				ruleID:     ev.RuleID,
				title:      ev.Title,
				component:  ev.Component,
				narrative:  ev.Narrative,
				suggestion: ev.Suggestion,
				related:    append([]string(nil), ev.Related...),
			}
		}
	}
	out := make([]acc, 0, len(byRule))
	for _, v := range byRule {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool {
		ti, tj := out[i].typ, out[j].typ
		if ti != tj {
			if ti == "cascade" {
				return true
			}
			if tj == "cascade" {
				return false
			}
			return ti < tj
		}
		return out[i].ts.After(out[j].ts)
	})
	if len(out) > maxEntries {
		out = out[:maxEntries]
	}
	rows := make([]map[string]any, len(out))
	for i, v := range out {
		rows[i] = map[string]any{
			"timestamp":  v.ts.UTC().Format(time.RFC3339Nano),
			"type":       v.typ,
			"ruleId":     v.ruleID,
			"title":      v.title,
			"component":  v.component,
			"narrative":  v.narrative,
			"suggestion": v.suggestion,
			"related":    v.related,
		}
	}
	return rows
}
