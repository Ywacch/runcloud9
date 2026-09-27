package metrics

import (
	"sort"
	"time"
)

const (
	defaultEventsSummaryMax  = 10
	eventSummaryDetailMaxLen = 256
)

// BuildEventsSummary scans downsampled session history (oldest → newest) for threshold, cascade,
// and invariant events. Each ruleId appears at most once, using the latest snapshot's copy. The slice
// is sorted with cascades before invariants before thresholds, then by timestamp descending, capped at maxEntries
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
		detail     map[string]string
	}
	byRule := make(map[string]acc)
	for _, snap := range hist {
		for _, ev := range snap.Events {
			if ev.Type != "threshold" && ev.Type != "cascade" && ev.Type != "invariant" {
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
				detail:     cloneEventSummaryDetail(ev.Detail),
			}
		}
	}
	out := make([]acc, 0, len(byRule))
	for _, v := range byRule {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool {
		ri, rj := eventSummaryRank(out[i].typ), eventSummaryRank(out[j].typ)
		if ri != rj {
			return ri < rj
		}
		return out[i].ts.After(out[j].ts)
	})
	if len(out) > maxEntries {
		out = out[:maxEntries]
	}
	rows := make([]map[string]any, len(out))
	for i, v := range out {
		row := map[string]any{
			"timestamp":  v.ts.UTC().Format(time.RFC3339Nano),
			"type":       v.typ,
			"ruleId":     v.ruleID,
			"title":      v.title,
			"component":  v.component,
			"narrative":  v.narrative,
			"suggestion": v.suggestion,
			"related":    v.related,
		}
		if len(v.detail) > 0 {
			row["detail"] = v.detail
		}
		rows[i] = row
	}
	return rows
}

func cloneEventSummaryDetail(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		if len(v) > eventSummaryDetailMaxLen {
			v = v[:eventSummaryDetailMaxLen]
		}
		out[k] = v
	}
	return out
}

func eventSummaryRank(typ string) int {
	switch typ {
	case "cascade":
		return 0
	case "invariant":
		return 1
	case "threshold":
		return 2
	default:
		return 3
	}
}
