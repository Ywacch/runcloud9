package metrics

import (
	"sort"
	"time"
)

const defaultWorkloadEventsMax = 20

// BuildWorkloadEventsSummary scans session history (oldest → newest) for workload anchors
// (events with a non-empty Kind). Each (kind, timestamp) appears once; results are sorted
// by timestamp ascending and capped at maxEntries (default 20 when maxEntries <= 0).
// Output maps are JSON-friendly for metrics_summary persistence.
func BuildWorkloadEventsSummary(hist []DeploymentMetrics, maxEntries int) []map[string]any {
	if maxEntries <= 0 {
		maxEntries = defaultWorkloadEventsMax
	}
	type acc struct {
		ts         time.Time
		kind       string
		source     string
		component  string
		title      string
		detail     map[string]string
		visibility string
	}
	seen := make(map[string]struct{})
	var out []acc
	for _, snap := range hist {
		for _, ev := range snap.Events {
			if ev.Kind == "" {
				continue
			}
			key := ev.Kind + "|" + ev.Timestamp.UTC().Format(time.RFC3339Nano)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			detail := cloneStringMap(ev.Detail)
			visibility := NormalizeVisibility(ev.Visibility)
			out = append(out, acc{
				ts:         ev.Timestamp,
				kind:       ev.Kind,
				source:     ev.Source,
				component:  ev.Component,
				title:      ev.Title,
				detail:     detail,
				visibility: visibility,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ts.Before(out[j].ts)
	})
	if len(out) > maxEntries {
		// Keep the most recent anchors when capped.
		out = out[len(out)-maxEntries:]
	}
	rows := make([]map[string]any, len(out))
	for i, v := range out {
		row := map[string]any{
			"timestamp":  v.ts.UTC().Format(time.RFC3339Nano),
			"kind":       v.kind,
			"source":     v.source,
			"title":      v.title,
			"visibility": v.visibility,
		}
		if v.component != "" {
			row["component"] = v.component
		}
		if len(v.detail) > 0 {
			row["detail"] = v.detail
		}
		rows[i] = row
	}
	return rows
}

func cloneStringMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
