package signals

import (
	"slices"
	"sort"
	"strings"
)

// ControlPlaneMetricsPrefix is kept on cloud9-api scrapes alongside catalog metrics.
const ControlPlaneMetricsPrefix = "cloud9_"

// KeepList returns sorted unique raw metric names referenced by catalog Metrics fields.
func KeepList() []string {
	seen := make(map[string]struct{})
	for _, s := range Catalog() {
		for _, m := range s.Metrics {
			m = strings.TrimSpace(m)
			if m == "" {
				continue
			}
			seen[m] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for m := range seen {
		out = append(out, m)
	}
	sort.Strings(out)
	return out
}

// prometheusKeepExtra are raw names still read by legacy PromQL paths until P3.9.
// They are included in PrometheusKeepRegex only; they are not part of KeepList.
var prometheusKeepExtra = []string{
	"container_cpu_usage_seconds_total",
	"container_memory_working_set_bytes",
	"consistency_read_stale_total",
	"db_query_duration_seconds_bucket",
	"db_query_duration_seconds_count",
	"db_query_duration_seconds_sum",
	"db_read_total",
	"monotonic_violations_total",
	"replica_stale_reads_total",
	"replication_replay_lag_replay_lag_bytes",
	"replication_replay_lag_replay_lag_seconds",
	"ryw_leader_redirects_total",
	"ryw_violations_total",
	"social_thread_orphan_reads_total",
	"social_thread_reads_total",
	"up",
	"wal_lsn_bytes",
	"wal_replay_lsn_bytes",
}

// PrometheusKeepRegex is the metric_relabel_configs keep regex for session pod scrapes.
func PrometheusKeepRegex() string {
	parts := append(slices.Clone(KeepList()), prometheusKeepExtra...)
	parts = append(parts, ControlPlaneMetricsPrefix+".*")
	sort.Strings(parts)
	return "^(?:" + strings.Join(parts, "|") + ")$"
}
