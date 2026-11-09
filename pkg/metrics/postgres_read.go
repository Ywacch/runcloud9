package metrics

import (
	"context"
	"fmt"
	"strings"

	"github.com/ywacch/cloud9/pkg/metrics/signals"
)

// sigSumCompat sums namespace-scalar signals. When no part is present, falls back to legacyPromQL.
// Compare mode uses compareID against the full legacy composite query.
func (c *MetricsCollector) sigSumCompat(ctx context.Context, ns string, legacyPromQL string, compareID signals.ID, ids ...signals.ID) (float64, bool) {
	if c == nil || ns == "" || len(ids) == 0 {
		return 0, false
	}
	var sum float64
	var anyPresent bool
	for _, id := range ids {
		sig, ok := signals.Lookup(id)
		if !ok {
			continue
		}
		v, present := c.readSignalScalar(ctx, ns, sig)
		if present {
			anyPresent = true
			sum += v
		}
	}
	legacyPromQL = strings.TrimSpace(legacyPromQL)
	if !anyPresent && legacyPromQL != "" {
		if v, ok, err := c.queryScalar(ctx, legacyPromQL); err == nil && ok {
			sum, anyPresent = v, true
		}
	}
	out, okOut := applySignalMissing(sum, anyPresent, signals.MissingIsZero)
	if signalCompareEnabled() && legacyPromQL != "" {
		var sigVal float64
		var sigOK bool
		if anyPresent {
			sigVal, sigOK = sum, true
		}
		c.compareSignalScalar(ctx, ns, compareID, sigVal, sigOK, legacyPromQL)
	}
	return out, okOut
}

func (c *MetricsCollector) postgresUserTableReadRate(ctx context.Context, ns string) (float64, bool) {
	return c.sigSumCompat(ctx, ns, postgresUserTableReadRateQuery(ns), signals.PgUserTableSeqTupReadRate,
		signals.PgUserTableSeqTupReadRate, signals.PgUserTableIdxTupFetchRate)
}

func (c *MetricsCollector) postgresUserTableWriteRate(ctx context.Context, ns string) (float64, bool) {
	return c.sigSumCompat(ctx, ns, postgresUserTableWriteRateQuery(ns), signals.PgUserTableNTupInsRate,
		signals.PgUserTableNTupInsRate, signals.PgUserTableNTupUpdRate, signals.PgUserTableNTupDelRate)
}

func (c *MetricsCollector) postgresUserTableReadTotal(ctx context.Context, ns string) (float64, bool) {
	return c.sigSumCompat(ctx, ns, postgresUserTableReadTotal9mQuery(ns), signals.PgUserTableSeqTupReadTotal,
		signals.PgUserTableSeqTupReadTotal, signals.PgUserTableIdxTupFetchTotal)
}

func (c *MetricsCollector) postgresUserTableWriteTotal(ctx context.Context, ns string) (float64, bool) {
	return c.sigSumCompat(ctx, ns, postgresUserTableWriteTotal9mQuery(ns), signals.PgUserTableNTupInsTotal,
		signals.PgUserTableNTupInsTotal, signals.PgUserTableNTupUpdTotal, signals.PgUserTableNTupDelTotal)
}

func (c *MetricsCollector) postgresXactCommitRate(ctx context.Context, ns string) (float64, bool) {
	old := fmt.Sprintf(`sum(rate(pg_stat_database_xact_commit{namespace=%q,datname!=""}%s))`, ns, promRateRange())
	return c.sigCompat(ctx, ns, signals.PgDatabaseXactCommitRate, old)
}

func (c *MetricsCollector) postgresXactRollbackRate(ctx context.Context, ns string) (float64, bool) {
	old := fmt.Sprintf(`sum(rate(pg_stat_database_xact_rollback{namespace=%q,datname!=""}%s))`, ns, promRateRange())
	return c.sigCompat(ctx, ns, signals.PgDatabaseXactRollbackRate, old)
}

func (c *MetricsCollector) postgresXactRateByPod(ctx context.Context, ns string) map[string]float64 {
	if c == nil || ns == "" {
		return nil
	}
	commit := c.sigByPod(ctx, ns, signals.PgDatabaseXactCommitRatePod)
	rollback := c.sigByPod(ctx, ns, signals.PgDatabaseXactRollbackRatePod)
	out := mergePodMaps(commit, rollback)
	if len(out) > 0 {
		return out
	}
	return c.queryMultiResult(ctx, postgresXactRateByPodQuery(ns), "pod")
}

func (c *MetricsCollector) postgresXactTotalByPod(ctx context.Context, ns string) map[string]float64 {
	if c == nil || ns == "" {
		return nil
	}
	commit := c.sigByPod(ctx, ns, signals.PgDatabaseXactCommitTotal)
	rollback := c.sigByPod(ctx, ns, signals.PgDatabaseXactRollbackTotal)
	out := mergePodMaps(commit, rollback)
	if len(out) > 0 {
		return out
	}
	return c.queryMultiResult(ctx, postgresXactTotalByPodQuery(ns), "pod")
}

func mergePodMaps(parts ...map[string]float64) map[string]float64 {
	out := make(map[string]float64)
	for _, m := range parts {
		for pod, v := range m {
			out[pod] += v
		}
	}
	return out
}
