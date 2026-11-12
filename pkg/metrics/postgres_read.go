package metrics

import (
	"context"

	"github.com/ywacch/cloud9/pkg/metrics/signals"
)

func (c *MetricsCollector) sigSum(ctx context.Context, ns string, ids ...signals.ID) (float64, bool) {
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
	return applySignalMissing(sum, anyPresent, signals.MissingIsZero)
}

func (c *MetricsCollector) postgresUserTableReadRate(ctx context.Context, ns string) (float64, bool) {
	return c.sigSum(ctx, ns,
		signals.PgUserTableSeqTupReadRate, signals.PgUserTableIdxTupFetchRate)
}

func (c *MetricsCollector) postgresUserTableWriteRate(ctx context.Context, ns string) (float64, bool) {
	return c.sigSum(ctx, ns,
		signals.PgUserTableNTupInsRate, signals.PgUserTableNTupUpdRate, signals.PgUserTableNTupDelRate)
}

func (c *MetricsCollector) postgresUserTableReadTotal(ctx context.Context, ns string) (float64, bool) {
	return c.sigSum(ctx, ns,
		signals.PgUserTableSeqTupReadTotal, signals.PgUserTableIdxTupFetchTotal)
}

func (c *MetricsCollector) postgresUserTableWriteTotal(ctx context.Context, ns string) (float64, bool) {
	return c.sigSum(ctx, ns,
		signals.PgUserTableNTupInsTotal, signals.PgUserTableNTupUpdTotal, signals.PgUserTableNTupDelTotal)
}

func (c *MetricsCollector) postgresXactCommitRate(ctx context.Context, ns string) (float64, bool) {
	return c.sig(ctx, ns, signals.PgDatabaseXactCommitRate)
}

func (c *MetricsCollector) postgresXactRollbackRate(ctx context.Context, ns string) (float64, bool) {
	return c.sig(ctx, ns, signals.PgDatabaseXactRollbackRate)
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
