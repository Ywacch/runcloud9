package metrics

import (
	"context"
	"fmt"

	"github.com/ywacch/cloud9/pkg/metrics/signals"
	"github.com/ywacch/cloud9/pkg/templateapp/jobs/timeline"
)

func timelineAppendRateSignal(engine string) (signals.ID, bool) {
	switch engine {
	case "postgresql":
		return signals.TimelineAppendRatePostgres, true
	case "redis":
		return signals.TimelineAppendRateRedis, true
	default:
		return "", false
	}
}

func timelineAppendP99Signal(engine string) (signals.ID, bool) {
	switch engine {
	case "postgresql":
		return signals.TimelineAppendP99Postgres, true
	case "redis":
		return signals.TimelineAppendP99Redis, true
	default:
		return "", false
	}
}

func timelineRangeP99Signal(engine string) (signals.ID, bool) {
	switch engine {
	case "postgresql":
		return signals.TimelineRangeP99Postgres, true
	case "redis":
		return signals.TimelineRangeP99Redis, true
	default:
		return "", false
	}
}

func timelineOpErrorsRateSignal(engine string) (signals.ID, bool) {
	switch engine {
	case "postgresql":
		return signals.TimelineOpErrorsRatePostgres, true
	case "redis":
		return signals.TimelineOpErrorsRateRedis, true
	default:
		return "", false
	}
}

func timelineAppendRateLegacy(nsSel, engine string) string {
	return fmt.Sprintf(
		`sum(rate(%s{%s,engine=%q}%s))`,
		timeline.MetricAppendTotal, nsSel, engine, promRateRange(),
	)
}

func timelineAppendP99Legacy(nsSel, engine string) string {
	return fmt.Sprintf(
		`histogram_quantile(0.99, sum by (le) (rate(%s{%s,engine=%q}%s)))`,
		timeline.MetricAppendDuration+"_bucket", nsSel, engine, promRateRange(),
	)
}

func timelineRangeP99Legacy(nsSel, engine string) string {
	return fmt.Sprintf(
		`histogram_quantile(0.99, sum by (le) (rate(%s{%s,engine=%q}%s)))`,
		timeline.MetricRangeDuration+"_bucket", nsSel, engine, promRateRange(),
	)
}

func timelineOpErrorsRateLegacy(nsSel, engine string) string {
	return fmt.Sprintf(
		`sum(rate(%s{%s,engine=%q}%s))`,
		timeline.MetricOpErrorsTotal, nsSel, engine, promRateRange(),
	)
}

func (c *MetricsCollector) timelineAppendRate(ctx context.Context, ns, nsSel, engine string) (float64, bool) {
	id, ok := timelineAppendRateSignal(engine)
	if !ok {
		return 0, false
	}
	return c.sigCompat(ctx, ns, id, timelineAppendRateLegacy(nsSel, engine))
}

func (c *MetricsCollector) timelineAppendP99Sec(ctx context.Context, ns, nsSel, engine string) (float64, bool) {
	id, ok := timelineAppendP99Signal(engine)
	if !ok {
		return 0, false
	}
	return c.sigCompat(ctx, ns, id, timelineAppendP99Legacy(nsSel, engine))
}

func (c *MetricsCollector) timelineRangeP99Sec(ctx context.Context, ns, nsSel, engine string) (float64, bool) {
	id, ok := timelineRangeP99Signal(engine)
	if !ok {
		return 0, false
	}
	return c.sigCompat(ctx, ns, id, timelineRangeP99Legacy(nsSel, engine))
}

func (c *MetricsCollector) timelineOpErrorsRate(ctx context.Context, ns, nsSel, engine string) (float64, bool) {
	id, ok := timelineOpErrorsRateSignal(engine)
	if !ok {
		return 0, false
	}
	return c.sigCompat(ctx, ns, id, timelineOpErrorsRateLegacy(nsSel, engine))
}

func (c *MetricsCollector) timelineRelationBytes(ctx context.Context, ns string) (float64, bool) {
	old := fmt.Sprintf(`max(c9_timeline_relation_bytes{namespace=%q})`, ns)
	return c.sigCompat(ctx, ns, signals.TimelineRelationBytes, old)
}

func (c *MetricsCollector) timelineDeadTuples(ctx context.Context, ns string) (float64, bool) {
	old := fmt.Sprintf(`max(c9_timeline_dead_tuples{namespace=%q})`, ns)
	return c.sigCompat(ctx, ns, signals.TimelineDeadTuples, old)
}
