package metrics

import (
	"context"

	"github.com/ywacch/cloud9/pkg/metrics/signals"
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

func (c *MetricsCollector) timelineAppendRate(ctx context.Context, ns, _, engine string) (float64, bool) {
	id, ok := timelineAppendRateSignal(engine)
	if !ok {
		return 0, false
	}
	return c.sig(ctx, ns, id)
}

func (c *MetricsCollector) timelineAppendP99Sec(ctx context.Context, ns, _, engine string) (float64, bool) {
	id, ok := timelineAppendP99Signal(engine)
	if !ok {
		return 0, false
	}
	return c.sig(ctx, ns, id)
}

func (c *MetricsCollector) timelineRangeP99Sec(ctx context.Context, ns, _, engine string) (float64, bool) {
	id, ok := timelineRangeP99Signal(engine)
	if !ok {
		return 0, false
	}
	return c.sig(ctx, ns, id)
}

func (c *MetricsCollector) timelineOpErrorsRate(ctx context.Context, ns, _, engine string) (float64, bool) {
	id, ok := timelineOpErrorsRateSignal(engine)
	if !ok {
		return 0, false
	}
	return c.sig(ctx, ns, id)
}

func (c *MetricsCollector) timelineRelationBytes(ctx context.Context, ns string) (float64, bool) {
	return c.sig(ctx, ns, signals.TimelineRelationBytes)
}

func (c *MetricsCollector) timelineDeadTuples(ctx context.Context, ns string) (float64, bool) {
	return c.sig(ctx, ns, signals.TimelineDeadTuples)
}
