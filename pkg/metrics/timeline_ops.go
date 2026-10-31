package metrics

import (
	"context"
	"fmt"

	"github.com/ywacch/cloud9/pkg/templateapp/jobs/timeline"
	"github.com/ywacch/cloud9/pkg/templates"
)

func templateHasTimelineJob(tmpl *templates.TemplateSpec) bool {
	if tmpl == nil {
		return false
	}
	for _, job := range tmpl.Jobs {
		if job.ID == "timeline" {
			return true
		}
	}
	return false
}

func (c *MetricsCollector) collectTimelineOps(ctx context.Context, nsSel, template string) *TimelineOps {
	tmpl := templates.GetTemplate(template)
	if !templateHasTimelineJob(tmpl) {
		return nil
	}
	engine := c.detectTimelineEngine(ctx, nsSel)
	if engine == "" {
		return nil
	}
	out := &TimelineOps{Engine: engine}
	rate := promRateRange()

	appendRate, ok, _ := c.queryScalar(ctx, fmt.Sprintf(
		`sum(rate(%s{%s,engine=%q}%s))`,
		timeline.MetricAppendTotal, nsSel, engine, rate,
	))
	if ok {
		out.AppendRate = ptrFloat64(appendRate)
	}

	appendP99Sec, ok, _ := c.queryScalar(ctx, fmt.Sprintf(
		`histogram_quantile(0.99, sum by (le) (rate(%s{%s,engine=%q}%s)))`,
		timeline.MetricAppendDuration+"_bucket", nsSel, engine, rate,
	))
	if ok {
		ms := appendP99Sec * 1000
		out.AppendP99Ms = ptrFloat64(ms)
	}

	rangeP99Sec, ok, _ := c.queryScalar(ctx, fmt.Sprintf(
		`histogram_quantile(0.99, sum by (le) (rate(%s{%s,engine=%q}%s)))`,
		timeline.MetricRangeDuration+"_bucket", nsSel, engine, rate,
	))
	if ok {
		ms := rangeP99Sec * 1000
		out.RangeP99Ms = ptrFloat64(ms)
	}

	errRate, ok, _ := c.queryScalar(ctx, fmt.Sprintf(
		`sum(rate(%s{%s,engine=%q}%s))`,
		timeline.MetricOpErrorsTotal, nsSel, engine, rate,
	))
	if ok {
		out.ErrorRate = ptrFloat64(errRate)
	}

	if out.AppendRate == nil && out.AppendP99Ms == nil && out.RangeP99Ms == nil && out.ErrorRate == nil {
		return nil
	}
	return out
}

func (c *MetricsCollector) detectTimelineEngine(ctx context.Context, nsSel string) string {
	rate := promRateRange()
	for _, engine := range []string{"postgresql", "redis"} {
		v, ok, _ := c.queryScalar(ctx, fmt.Sprintf(
			`sum(rate(%s{%s,engine=%q}%s))`,
			timeline.MetricAppendTotal, nsSel, engine, rate,
		))
		if ok && v > 0 {
			return engine
		}
	}
	for _, engine := range []string{"postgresql", "redis"} {
		v, ok, _ := c.queryScalar(ctx, fmt.Sprintf(`sum(%s{%s,engine=%q})`, timeline.MetricAppendTotal, nsSel, engine))
		if ok && v > 0 {
			return engine
		}
	}
	return ""
}

func (c *MetricsCollector) collectPostgresTimelineTableStats(ctx context.Context, ns string, cm *ComponentMetrics) {
	if cm == nil {
		return
	}
	relBytes, ok, _ := c.queryScalar(ctx, fmt.Sprintf(`max(c9_timeline_relation_bytes{namespace=%q})`, ns))
	if ok {
		cm.TimelineRelationBytes = ptrFloat64(relBytes)
	}
	dead, ok, _ := c.queryScalar(ctx, fmt.Sprintf(`max(c9_timeline_dead_tuples{namespace=%q})`, ns))
	if ok {
		cm.TimelineDeadTuples = ptrFloat64(dead)
	}
}

func ptrFloat64(v float64) *float64 {
	return &v
}
