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

func (c *MetricsCollector) collectTimelineOps(ctx context.Context, ns, nsSel, template string) *TimelineOps {
	ctx = withPromCaller(ctx, "timeline")
	tmpl := templates.GetTemplate(template)
	if !templateHasTimelineJob(tmpl) {
		return nil
	}
	engine := c.detectTimelineEngine(ctx, ns, nsSel)
	if engine == "" {
		return nil
	}
	out := &TimelineOps{Engine: engine}

	if appendRate, ok := c.timelineAppendRate(ctx, ns, nsSel, engine); ok {
		out.AppendRate = ptrFloat64(appendRate)
	}

	if appendP99Sec, ok := c.timelineAppendP99Sec(ctx, ns, nsSel, engine); ok {
		ms := appendP99Sec * 1000
		out.AppendP99Ms = ptrFloat64(ms)
	}

	if rangeP99Sec, ok := c.timelineRangeP99Sec(ctx, ns, nsSel, engine); ok {
		ms := rangeP99Sec * 1000
		out.RangeP99Ms = ptrFloat64(ms)
	}

	if errRate, ok := c.timelineOpErrorsRate(ctx, ns, nsSel, engine); ok {
		out.ErrorRate = ptrFloat64(errRate)
	}

	if out.AppendRate == nil && out.AppendP99Ms == nil && out.RangeP99Ms == nil && out.ErrorRate == nil {
		return nil
	}
	return out
}

func (c *MetricsCollector) detectTimelineEngine(ctx context.Context, ns, nsSel string) string {
	for _, engine := range []string{"postgresql", "redis"} {
		v, ok := c.timelineAppendRate(ctx, ns, nsSel, engine)
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
	if relBytes, ok := c.timelineRelationBytes(ctx, ns); ok {
		cm.TimelineRelationBytes = ptrFloat64(relBytes)
	}
	if dead, ok := c.timelineDeadTuples(ctx, ns); ok {
		cm.TimelineDeadTuples = ptrFloat64(dead)
	}
}

func ptrFloat64(v float64) *float64 {
	return &v
}
