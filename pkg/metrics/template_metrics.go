package metrics

import (
	"context"
	"strings"

	"github.com/ywacch/cloud9/pkg/templates"
)

// collectTemplateMetrics queries Prometheus using the deployment template's YAML metrics profile.
// Keys use category.metric_name for frontend grouping. Missing series return 0 via query().
func (c *MetricsCollector) collectTemplateMetrics(ctx context.Context, nsSel, template string) map[string]float64 {
	out := make(map[string]float64)
	tmpl := templates.GetTemplate(template)
	if tmpl == nil || tmpl.PrometheusMetrics == nil {
		return out
	}
	for _, q := range tmpl.PrometheusMetrics.Queries {
		expr := strings.ReplaceAll(q.Expr, templates.NSSelectorPlaceholder, nsSel)
		v, _ := c.query(ctx, expr)
		out[q.Key] = v
	}
	return out
}

// augmentTemplateMetrics merges component-level signals into templateMetrics per YAML augments.
func (c *MetricsCollector) augmentTemplateMetrics(dm *DeploymentMetrics, template string) {
	if dm == nil {
		return
	}
	tmpl := templates.GetTemplate(template)
	if tmpl == nil || tmpl.PrometheusMetrics == nil || len(tmpl.PrometheusMetrics.Augments) == 0 {
		return
	}
	if dm.TemplateMetrics == nil {
		dm.TemplateMetrics = make(map[string]float64)
	}
	for _, a := range tmpl.PrometheusMetrics.Augments {
		applyPrometheusMetricAugment(dm, a)
	}
}
