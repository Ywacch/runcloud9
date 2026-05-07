package metrics

import (
	"context"
	"fmt"
)

// collectTemplateMetrics queries Prometheus for metrics exposed by template images.
// Keys use category.metric_name for frontend grouping. Missing series return 0 via query().
func (c *MetricsCollector) collectTemplateMetrics(ctx context.Context, nsSel, template string) map[string]float64 {
	out := make(map[string]float64)
	if template == "" {
		return out
	}

	switch template {
	case "social-feed", "social-feed-pull":
		put := func(k, promql string) {
			v, _ := c.query(ctx, promql)
			out[k] = v
		}
		put("posts.total_sync", fmt.Sprintf(`sum(social_post_total{%s,mode="sync"})`, nsSel))
		put("posts.total_async", fmt.Sprintf(`sum(social_post_total{%s,mode="async"})`, nsSel))
		put("feed.pull_rate", fmt.Sprintf(`sum(rate(social_feed_request_total{%s,mode="pull"}[30s]))`, nsSel))
		put("feed.push_rate", fmt.Sprintf(`sum(rate(social_feed_request_total{%s,mode=~"push.*"}[30s]))`, nsSel))
		put("feed.pull_p95", fmt.Sprintf(`histogram_quantile(0.95, sum(rate(social_feed_latency_seconds_bucket{%s,mode="pull"}[30s])) by (le))`, nsSel))
		put("feed.push_p95", fmt.Sprintf(`histogram_quantile(0.95, sum(rate(social_feed_latency_seconds_bucket{%s,mode=~"push.*"}[30s])) by (le))`, nsSel))
		put("worker.processed", fmt.Sprintf(`sum(social_worker_processed_total{%s})`, nsSel))
		put("worker.errors", fmt.Sprintf(`sum(social_worker_errors_total{%s})`, nsSel))
		put("worker.writes.redis", fmt.Sprintf(`sum(social_worker_writes_total{%s,store="redis"})`, nsSel))
		put("worker.fanout_size_p99", fmt.Sprintf(`histogram_quantile(0.99, sum(rate(social_worker_fanout_size_bucket{%s}[30s])) by (le))`, nsSel))
		put("worker.latency_p99", fmt.Sprintf(`histogram_quantile(0.99, sum(rate(social_worker_processing_duration_seconds_bucket{%s}[30s])) by (le))`, nsSel))
		put("mq.publish_total", fmt.Sprintf(`sum(social_mq_publish_total{%s})`, nsSel))
		put("mq.publish_errors", fmt.Sprintf(`sum(social_mq_publish_errors_total{%s})`, nsSel))

	case "ecommerce":
		put := func(k, promql string) {
			v, _ := c.query(ctx, promql)
			out[k] = v
		}
		put("checkout.sync_total", fmt.Sprintf(`sum(ecommerce_checkout_total{%s,mode="sync"})`, nsSel))
		put("checkout.async_total", fmt.Sprintf(`sum(ecommerce_checkout_total{%s,mode="async"})`, nsSel))
		put("checkout.sync_p95", fmt.Sprintf(`histogram_quantile(0.95, sum(rate(ecommerce_checkout_duration_seconds_bucket{%s,mode="sync"}[30s])) by (le))`, nsSel))
		put("checkout.async_p95", fmt.Sprintf(`histogram_quantile(0.95, sum(rate(ecommerce_checkout_duration_seconds_bucket{%s,mode="async"}[30s])) by (le))`, nsSel))
		put("checkout.inventory_conflicts", fmt.Sprintf(`sum(ecommerce_inventory_conflicts_total{%s})`, nsSel))
		put("mq.publish_total", fmt.Sprintf(`sum(ecommerce_mq_publish_total{%s})`, nsSel))
		put("mq.publish_errors", fmt.Sprintf(`sum(ecommerce_mq_publish_errors_total{%s})`, nsSel))

	case "url-shortener":
		v, _ := c.query(ctx, fmt.Sprintf(`sum(shortener_code_collisions_total{%s})`, nsSel))
		out["shortener.collisions"] = v
	}

	return out
}
