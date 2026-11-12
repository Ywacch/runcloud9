package metrics

import "github.com/ywacch/cloud9/pkg/metrics/hub"

// SetMetricsHub attaches the cluster-wide metrics hub (METRICS_HUB).
func (c *MetricsCollector) SetMetricsHub(h *hub.Hub) {
	if c == nil {
		return
	}
	c.metricsHub = h
}

// MetricsHub returns the hub when METRICS_HUB is enabled.
func (c *MetricsCollector) MetricsHub() *hub.Hub {
	if c == nil {
		return nil
	}
	return c.metricsHub
}
