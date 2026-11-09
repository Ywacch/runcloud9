package metrics

import (
	"context"
	"log"
	"strings"

	"github.com/ywacch/cloud9/pkg/metrics/signals"
)

// sigByPodWithLegacy reads a PerPod signal and falls back to legacyPromQL when the hub is absent.
func (c *MetricsCollector) sigByPodWithLegacy(ctx context.Context, ns string, id signals.ID, legacyPromQL string) map[string]float64 {
	if c == nil || ns == "" {
		return nil
	}
	legacyPromQL = strings.TrimSpace(legacyPromQL)
	m := c.sigByPod(ctx, ns, id)
	if len(m) > 0 {
		if signalCompareEnabled() && legacyPromQL != "" {
			c.compareSignalPodSum(ctx, ns, id, m, legacyPromQL)
		}
		return m
	}
	if legacyPromQL == "" {
		return nil
	}
	ctx = withSignalDirectProm(ctx)
	legacy := c.queryMultiResult(ctx, legacyPromQL, "pod")
	if signalCompareEnabled() {
		c.compareSignalPodSum(ctx, ns, id, m, legacyPromQL)
	}
	return legacy
}

func (c *MetricsCollector) compareSignalPodSum(ctx context.Context, ns string, id signals.ID, hub map[string]float64, legacyPromQL string) {
	hubSum, hubOK := sumMap(hub)
	legacy := c.queryMultiResult(ctx, legacyPromQL, "pod")
	legSum, legOK := sumMap(legacy)
	match := promValuesMatch(hubSum, legSum, hubOK, legOK)
	result := "match"
	if !match {
		result = "mismatch"
		log.Printf("metrics: signal compare mismatch ns=%s signal=%s hub=%v old=%v ok=(%v,%v)", ns, id, hubSum, legSum, hubOK, legOK)
	}
	if c != nil && c.signalCompareObserver != nil {
		c.signalCompareObserver(string(id), result)
	}
}

func sumMap(m map[string]float64) (float64, bool) {
	if len(m) == 0 {
		return 0, false
	}
	var s float64
	for _, v := range m {
		s += v
	}
	return s, true
}
