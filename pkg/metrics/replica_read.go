package metrics

import (
	"context"
	"log"

	"github.com/ywacch/cloud9/pkg/metrics/signals"
)

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
	var sum float64
	for _, v := range m {
		sum += v
	}
	return sum, true
}
