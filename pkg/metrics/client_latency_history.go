package metrics

import (
	"math"
	"sort"
	"time"

	"github.com/ywacch/cloud9/pkg/loadgen"
)

// clientLatencyAccumulator merges closed loadgen 5s windows for session-end persistence.
type clientLatencyAccumulator struct {
	byTime map[int64]loadgen.LoadIntervalStats
}

func newClientLatencyAccumulator() *clientLatencyAccumulator {
	return &clientLatencyAccumulator{
		byTime: make(map[int64]loadgen.LoadIntervalStats),
	}
}

func validClientLatencyInterval(iv loadgen.LoadIntervalStats) bool {
	if iv.At.IsZero() {
		return false
	}
	if iv.P99Ms < 0 || math.IsNaN(iv.P99Ms) || math.IsInf(iv.P99Ms, 0) {
		return false
	}
	if iv.ActualRps < 0 || math.IsNaN(iv.ActualRps) || math.IsInf(iv.ActualRps, 0) {
		return false
	}
	return true
}

func (a *clientLatencyAccumulator) merge(intervals []loadgen.LoadIntervalStats) {
	if a == nil || len(intervals) == 0 {
		return
	}
	if a.byTime == nil {
		a.byTime = make(map[int64]loadgen.LoadIntervalStats)
	}
	for _, iv := range intervals {
		if !validClientLatencyInterval(iv) {
			continue
		}
		a.byTime[iv.At.UnixNano()] = iv
	}
}

// snapshot returns oldest→newest rows for metrics_summary JSON persistence.
func (a *clientLatencyAccumulator) snapshot() []map[string]any {
	if a == nil || len(a.byTime) == 0 {
		return nil
	}
	keys := make([]int64, 0, len(a.byTime))
	for k := range a.byTime {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	out := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		iv := a.byTime[k]
		out = append(out, map[string]any{
			"t":         iv.At.UTC().Format(time.RFC3339Nano),
			"p99Ms":     iv.P99Ms,
			"actualRps": iv.ActualRps,
		})
	}
	return out
}

func (c *MetricsCollector) mergeClientLatencyHistory(namespace string, intervals []loadgen.LoadIntervalStats) {
	if c == nil || namespace == "" || len(intervals) == 0 {
		return
	}
	c.clientLatencyMu.Lock()
	defer c.clientLatencyMu.Unlock()
	if c.clientLatency == nil {
		c.clientLatency = make(map[string]*clientLatencyAccumulator)
	}
	acc := c.clientLatency[namespace]
	if acc == nil {
		acc = newClientLatencyAccumulator()
		c.clientLatency[namespace] = acc
	}
	acc.merge(intervals)
}

// ClientLatencyHistoryFor returns accumulated client-path 5s p99 buckets (oldest→newest)
// for metrics_summary persistence at deployment teardown.
func (c *MetricsCollector) ClientLatencyHistoryFor(namespace string) []map[string]any {
	if c == nil || namespace == "" {
		return nil
	}
	c.clientLatencyMu.Lock()
	defer c.clientLatencyMu.Unlock()
	acc := c.clientLatency[namespace]
	if acc == nil {
		return nil
	}
	return acc.snapshot()
}
