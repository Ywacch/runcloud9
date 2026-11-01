package metrics

import (
	"strings"

	"github.com/ywacch/cloud9/pkg/loadgen"
)

func statusCodeCountsAsError(key string) bool {
	if key == "" || key == "0" {
		return true
	}
	return !strings.HasPrefix(key, "2")
}

// non2xxCount sums loadgen StatusCodes entries that are not HTTP 2xx.
func non2xxCount(codes map[string]int) int {
	if len(codes) == 0 {
		return 0
	}
	var n int
	for k, v := range codes {
		if v <= 0 {
			continue
		}
		if statusCodeCountsAsError(k) {
			n += v
		}
	}
	return n
}

// nextScaleLagErrors advances the scale-up lag error counter from cumulative status codes.
// When lagging is false, the counter resets to 0. When cumulative totals drop (run restart),
// the delta for that tick is the new cumulative non-2xx total, not a negative diff.
func nextScaleLagErrors(prev uint64, lagging bool, prevCodes, curCodes map[string]int) uint64 {
	if !lagging {
		return 0
	}
	curTotal := non2xxCount(curCodes)
	prevTotal := non2xxCount(prevCodes)
	var delta int
	if curTotal < prevTotal {
		delta = curTotal
	} else {
		delta = curTotal - prevTotal
	}
	if delta < 0 {
		delta = 0
	}
	return prev + uint64(delta)
}

type scaleLagState struct {
	errors uint64
	codes  map[string]int
}

func appServerScaleLagging(cm ComponentMetrics) bool {
	if cm.Scale == nil {
		return false
	}
	return cm.Scale.Desired > cm.Scale.Ready
}

func cloneIntMap(m map[string]int) map[string]int {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func (c *MetricsCollector) applyScaleLagToLoadStats(namespace string, lagging bool, stats loadgen.LoadStats) loadgen.LoadStats {
	if c == nil {
		return stats
	}
	c.scaleLagMu.Lock()
	defer c.scaleLagMu.Unlock()
	if c.scaleLag == nil {
		c.scaleLag = make(map[string]scaleLagState)
	}
	st := c.scaleLag[namespace]
	next := nextScaleLagErrors(st.errors, lagging, st.codes, stats.StatusCodes)
	st.errors = next
	st.codes = cloneIntMap(stats.StatusCodes)
	c.scaleLag[namespace] = st
	if next > 0 {
		stats.ScaleLagErrors = next
	}
	return stats
}
