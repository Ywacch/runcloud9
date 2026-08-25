package metrics

import "sync"

const (
	seriesHTTP        = "http"
	seriesHTTP5xx     = "http5xx"
	seriesSQL         = "sql"
	seriesCacheHits   = "cache_hits"
	seriesCacheMisses = "cache_misses"
)

type seriesAcc struct {
	sum  float64
	last map[string]float64
}

type nsSession struct {
	series map[string]*seriesAcc
}

// sessionAccumulator tracks deployment-lifetime counter totals per namespace.
// It survives app-server pod restarts by accumulating deltas from sum-by-pod scrapes.
type sessionAccumulator struct {
	mu sync.Mutex
	ns map[string]*nsSession
}

func newSessionAccumulator() *sessionAccumulator {
	return &sessionAccumulator{
		ns: make(map[string]*nsSession),
	}
}

// Cleanup drops all session state for a namespace (call when a deployment ends).
func (a *sessionAccumulator) Cleanup(namespace string) {
	if a == nil || namespace == "" {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.ns, namespace)
}

// Observe ingests the current per-pod counter values for one series and returns
// the running session sum. Empty nowByPod leaves state unchanged and returns the
// existing sum (e.g. when Prometheus is temporarily unavailable).
func (a *sessionAccumulator) Observe(namespace, series string, nowByPod map[string]float64) float64 {
	if a == nil || namespace == "" || series == "" {
		return sumByPod(nowByPod)
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	nsAcc, ok := a.ns[namespace]
	if !ok {
		nsAcc = &nsSession{series: make(map[string]*seriesAcc)}
		a.ns[namespace] = nsAcc
	}
	sa, ok := nsAcc.series[series]
	if !ok {
		sa = &seriesAcc{last: make(map[string]float64)}
		nsAcc.series[series] = sa
	}

	if len(nowByPod) == 0 {
		return sa.sum
	}

	for pod, now := range nowByPod {
		last, seen := sa.last[pod]
		if !seen {
			last = 0
		}
		if now >= last {
			sa.sum += now - last
		} else {
			// Counter reset (pod restart or chaos kill).
			sa.sum += now
		}
		sa.last[pod] = now
	}
	return sa.sum
}

func sumByPod(nowByPod map[string]float64) float64 {
	var sum float64
	for _, v := range nowByPod {
		sum += v
	}
	return sum
}
