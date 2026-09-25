package metrics

import (
	"fmt"
	"sync"
	"time"
)

const (
	seriesHTTP            = "http"
	seriesHTTP5xx         = "http5xx"
	seriesWorkerProcessed = "worker_processed"
	seriesWorkerErrors    = "worker_errors"
	seriesSQL             = "sql"
	seriesCacheHits       = "cache_hits"
	seriesCacheMisses     = "cache_misses"
	seriesRedisRead       = "redis_read"
	seriesRedisWrite      = "redis_write"
	seriesRedisEvicted    = "redis_evicted"
	seriesRedisExpired    = "redis_expired"
	seriesPgRead          = "pg_read"
	seriesPgWrite         = "pg_write"
	seriesPgXact          = "pg_xact"
)

// sessionP99MinWindow is the minimum PromQL range for session p99 histogram rates.
const sessionP99MinWindow = 1 * time.Minute

// sessionP99MaxWindow matches the internal-tier SessionCapMins ceiling in pkg/store/user_tier.go.
const sessionP99MaxWindow = 30 * time.Minute

type seriesAcc struct {
	sum  float64
	last map[string]float64
}

type nsSession struct {
	series       map[string]*seriesAcc
	firstObserve time.Time // set on first Observe/ObserveDeltas for this namespace
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

// Elapsed returns wall time since the first Observe/ObserveDeltas for namespace.
// Returns 0 when no scrape has been recorded yet (caller should clamp to sessionP99MinWindow).
func (a *sessionAccumulator) Elapsed(namespace string, now time.Time) time.Duration {
	if a == nil || namespace == "" {
		return 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	nsAcc, ok := a.ns[namespace]
	if !ok || nsAcc.firstObserve.IsZero() {
		return 0
	}
	return now.Sub(nsAcc.firstObserve)
}

// sessionP99PromRange formats elapsed session time as a PromQL range vector, clamped to [1m, 30m].
func sessionP99PromRange(elapsed time.Duration) string {
	if elapsed < sessionP99MinWindow {
		elapsed = sessionP99MinWindow
	}
	if elapsed > sessionP99MaxWindow {
		elapsed = sessionP99MaxWindow
	}
	mins := int(elapsed / time.Minute)
	if mins < 1 {
		mins = 1
	}
	return fmt.Sprintf("[%dm]", mins)
}

// Observe ingests the current per-pod counter values for one series and returns
// the running session sum. The first scrape for a pod adds the full counter
// (pod lifetime). Empty nowByPod leaves state unchanged and returns the
// existing sum (e.g. when Prometheus is temporarily unavailable).
func (a *sessionAccumulator) Observe(namespace, series string, nowByPod map[string]float64) float64 {
	return a.observe(namespace, series, nowByPod, false)
}

// ObserveDeltas is like Observe but the first scrape for a pod only records a
// baseline (sum is unchanged). Use this for engine counters that already include
// seed dumps or process-lifetime exporter traffic. Later scrapes add deltas;
// a counter reset still adds now.
func (a *sessionAccumulator) ObserveDeltas(namespace, series string, nowByPod map[string]float64) float64 {
	return a.observe(namespace, series, nowByPod, true)
}

// rebaseline zeros the session sum for series and replaces last with nowByPod.
// Call after a seed dump so engine session totals exclude inserts that already
// landed. An empty nowByPod clears last so the next ObserveDeltas scrape becomes
// the baseline.
func (a *sessionAccumulator) rebaseline(namespace, series string, nowByPod map[string]float64) {
	if a == nil || namespace == "" || series == "" {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	nsAcc, ok := a.ns[namespace]
	if !ok {
		nsAcc = &nsSession{series: make(map[string]*seriesAcc)}
		a.ns[namespace] = nsAcc
	}
	last := make(map[string]float64, len(nowByPod))
	for pod, now := range nowByPod {
		last[pod] = now
	}
	nsAcc.series[series] = &seriesAcc{sum: 0, last: last}
}

func (a *sessionAccumulator) observe(namespace, series string, nowByPod map[string]float64, baselineFirst bool) float64 {
	if a == nil || namespace == "" || series == "" {
		if baselineFirst {
			return 0
		}
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

	if nsAcc.firstObserve.IsZero() {
		nsAcc.firstObserve = time.Now()
	}

	for pod, now := range nowByPod {
		last, seen := sa.last[pod]
		if !seen {
			if baselineFirst {
				sa.last[pod] = now
				continue
			}
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
