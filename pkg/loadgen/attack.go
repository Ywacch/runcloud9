package loadgen

import (
	"math"
	"os"
	"strconv"
	"time"

	vegeta "github.com/tsenart/vegeta/v12/lib"
)

const (
	// IntervalWindow is the Vegeta metrics window for intervalP99Ms / intervalActualRps.
	IntervalWindow = 5 * time.Second
	// RecentIntervalsCap is how many closed 5s buckets LoadStats retains.
	RecentIntervalsCap = 12
)

const (
	defaultMinWorkers     = 50
	defaultMaxWorkersCap  = 5000
	defaultLatencyBudgetS = 2.0
)

// LoadIntervalStats is one closed loadgen metrics window (client-observed).
type LoadIntervalStats struct {
	At        time.Time `json:"t"`
	P99Ms     float64   `json:"p99Ms"`
	ActualRps float64   `json:"actualRps"`
}

// MaxWorkersForRPS returns Vegeta MaxWorkers from target RPS and a latency headroom budget.
// Little's law: need roughly RPS * latencySeconds concurrent in-flight slots.
func MaxWorkersForRPS(rps int) uint64 {
	if rps < 1 {
		rps = 1
	}
	if override := os.Getenv("LOADGEN_MAX_WORKERS"); override != "" {
		if n, err := strconv.Atoi(override); err == nil && n > 0 {
			return uint64(n)
		}
	}
	need := int(math.Ceil(float64(rps) * defaultLatencyBudgetS))
	if need < defaultMinWorkers {
		need = defaultMinWorkers
	}
	if need > defaultMaxWorkersCap {
		need = defaultMaxWorkersCap
	}
	return uint64(need)
}

// InitialWorkersForRPS returns the starting worker pool size (ramps up to MaxWorkers).
func InitialWorkersForRPS(rps int) uint64 {
	max := MaxWorkersForRPS(rps)
	start := max / 4
	if start < 10 {
		start = 10
	}
	if start > max {
		start = max
	}
	return start
}

// NewVegetaAttacker builds an open-loop attacker scaled to target RPS.
func NewVegetaAttacker(rps int) *vegeta.Attacker {
	return vegeta.NewAttacker(
		vegeta.Timeout(5*time.Second),
		vegeta.Workers(InitialWorkersForRPS(rps)),
		vegeta.MaxWorkers(MaxWorkersForRPS(rps)),
	)
}

// ApplyMetricsUpdate copies cumulative Vegeta metrics into LoadStats (1s live refresh).
func ApplyMetricsUpdate(stats *LoadStats, vm *vegeta.Metrics, targetRPS int, startedAt *time.Time) {
	if stats == nil || vm == nil {
		return
	}
	target := float64(targetRPS)
	stats.CurrentRPS = target
	stats.TargetRPS = target
	stats.TotalRequests = vm.Requests
	stats.SuccessRate = vm.Success * 100
	stats.MeanLatency = vm.Latencies.Mean.Seconds()
	stats.P99Latency = vm.Latencies.P99.Seconds()
	stats.ActualRPS = vm.Rate
	if startedAt != nil {
		stats.DurationSeconds = time.Since(*startedAt).Seconds()
	}
	stats.StatusCodes = copyIntMap(vm.StatusCodes)
}

// CloseInterval finalizes a 5s window, appends to RecentIntervals, and returns a fresh window metrics.
func CloseInterval(stats *LoadStats, window *vegeta.Metrics, now time.Time) vegeta.Metrics {
	if stats == nil || window == nil {
		return vegeta.Metrics{}
	}
	window.Close()
	stats.IntervalP99Ms = window.Latencies.P99.Seconds() * 1000
	elapsed := IntervalWindow.Seconds()
	if elapsed > 0 {
		stats.IntervalActualRPS = float64(window.Requests) / elapsed
	}
	snap := LoadIntervalStats{
		At:        now,
		P99Ms:     stats.IntervalP99Ms,
		ActualRps: stats.IntervalActualRPS,
	}
	stats.RecentIntervals = appendRecentInterval(stats.RecentIntervals, snap)
	return vegeta.Metrics{}
}

func appendRecentInterval(existing []LoadIntervalStats, snap LoadIntervalStats) []LoadIntervalStats {
	out := append(existing, snap)
	if len(out) > RecentIntervalsCap {
		out = out[len(out)-RecentIntervalsCap:]
	}
	return out
}
