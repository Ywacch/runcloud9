// Attack scaling and metrics helpers (keep aligned with pkg/loadgen/attack.go).
package main

import (
	"math"
	"os"
	"strconv"
	"time"

	vegeta "github.com/tsenart/vegeta/v12/lib"
)

const (
	intervalWindow      = 5 * time.Second
	recentIntervalsCap  = 12
	defaultMinWorkers     = 50
	defaultMaxWorkersCap  = 5000
	defaultLatencyBudgetS = 2.0
)

type loadIntervalStats struct {
	At        time.Time `json:"t"`
	P99Ms     float64   `json:"p99Ms"`
	ActualRps float64   `json:"actualRps"`
}

func maxWorkersForRPS(rps int) uint64 {
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

func initialWorkersForRPS(rps int) uint64 {
	max := maxWorkersForRPS(rps)
	start := max / 4
	if start < 10 {
		start = 10
	}
	if start > max {
		start = max
	}
	return start
}

func newVegetaAttacker(rps int) *vegeta.Attacker {
	return vegeta.NewAttacker(
		vegeta.Timeout(5*time.Second),
		vegeta.Workers(initialWorkersForRPS(rps)),
		vegeta.MaxWorkers(maxWorkersForRPS(rps)),
	)
}

func applyMetricsUpdate(stats *loadStats, vm *vegeta.Metrics, targetRPS int, startedAt *time.Time) {
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

func closeInterval(stats *loadStats, window *vegeta.Metrics, now time.Time) vegeta.Metrics {
	if stats == nil || window == nil {
		return vegeta.Metrics{}
	}
	window.Close()
	stats.IntervalP99Ms = window.Latencies.P99.Seconds() * 1000
	elapsed := intervalWindow.Seconds()
	if elapsed > 0 {
		stats.IntervalActualRPS = float64(window.Requests) / elapsed
	}
	snap := loadIntervalStats{
		At:        now,
		P99Ms:     stats.IntervalP99Ms,
		ActualRps: stats.IntervalActualRPS,
	}
	stats.RecentIntervals = appendRecentInterval(stats.RecentIntervals, snap)
	return vegeta.Metrics{}
}

func appendRecentInterval(existing []loadIntervalStats, snap loadIntervalStats) []loadIntervalStats {
	out := append(existing, snap)
	if len(out) > recentIntervalsCap {
		out = out[len(out)-recentIntervalsCap:]
	}
	return out
}
