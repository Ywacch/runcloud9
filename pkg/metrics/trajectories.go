package metrics

import "time"

// trajectorySeries describes one headline metric extracted from session history.
// Add new engines/metrics by appending to headlineTrajectories — keep extractors
// engine-agnostic (normalized ComponentMetrics fields).
type trajectorySeries struct {
	Key     string
	Extract func(dm *DeploymentMetrics) (float64, bool)
}

var headlineTrajectories = []trajectorySeries{
	{
		Key: "cache_hit_rate",
		Extract: func(dm *DeploymentMetrics) (float64, bool) {
			c, ok := dm.Components["cache"]
			if !ok {
				return 0, false
			}
			return lifetimeHitRate(c), true
		},
	},
	{
		Key: "db_cpu_percent",
		Extract: func(dm *DeploymentMetrics) (float64, bool) {
			c, ok := dm.Components["db"]
			if !ok || c.CPULimit <= 0 {
				return 0, false
			}
			return (c.CPUUsage / c.CPULimit) * 100, true
		},
	},
	{
		Key: "app_p99_ms",
		Extract: func(dm *DeploymentMetrics) (float64, bool) {
			c, ok := dm.Components["app-server"]
			if !ok {
				return 0, false
			}
			return c.P99Latency * 1000, true
		},
	},
	{
		Key: "app_error_rate",
		Extract: func(dm *DeploymentMetrics) (float64, bool) {
			c, ok := dm.Components["app-server"]
			if !ok {
				return 0, false
			}
			return c.ErrorRate, true
		},
	},
}

const (
	trajectoryFlatEps    = 1e-6
	trajectoryInflectEps = 1e-4
)

// BuildTrajectories compacts session history into per-metric slope/direction/inflection
// summaries for truncated-session inference. Missing components yield no entry for that key.
func BuildTrajectories(hist []DeploymentMetrics) map[string]any {
	out := make(map[string]any, len(headlineTrajectories))
	if len(hist) == 0 {
		return out
	}
	for _, series := range headlineTrajectories {
		points := make([]float64, 0, len(hist))
		times := make([]time.Time, 0, len(hist))
		for i := range hist {
			v, ok := series.Extract(&hist[i])
			if !ok {
				continue
			}
			points = append(points, v)
			times = append(times, hist[i].Timestamp)
		}
		if len(points) < 2 {
			continue
		}
		out[series.Key] = compactTrajectory(points, times)
	}
	return out
}

func compactTrajectory(points []float64, times []time.Time) map[string]any {
	n := len(points)
	first, last := points[0], points[n-1]
	elapsed := times[n-1].Sub(times[0]).Seconds()
	slope := 0.0
	if elapsed > 0 {
		slope = (last - first) / elapsed
	}

	direction := "flat"
	switch {
	case slope > trajectoryFlatEps:
		direction = "up"
	case slope < -trajectoryFlatEps:
		direction = "down"
	}

	row := map[string]any{
		"slope":     slope,
		"direction": direction,
		"samples":   n,
		"first":     first,
		"last":      last,
	}

	if idx, ok := firstInflection(points); ok {
		row["inflection"] = map[string]any{
			"index":     idx,
			"timestamp": times[idx].UTC().Format(time.RFC3339Nano),
			"value":     points[idx],
		}
	}
	return row
}

// firstInflection returns the first index where consecutive deltas change sign
// (local min/max), skipping leading flat segments.
func firstInflection(points []float64) (int, bool) {
	if len(points) < 3 {
		return 0, false
	}
	prevSign := 0
	for i := 1; i < len(points); i++ {
		d := points[i] - points[i-1]
		sign := 0
		switch {
		case d > trajectoryInflectEps:
			sign = 1
		case d < -trajectoryInflectEps:
			sign = -1
		}
		if sign == 0 {
			continue
		}
		if prevSign != 0 && sign != prevSign {
			return i - 1, true
		}
		prevSign = sign
	}
	return 0, false
}
