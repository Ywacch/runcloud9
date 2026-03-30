package metrics

import (
	"time"

	"github.com/ywacch/cloud9/pkg/loadgen"
)

// DeploymentMetrics is the payload sent to the frontend every 2s.
// One struct covers all components in the deployment.
type DeploymentMetrics struct {
	Timestamp  time.Time                   `json:"timestamp"`
	Namespace  string                      `json:"namespace"`
	Components map[string]ComponentMetrics `json:"components"`
	LoadGen    *loadgen.LoadStats          `json:"loadgen,omitempty"`
}

// ComponentMetrics holds per-pod metrics for one component (db, cache, app-server).
// JSON field names use camelCase for frontend consumers.
type ComponentMetrics struct {
	Component   string  `json:"component"` // "db" | "cache" | "app-server"
	CPUUsage    float64 `json:"cpuUsage"`    // cores (e.g. 0.15 = 150m)
	MemoryBytes int64   `json:"memoryUsage"` // working set bytes
	RequestRate float64 `json:"requestRate"` // requests/sec, 0 if not applicable
	P99Latency  float64 `json:"p99Latency"`  // seconds, 0 if not applicable
	CacheHits   float64 `json:"cacheHits"`   // hits/sec, 0 if not applicable
	CacheMisses float64 `json:"cacheMisses"` // misses/sec, 0 if not applicable
	// CacheHitsLast9m / CacheMissesLast9m are Prometheus increase(...[9m]) sums (estimated counts in window).
	CacheHitsLast9m   float64 `json:"cacheHitsLast9m"`
	CacheMissesLast9m float64 `json:"cacheMissesLast9m"`
	ErrorRate          float64 `json:"errorRate"` // errors/sec
}

// PeakSummary computes summary metrics from a single snapshot taken at teardown time.
// UNDERSTAND: this is "latest-at-teardown", not a true session-long peak across the whole run.
func PeakSummary(dm *DeploymentMetrics) map[string]any {
	if dm == nil {
		return map[string]any{
			"peak_rps":    0.0,
			"peak_p99_ms": 0.0,
			"error_rate":  0.0,
		}
	}
	app, ok := dm.Components["app-server"]
	if !ok {
		return map[string]any{
			"peak_rps":    0.0,
			"peak_p99_ms": 0.0,
			"error_rate":  0.0,
		}
	}
	return map[string]any{
		"peak_rps":    app.RequestRate,
		"peak_p99_ms": app.P99Latency * 1000,
		"error_rate":  app.ErrorRate,
	}
}
