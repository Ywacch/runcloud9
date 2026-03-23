package metrics

import "time"

// DeploymentMetrics is the payload sent to the frontend every 2s.
// One struct covers all components in the deployment.
type DeploymentMetrics struct {
	Timestamp  time.Time                   `json:"timestamp"`
	Namespace  string                      `json:"namespace"`
	Components map[string]ComponentMetrics `json:"components"`
}

// ComponentMetrics holds per-pod metrics for one component (db, cache, app-server).
type ComponentMetrics struct {
	Component   string  `json:"component"` // "db" | "cache" | "app-server"
	CPUUsage    float64 `json:"cpu_usage"` // cores (e.g. 0.15 = 150m)
	MemoryBytes int64   `json:"memory_bytes"`
	RequestRate float64 `json:"request_rate"` // requests/sec, 0 if not applicable
	P99Latency  float64 `json:"p99_latency"`  // seconds, 0 if not applicable
	CacheHits   float64 `json:"cache_hits"`   // hits/sec, 0 if not applicable
	CacheMisses float64 `json:"cache_misses"` // misses/sec, 0 if not applicable
	ErrorRate   float64 `json:"error_rate"`   // errors/sec
}
