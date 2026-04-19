package metrics

import (
	"time"

	"github.com/ywacch/cloud9/pkg/loadgen"
)

// DeploymentMetrics is the payload sent to the frontend every 2s.
// One struct covers all components in the deployment.
type DeploymentMetrics struct {
	Timestamp       time.Time                   `json:"timestamp"`
	Namespace       string                      `json:"namespace"`
	Components      map[string]ComponentMetrics `json:"components"`
	TemplateMetrics map[string]float64          `json:"templateMetrics,omitempty"`
	LoadGen         *loadgen.LoadStats          `json:"loadgen,omitempty"`
}

// ComparisonMetrics is reserved for side-by-side deployment streams (future).
type ComparisonMetrics struct {
	DeploymentA *DeploymentMetrics `json:"deploymentA"`
	DeploymentB *DeploymentMetrics `json:"deploymentB"`
	AlignedAt   time.Time          `json:"alignedAt"`
}

// ComponentMetrics holds per-pod metrics for one component (db, cache, app-server).
// JSON field names use camelCase for frontend consumers.
type ComponentMetrics struct {
	Component   string  `json:"component"`   // "db" | "cache" | "app-server"
	CPUUsage    float64 `json:"cpuUsage"`    // cores (e.g. 0.15 = 150m)
	MemoryBytes int64   `json:"memoryUsage"` // working set bytes
	RequestRate float64 `json:"requestRate"` // requests/sec, 0 if not applicable
	// TotalRequests is sum(increase(http_requests_total[9m])) — rolling ~9m window, not full session.
	TotalRequests float64 `json:"totalRequests"`
	P50Latency    float64 `json:"p50Latency"` // seconds (Prometheus histogram), 0 if N/A
	P95Latency    float64 `json:"p95Latency"` // seconds
	P99Latency    float64 `json:"p99Latency"` // seconds, 0 if not applicable
	CacheHits     float64 `json:"cacheHits"`   // hits/sec, 0 if not applicable
	CacheMisses   float64 `json:"cacheMisses"` // misses/sec, 0 if not applicable
	// CacheHitsLast9m / CacheMissesLast9m are Prometheus increase(...[9m]) sums (estimated counts in window).
	CacheHitsLast9m   float64 `json:"cacheHitsLast9m"`
	CacheMissesLast9m float64 `json:"cacheMissesLast9m"`
	ErrorRate         float64 `json:"errorRate"` // errors/sec
	// Db* metrics come from url-shortener's db_query_duration_seconds histogram (app /metrics), same namespace label.
	DbQueryRate    float64 `json:"dbQueryRate"`    // queries/sec
	DbQueryLatency float64 `json:"dbQueryLatency"` // seconds (avg)
	DbTotalQueries float64 `json:"dbTotalQueries"` // increase(count[9m])
	// Postgres exporter (when scraped); complements DbQueryRate from app metrics.
	DbActiveConnections float64 `json:"dbActiveConnections,omitempty"`
	MaxConnections      float64 `json:"maxConnections,omitempty"`
	// Redis exporter keyspace rates (supplement app-reported cache hit/miss).
	RedisKeyspaceHits   float64 `json:"redisKeyspaceHits,omitempty"`
	RedisKeyspaceMisses float64 `json:"redisKeyspaceMisses,omitempty"`
	// kube_pod_container_resource_limits / redis maxmemory (capacity denominator for UI %).
	CPULimit    float64 `json:"cpuLimit,omitempty"`
	MemoryLimit int64   `json:"memoryLimit,omitempty"`
	// True when usage came from kube resource requests (no real-time series).
	CPUIsEstimated bool `json:"cpuIsEstimated,omitempty"`
	MemIsEstimated bool `json:"memIsEstimated,omitempty"`
	// LB (nginx stub_status) + mirrored app RPS; MQ management API; worker mirrors MQ consume rate.
	ActiveConnections float64 `json:"activeConnections"`
	QueueDepth        float64 `json:"queueDepth"`
	ProduceRate       float64 `json:"produceRate"`
	ConsumeRate       float64 `json:"consumeRate"`
	MessagesReady     float64 `json:"messagesReady"`
	MessagesUnacked   float64 `json:"messagesUnacked"`
	// LB config from deploy-time ConfigMap annotations (not Prometheus).
	LBAlgorithm string   `json:"lbAlgorithm,omitempty"`
	LBBackends  []string `json:"lbBackends,omitempty"`
	// PerBackendRPS is set only for the lb component: RPS per app-server pod (from Prometheus pod label).
	PerBackendRPS map[string]float64 `json:"perBackendRps,omitempty"`
}

func emptyPeakSummary() map[string]any {
	return map[string]any{
		"peak_rps":           0.0,
		"peak_p99_ms":        0.0,
		"error_rate":         0.0,
		"total_requests":     0.0,
		"total_cache_hits":   0.0,
		"total_cache_misses": 0.0,
		"cache_hit_rate":     0.0,
		"db_query_rate":      0.0,
		"db_total_queries":   0.0,
	}
}

// lifetimeHitRate returns hit percentage from 9m increase counters (0–100).
func lifetimeHitRate(c ComponentMetrics) float64 {
	total := c.CacheHitsLast9m + c.CacheMissesLast9m
	if total == 0 {
		return 0
	}
	return c.CacheHitsLast9m / total * 100
}

// PeakSummary computes summary metrics from a single snapshot taken at teardown time.
// UNDERSTAND: this is "latest-at-teardown", not a true session-long peak across the whole run.
func PeakSummary(dm *DeploymentMetrics) map[string]any {
	if dm == nil {
		return emptyPeakSummary()
	}
	app, ok := dm.Components["app-server"]
	if !ok {
		return emptyPeakSummary()
	}
	cache := dm.Components["cache"]
	db := dm.Components["db"]
	return map[string]any{
		"peak_rps":           app.RequestRate,
		"peak_p99_ms":        app.P99Latency * 1000,
		"error_rate":         app.ErrorRate,
		"total_requests":     app.TotalRequests,
		"total_cache_hits":   cache.CacheHitsLast9m,
		"total_cache_misses": cache.CacheMissesLast9m,
		"cache_hit_rate":     lifetimeHitRate(cache),
		"db_query_rate":      db.DbQueryRate,
		"db_total_queries":   db.DbTotalQueries,
	}
}
