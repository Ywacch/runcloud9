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
	Events          []Event                     `json:"events,omitempty"`
	LoadGen         *loadgen.LoadStats          `json:"loadgen,omitempty"`
}

// Event annotates the metrics timeline (loadgen, thresholds, future cascades).
type Event struct {
	Timestamp time.Time `json:"timestamp"`
	Type      string    `json:"type"` // "action" | "threshold" | "cascade"
	Source    string    `json:"source"`
	Component string    `json:"component,omitempty"`
	// RuleID is the stable detector key (e.g. "cache_hit_low", "cascade_cache_db") for UI prediction matching.
	RuleID      string `json:"ruleId,omitempty"`
	Title       string `json:"title"`
	Description string `json:"description"`
	// Storytelling: cause→effect narrative and topology-only suggestions for the UI.
	Narrative  string   `json:"narrative,omitempty"`
	Suggestion string   `json:"suggestion,omitempty"`
	Related    []string `json:"related,omitempty"`
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
	// AvgLatency is used for components that don't expose histogram buckets (e.g. Redis commands).
	AvgLatency  float64 `json:"avgLatency,omitempty"` // seconds
	CacheHits   float64 `json:"cacheHits"`            // hits/sec, 0 if not applicable
	CacheMisses float64 `json:"cacheMisses"`          // misses/sec, 0 if not applicable
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
	RedisOpsPerSec      float64 `json:"redisOpsPerSec,omitempty"`
	RedisRejectedRate   float64 `json:"redisRejectedRate,omitempty"`
	CacheStrategy       string  `json:"cacheStrategy,omitempty"`
	CacheMetricMode     string  `json:"cacheMetricMode,omitempty"`
	CacheReadOperation  string  `json:"cacheReadOperation,omitempty"`
	CacheWriteOperation string  `json:"cacheWriteOperation,omitempty"`
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
	// Replicas is per-pod breakdown on app-server (empty when single replica or pods not listed).
	Replicas []ReplicaMetrics `json:"replicas,omitempty"`
	// BackendReplicas / BackendPods expose LB-facing pod rollout and per-backend metrics.
	BackendReplicas *BackendReplicas `json:"backendReplicas,omitempty"`
	BackendPods     []BackendPod     `json:"backendPods,omitempty"`
	// Capacity / utilization (component-specific; omit when unknown or N/A).
	LBConnectionLimit float64 `json:"lbConnectionLimit,omitempty"` // nginx worker_connections (see deployer k8s LB template)
	RedisMaxClients   float64 `json:"redisMaxClients,omitempty"`   // from redis_exporter, if scraped
	MQMemoryUsed      float64 `json:"mqMemoryUsed,omitempty"`      // RabbitMQ management /api/nodes
	MQMemoryLimit     float64 `json:"mqMemoryLimit,omitempty"`
	MQSaturationRatio float64 `json:"mqSaturationRatio,omitempty"` // produce/consume; >1 means consumer falling behind
	WorkerSaturation  float64 `json:"workerSaturation,omitempty"`  // queueDepth/100 capped at 1.0
}

// BackendReplicas tracks desired vs ready app-server pods (LB-facing view).
type BackendReplicas struct {
	Desired int32 `json:"desired"`
	Ready   int32 `json:"ready"`
}

// PodMetrics holds per-pod runtime metrics shared by app-server replicas and LB backends.
type PodMetrics struct {
	Name        string  `json:"name"`
	Ready       bool    `json:"ready"`
	RequestRate float64 `json:"requestRate"`
	P99Latency  float64 `json:"p99Latency"`
	CPUUsage    float64 `json:"cpuUsage,omitempty"`
	ErrorRate   float64 `json:"errorRate,omitempty"`
}

// ReplicaMetrics is the app-server component view of PodMetrics.
type ReplicaMetrics = PodMetrics

// BackendPod is the lb component view of PodMetrics.
type BackendPod = PodMetrics

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

// PeakSummaryFromHistory aggregates peaks across all snapshots in the ring buffer.
// Uses maxima for rates and latencies; uses minimum cache_hit_rate (worst) across the window.
func PeakSummaryFromHistory(rb *RingBuffer) map[string]any {
	if rb == nil || rb.Len() == 0 {
		return emptyPeakSummary()
	}
	all := rb.All()
	var peakRPS, peakP99ms, peakErr, maxTotalReq, maxHits, maxMisses, minHitRate float64 = 0, 0, 0, 0, 0, 0, -1
	var maxDbRate, maxDbTotal float64
	haveHitRate := false
	for i := range all {
		dm := &all[i]
		app, ok := dm.Components["app-server"]
		if !ok {
			continue
		}
		cache := dm.Components["cache"]
		db := dm.Components["db"]
		p99ms := app.P99Latency * 1000
		if app.RequestRate > peakRPS {
			peakRPS = app.RequestRate
		}
		if p99ms > peakP99ms {
			peakP99ms = p99ms
		}
		if app.ErrorRate > peakErr {
			peakErr = app.ErrorRate
		}
		if app.TotalRequests > maxTotalReq {
			maxTotalReq = app.TotalRequests
		}
		if cache.CacheHitsLast9m > maxHits {
			maxHits = cache.CacheHitsLast9m
		}
		if cache.CacheMissesLast9m > maxMisses {
			maxMisses = cache.CacheMissesLast9m
		}
		hr := lifetimeHitRate(cache)
		if !haveHitRate || hr < minHitRate {
			minHitRate = hr
			haveHitRate = true
		}
		if db.DbQueryRate > maxDbRate {
			maxDbRate = db.DbQueryRate
		}
		if db.DbTotalQueries > maxDbTotal {
			maxDbTotal = db.DbTotalQueries
		}
	}
	if !haveHitRate {
		minHitRate = 0
	}
	return map[string]any{
		"peak_rps":           peakRPS,
		"peak_p99_ms":        peakP99ms,
		"error_rate":         peakErr,
		"total_requests":     maxTotalReq,
		"total_cache_hits":   maxHits,
		"total_cache_misses": maxMisses,
		"cache_hit_rate":     minHitRate,
		"db_query_rate":      maxDbRate,
		"db_total_queries":   maxDbTotal,
	}
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
