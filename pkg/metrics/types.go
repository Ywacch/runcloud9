package metrics

import (
	"time"

	"github.com/ywacch/cloud9/pkg/loadgen"
	"github.com/ywacch/cloud9/pkg/phenomena"
	"github.com/ywacch/cloud9/pkg/placement"
)

// DeploymentMetrics is the payload sent to the frontend every 2s.
// One struct covers all components in the deployment.
type DeploymentMetrics struct {
	// Timestamp is the snapshot observation wall clock (observedAt): when this
	// Collect tick started, independent of per-signal collection lag (see
	// ComponentMetrics.Freshness for per-signal-group tiers).
	Timestamp       time.Time                   `json:"timestamp"`
	Namespace       string                      `json:"namespace"`
	RunRole         string                      `json:"runRole,omitempty"`
	ComparisonID    string                      `json:"comparisonId,omitempty"`
	Components      map[string]ComponentMetrics `json:"components"`
	TemplateMetrics map[string]float64          `json:"templateMetrics,omitempty"`
	Events          []Event                     `json:"events,omitempty"`
	LoadGen         *loadgen.LoadStats          `json:"loadgen,omitempty"`
	Phenomena       []phenomena.Entry           `json:"phenomena,omitempty"`
	// SnapshotAgeMs is time since the cached Collect snapshot at emit time (0 when pending).
	SnapshotAgeMs int64 `json:"snapshotAgeMs"`
	// CollectPending is true before the session publisher has stored its first snapshot.
	CollectPending bool `json:"collectPending,omitempty"`
	// Timeline carries shared timeline job op metrics (append/range latency, errors).
	Timeline *TimelineOps `json:"timeline,omitempty"`
	// Placement is the resolved job-to-node topology for this deployment.
	Placement *placement.Topology `json:"placement,omitempty"`
}

// TimelineOps summarizes instrumented timeline store operations for the inspector.
type TimelineOps struct {
	Engine      string   `json:"engine"`
	AppendRate  *float64 `json:"appendRate,omitempty"`
	AppendP99Ms *float64 `json:"appendP99Ms,omitempty"`
	RangeP99Ms  *float64 `json:"rangeP99Ms,omitempty"`
	ErrorRate   *float64 `json:"errorRate,omitempty"`
}

// Event annotates the metrics timeline (loadgen, thresholds, cascades, invariants, runtime).
// Type remains "action" | "threshold" | "cascade" | "invariant". Workload anchors (loadgen/chaos)
// keep Type "action" and set Kind/Detail/Visibility for L2 causal evidence.
type Event struct {
	Timestamp time.Time `json:"timestamp"`
	Type      string    `json:"type"` // "action" | "threshold" | "cascade" | "invariant"
	Source    string    `json:"source"`
	Component string    `json:"component,omitempty"`
	// Kind identifies a workload anchor when set (e.g. "loadgen_start", "loadgen_stop", "chaos_inject").
	// Empty for threshold/cascade/runtime events that are not injected workload actions.
	Kind string `json:"kind,omitempty"`
	// Detail carries structured params for workload anchors (rps, template, scenario, …).
	Detail map[string]string `json:"detail,omitempty"`
	// Visibility selects who should see this event: "student" | "operator" | "all".
	// Empty means all.
	Visibility string `json:"visibility,omitempty"`
	// RuleID is the stable detector key (e.g. "cache_hit_low", "cascade_cache_db") for UI prediction matching.
	RuleID      string `json:"ruleId,omitempty"`
	Title       string `json:"title"`
	Description string `json:"description"`
	// Storytelling: cause→effect narrative and topology-only suggestions for the UI.
	Narrative  string   `json:"narrative,omitempty"`
	Suggestion string   `json:"suggestion,omitempty"`
	Related    []string `json:"related,omitempty"`
	// CrashContext snapshots component metrics from the same collect tick when a pod restart is detected.
	CrashContext *CrashContext `json:"crashContext,omitempty"`
}

// CrashContext correlates a pod restart with live component metrics at detection time.
type CrashContext struct {
	Pod               string  `json:"pod,omitempty"`
	TerminationReason string  `json:"terminationReason,omitempty"`
	WaitingReason     string  `json:"waitingReason,omitempty"`
	ExitCode          int32   `json:"exitCode,omitempty"`
	CPUPercent        float64 `json:"cpuPercent,omitempty"`
	MemPercent        float64 `json:"memPercent,omitempty"`
	MemoryLimitBytes  int64   `json:"memoryLimitBytes,omitempty"`
	ErrorRate         float64 `json:"errorRate,omitempty"`
	ActiveConnections float64 `json:"activeConnections,omitempty"`
	RedisRejectedRate float64 `json:"redisRejectedRate,omitempty"`
	RedisOpsPerSec    float64 `json:"redisOpsPerSec,omitempty"`
	QueueDepth        float64 `json:"queueDepth,omitempty"`
}

// RuntimeHealth summarizes live pod readiness and restart state for a component.
type RuntimeHealth struct {
	UnreadyCount int                `json:"unreadyCount"`
	Restarting   bool               `json:"restarting"`
	Pods         []PodRuntimeHealth `json:"pods,omitempty"`
}

// PodRuntimeHealth is one pod's K8s status exposed on the metrics stream.
type PodRuntimeHealth struct {
	Name                  string `json:"name"`
	Ready                 bool   `json:"ready"`
	RestartCount          int32  `json:"restartCount"`
	WaitingReason         string `json:"waitingReason,omitempty"`
	LastTerminationReason string `json:"lastTerminationReason,omitempty"`
	ExitCode              int32  `json:"exitCode,omitempty"`
}

// ComparisonMetrics is reserved for side-by-side deployment streams (future).
type ComparisonMetrics struct {
	DeploymentA *DeploymentMetrics `json:"deploymentA"`
	DeploymentB *DeploymentMetrics `json:"deploymentB"`
	AlignedAt   time.Time          `json:"alignedAt"`
}

// DbQuerySource tells the UI whether dbQueryRate is app-instrumented SQL throughput.
const (
	DbQuerySourceApp  = "app"
	DbQuerySourceNone = "none"
)

// StorageIO holds engine-native persistence signals for any storage-capable component.
// Throughput, latency, capacity, and errors share one block so the UI and A/B
// comparators stay engine-agnostic. Phase 2+ populates rates via collectStorageIO.
type StorageIO struct {
	ReadRate  float64 `json:"readRate"`
	WriteRate float64 `json:"writeRate"`
	// ReadTotal9m / WriteTotal9m are deployment-lifetime engine I/O totals (session accumulator).
	ReadTotal9m  float64 `json:"readTotal9m,omitempty"`
	WriteTotal9m float64 `json:"writeTotal9m,omitempty"`
	Unit         string  `json:"unit"` // "rows/s" | "keys/s" | "ops/s"

	// ReadLabel / WriteLabel are display titles for the inspector (e.g. "Rows scanned/s").
	ReadLabel  string `json:"readLabel,omitempty"`
	WriteLabel string `json:"writeLabel,omitempty"`
	// Scope describes which clients or layer the rates include (e.g. all-db-clients).
	Scope string `json:"scope,omitempty"`
	// Footnote is template/topology context for the DB Load panel (optional).
	Footnote string `json:"footnote,omitempty"`

	ReadLatency  float64 `json:"readLatency,omitempty"`  // seconds
	WriteLatency float64 `json:"writeLatency,omitempty"` // seconds

	BytesUsed  int64 `json:"bytesUsed,omitempty"`
	BytesLimit int64 `json:"bytesLimit,omitempty"`

	// ErrorRate is storage-level failures (rollbacks, rejected writes), not app HTTP 5xx.
	ErrorRate float64 `json:"errorRate,omitempty"` // errors/sec

	Source string `json:"source"` // e.g. "postgres_exporter"
}

// ComponentMetrics holds per-pod metrics for one component (db, cache, app-server).
// JSON field names use camelCase for frontend consumers.
type ComponentMetrics struct {
	Component   string  `json:"component"`   // "db" | "cache" | "app-server"
	CPUUsage    float64 `json:"cpuUsage"`    // cores (e.g. 0.15 = 150m)
	MemoryBytes int64   `json:"memoryUsage"` // working set bytes
	RequestRate float64 `json:"requestRate"` // requests/sec, 0 if not applicable
	// TotalRequests is the session sum of http_requests_total on app-server pods,
	// or the session sum of *_worker_processed_total on worker pods.
	// Survives pod restart; resets when cloud9-api restarts or Cleanup runs.
	TotalRequests float64 `json:"totalRequests"`
	P50Latency    float64 `json:"p50Latency"` // seconds (Prometheus histogram), 0 if N/A
	P95Latency    float64 `json:"p95Latency"` // seconds
	P99Latency    float64 `json:"p99Latency"` // seconds, 0 if not applicable
	// AvgLatency is used for components that don't expose histogram buckets (e.g. Redis commands).
	AvgLatency  float64 `json:"avgLatency,omitempty"` // seconds
	CacheHits   float64 `json:"cacheHits"`            // hits/sec. The cache answered and the key was present.
	CacheMisses float64 `json:"cacheMisses"`          // misses/sec. The cache answered and the key was absent.
	CacheErrors float64 `json:"cacheErrors"`          // errors/sec. The app could not ask the cache.
	// CacheHitsLast9m / CacheMissesLast9m / CacheErrorsLast9m are session sums on app-server pods.
	// Survive pod restart; reset when cloud9-api restarts or Cleanup runs. JSON field names kept for the inspector contract.
	CacheHitsLast9m   float64 `json:"cacheHitsLast9m"`
	CacheMissesLast9m float64 `json:"cacheMissesLast9m"`
	CacheErrorsLast9m float64 `json:"cacheErrorsLast9m"`
	ErrorRate         float64 `json:"errorRate"` // errors/sec (rate over 30s)
	// TotalErrors9m is the session sum of http_requests_total{status=~"5.."} on app-server pods,
	// or the session sum of *_worker_errors_total on worker pods. JSON name kept for the inspector contract.
	TotalErrors9m float64 `json:"totalErrors9m,omitempty"`
	// P50Latency9m / P95Latency9m / P99Latency9m are histogram_quantile over
	// rate(http_request_duration_seconds_bucket[elapsed]) in seconds.
	// Elapsed session time is clamped to [1m, 30m]; JSON *9m names kept for API compatibility.
	P50Latency9m float64 `json:"p50Latency9m,omitempty"`
	P95Latency9m float64 `json:"p95Latency9m,omitempty"`
	P99Latency9m float64 `json:"p99Latency9m,omitempty"`
	// Db* metrics come from app db_query_duration_seconds when scraped (app-server /metrics).
	DbQueryRate    float64 `json:"dbQueryRate"`             // queries/sec; meaningful when dbQuerySource=app
	DbQueryLatency float64 `json:"dbQueryLatency"`          // seconds (avg)
	DbQuerySource  string  `json:"dbQuerySource,omitempty"` // "app" | "none"
	// DbTotalQueries is the session sum of db_query_duration_seconds_count on app-server pods.
	// App SQL only (not worker). Resets when cloud9-api restarts or Cleanup runs.
	DbTotalQueries float64 `json:"dbTotalQueries"`
	// Postgres exporter numbackends (open sessions), not active-query count.
	DbActiveConnections float64 `json:"dbActiveConnections,omitempty"`
	MaxConnections      float64 `json:"maxConnections,omitempty"`
	// Redis exporter keyspace rates (supplement app-reported cache hit/miss).
	RedisKeyspaceHits   float64            `json:"redisKeyspaceHits,omitempty"`
	RedisKeyspaceMisses float64            `json:"redisKeyspaceMisses,omitempty"`
	RedisOpsPerSec      float64            `json:"redisOpsPerSec,omitempty"`
	RedisCmdRates       map[string]float64 `json:"redisCmdRates,omitempty"`
	RedisRejectedRate   float64            `json:"redisRejectedRate,omitempty"`
	// EvictionsPerSec / ExpirationsPerSec are engine-agnostic cache store rates (L0).
	// Redis filler uses redis_evicted_keys_total / redis_expired_keys_total today.
	EvictionsPerSec   float64 `json:"evictionsPerSec,omitempty"`
	ExpirationsPerSec float64 `json:"expirationsPerSec,omitempty"`
	// EvictionsLast9m / ExpirationsLast9m are session sums from redis_evicted_keys_total / redis_expired_keys_total.
	EvictionsLast9m     float64 `json:"evictionsLast9m,omitempty"`
	ExpirationsLast9m   float64 `json:"expirationsLast9m,omitempty"`
	CacheStrategy       string  `json:"cacheStrategy,omitempty"`
	CacheMetricMode     string  `json:"cacheMetricMode,omitempty"`
	CacheReadOperation  string  `json:"cacheReadOperation,omitempty"`
	CacheWriteOperation string  `json:"cacheWriteOperation,omitempty"`
	// TimelineKeyCount is the number of timeline:* keys (social-feed gauge social_timeline_keys).
	TimelineKeyCount float64 `json:"timelineKeyCount,omitempty"`
	// TimelineRelationBytes is pg_total_relation_size(timelines) from postgres-exporter custom query.
	TimelineRelationBytes *float64 `json:"timelineRelationBytes,omitempty"`
	// TimelineDeadTuples is n_dead_tup for timelines from pg_stat_user_tables.
	TimelineDeadTuples *float64 `json:"timelineDeadTuples,omitempty"`
	// FeedRequestRatePush / FeedRequestRatePull are social_feed_request_total rates by mode.
	FeedRequestRatePush float64 `json:"feedRequestRatePush,omitempty"`
	FeedRequestRatePull float64 `json:"feedRequestRatePull,omitempty"`
	// kube_pod_container_resource_limits / redis maxmemory (capacity denominator for UI %).
	// When redis_exporter reports maxmemory, MemoryLimit is that value (used vs maxmemory for UI %).
	CPULimit    float64 `json:"cpuLimit,omitempty"`
	MemoryLimit int64   `json:"memoryLimit,omitempty"`
	// True when usage came from kube resource requests (no real-time series).
	CPUIsEstimated bool `json:"cpuIsEstimated,omitempty"`
	MemIsEstimated bool `json:"memIsEstimated,omitempty"`
	// cpuSource/memSource: cadvisor | metrics_api | requests (empty when unknown).
	CPUSource string `json:"cpuSource,omitempty"`
	MemSource string `json:"memSource,omitempty"`
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
	// Replicas is per-pod breakdown on app-server and worker (empty when single replica or pods not listed).
	Replicas []ReplicaMetrics `json:"replicas,omitempty"`
	// Scale reports HPA desired vs ready pod counts when a CPU HPA targets this component.
	Scale *ScaleStatus `json:"scale,omitempty"`
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
	// StorageEngine / StorageRole describe the persistence software and canvas role (primary, cache-aside).
	StorageEngine string     `json:"storageEngine,omitempty"`
	StorageRole   string     `json:"storageRole,omitempty"`
	StorageIO     *StorageIO `json:"storageIo,omitempty"`
	// RuntimeHealth is populated from the Kubernetes API when clientset is configured.
	RuntimeHealth *RuntimeHealth `json:"runtimeHealth,omitempty"`
	// Consistency carries observer probe status and cumulative violation counters (Wave 3 Phase 2).
	Consistency *ConsistencyStatus `json:"consistency,omitempty"`
	// dbWriterPod is the live postgres writer (Service db endpoints); not serialized.
	dbWriterPod string `json:"-"`
	// Layers splits cache proxy/backing-store or Postgres primary/replica instances by role.
	Layers []ComponentLayerMetrics `json:"layers,omitempty"`
	// Freshness tags signal groups (throughput, containerCpu, containerMem, connections,
	// queue) with their collection tier and nominal lag. See freshness.go.
	Freshness map[string]SignalFreshness `json:"freshness,omitempty"`
}

// ConsistencyStatus summarizes primary/replica probe results and violation counts for the DB inspector.
type ConsistencyStatus struct {
	RYWViolations            int64  `json:"rywViolations"`
	MonotonicViolations      int64  `json:"monotonicViolations"`
	CacheDbStale             int64  `json:"cacheDbStale"`
	PartitionStaleReads      int64  `json:"partitionStaleReads"`
	PartitionWriteStall      int64  `json:"partitionWriteStall"`
	ReplicaCannotCatchUp     int64  `json:"replicaCannotCatchUp"`
	ReplicaStalenessVersions int64  `json:"replicaStalenessVersions"`
	LastProbeError           string `json:"lastProbeError,omitempty"`
	LastCacheKey             string `json:"lastCacheKey,omitempty"`
	LastCacheServed          string `json:"lastCacheServed,omitempty"`
	LastCacheTruth           string `json:"lastCacheTruth,omitempty"`
	LastCacheRequestID       string `json:"lastCacheRequestId,omitempty"`
	// LeaderReads / ReplicaReads are cumulative app db_read_total by replica label (read offload split).
	LeaderReads  int64 `json:"leaderReads"`
	ReplicaReads int64 `json:"replicaReads"`
	// ReadRateByReplica is live db_read_total rate per replica label over the Prom rate window.
	ReadRateByReplica map[string]float64 `json:"readRateByReplica,omitempty"`
	// ReplicaStaleReads is cumulative replica_stale_reads_total (eventual policy tradeoff).
	ReplicaStaleReads int64 `json:"replicaStaleReads"`
	// GuaranteeHeldBy is "app" when the in-app read router has redirected for RYW,
	// "db" when remote_apply holds it (Phase 2.6), otherwise "none".
	GuaranteeHeldBy string `json:"guaranteeHeldBy,omitempty"`
}

// ComponentLayerMetrics holds per-process metrics for a layered component (e.g. redis-internal vs cache-proxy).
type ComponentLayerMetrics struct {
	ID       string  `json:"id"`
	Role     string  `json:"role"`
	Engine   string  `json:"engine"`
	CPUUsage float64 `json:"cpuUsage"`
	// MemoryBytes is working-set bytes for this layer.
	MemoryBytes    int64   `json:"memoryUsage"`
	CPULimit       float64 `json:"cpuLimit,omitempty"`
	MemoryLimit    int64   `json:"memoryLimit,omitempty"`
	CPUIsEstimated bool    `json:"cpuIsEstimated,omitempty"`
	MemIsEstimated bool    `json:"memIsEstimated,omitempty"`
	CPUSource      string  `json:"cpuSource,omitempty"`
	MemSource      string  `json:"memSource,omitempty"`
	// Redis exporter signals (backing-store layer only).
	RedisOpsPerSec      float64            `json:"redisOpsPerSec,omitempty"`
	RedisCmdRates       map[string]float64 `json:"redisCmdRates,omitempty"`
	RedisKeyspaceHits   float64            `json:"redisKeyspaceHits,omitempty"`
	RedisKeyspaceMisses float64            `json:"redisKeyspaceMisses,omitempty"`
	RedisRejectedRate   float64            `json:"redisRejectedRate,omitempty"`
	// Engine-agnostic store rates (backing-store layer).
	EvictionsPerSec   float64 `json:"evictionsPerSec,omitempty"`
	ExpirationsPerSec float64 `json:"expirationsPerSec,omitempty"`
	EvictionsLast9m   float64 `json:"evictionsLast9m,omitempty"`
	ExpirationsLast9m float64 `json:"expirationsLast9m,omitempty"`
	RestartCount      int32   `json:"restartCount,omitempty"`
	// ReplicationLagSeconds is primary-side replay lag on replica layers (seconds).
	// Pointer so a successful query of 0 serializes; nil when the series is absent.
	ReplicationLagSeconds *float64 `json:"replicationLagSeconds,omitempty"`
	// SyncState is pg_stat_replication.sync_state from the primary exporter ("async" when absent).
	SyncState string `json:"syncState,omitempty"`
	// LagBytes is WAL bytes behind. When ConnectedOnPrimary is true this is
	// pg_stat_replication replay lag on the writer. When false it is
	// pg_wal_lsn_diff(primary pg_current_wal_lsn(), standby pg_last_wal_replay_lsn()).
	LagBytes *int64 `json:"lagBytes,omitempty"`
	// PromotionCandidate marks the standby PickCandidate(mostCurrent) would promote.
	PromotionCandidate bool `json:"promotionCandidate,omitempty"`
	// ConnectedOnPrimary is true when pg_stat_replication has a row for this standby on the writer.
	ConnectedOnPrimary bool `json:"connectedOnPrimary"` // no omitempty: false must serialize
	// RequestRate is per-layer PostgreSQL transaction throughput (xact/s) when known.
	RequestRate float64 `json:"requestRate,omitempty"`
	// TotalRequests is the session sum of pg_stat_database xact commit+rollback for this layer.
	TotalRequests float64 `json:"totalRequests,omitempty"`
	// DbActiveConnections is open backends (numbackends) on this layer's postgres pod(s).
	DbActiveConnections float64 `json:"dbActiveConnections,omitempty"`
	// StorageIO holds engine-native I/O for this layer. Primary layers copy cluster
	// storageIo (read+write). Replica layers expose read scans only (no write totals).
	StorageIO *StorageIO `json:"storageIo,omitempty"`
	// App SQL from db_query_duration_seconds{target} on app-server pods (after PR8 images).
	DbQueryRate    float64 `json:"dbQueryRate,omitempty"`
	DbTotalQueries float64 `json:"dbTotalQueries,omitempty"`
	P50Latency     float64 `json:"p50Latency,omitempty"`
	P95Latency     float64 `json:"p95Latency,omitempty"`
	P99Latency     float64 `json:"p99Latency,omitempty"`
	P50Latency9m   float64 `json:"p50Latency9m,omitempty"`
	P95Latency9m   float64 `json:"p95Latency9m,omitempty"`
	P99Latency9m   float64 `json:"p99Latency9m,omitempty"`
	// Freshness tags containerCpu / containerMem for this layer (proxy vs backing store
	// can sit on different tiers). See freshness.go.
	Freshness map[string]SignalFreshness `json:"freshness,omitempty"`
}

// ScaleStatus is set only when an HPA targets that component.
// Desired comes from HorizontalPodAutoscaler status, not Deployment spec.
type ScaleStatus struct {
	Desired int32 `json:"desired"`
	Ready   int32 `json:"ready"`
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
	// TotalRequests is sum(http_requests_total) for this pod since process start.
	TotalRequests float64 `json:"totalRequests,omitempty"`
	P50Latency    float64 `json:"p50Latency,omitempty"`
	P95Latency    float64 `json:"p95Latency,omitempty"`
	P99Latency    float64 `json:"p99Latency"`
	// TotalErrors9m is sum(http_requests_total{status=~"5.."}) for this pod since process start.
	TotalErrors9m float64 `json:"totalErrors9m,omitempty"`
	P50Latency9m  float64 `json:"p50Latency9m,omitempty"`
	P95Latency9m  float64 `json:"p95Latency9m,omitempty"`
	P99Latency9m  float64 `json:"p99Latency9m,omitempty"`
	CPUUsage      float64 `json:"cpuUsage,omitempty"`
	MemoryBytes   int64   `json:"memoryBytes,omitempty"`
	ErrorRate     float64 `json:"errorRate,omitempty"`
}

// ReplicaMetrics is the app-server component view of PodMetrics.
type ReplicaMetrics = PodMetrics

// BackendPod is the lb component view of PodMetrics.
type BackendPod = PodMetrics

func emptyPeakSummary() map[string]any {
	return map[string]any{
		"peak_rps":                 0.0,
		"peak_p99_ms":              0.0,
		"error_rate":               0.0,
		"total_requests":           0.0,
		"total_cache_hits":         0.0,
		"total_cache_misses":       0.0,
		"cache_hit_rate":           0.0,
		"db_query_rate":            0.0,
		"db_total_queries":         0.0,
		"storage_read_rate":        0.0,
		"storage_write_rate":       0.0,
		"storage_read_latency_ms":  0.0,
		"storage_write_latency_ms": 0.0,
		"storage_utilization_pct":  0.0,
		"storage_error_rate":       0.0,
	}
}

// storagePeakFields extracts peak candidates from a component's storageIo block.
func storagePeakFields(c ComponentMetrics) (readRate, writeRate, readLatMs, writeLatMs, utilPct, errRate float64) {
	io := c.StorageIO
	if io == nil {
		return 0, 0, 0, 0, 0, 0
	}
	readRate = io.ReadRate
	writeRate = io.WriteRate
	if io.ReadLatency > 0 {
		readLatMs = io.ReadLatency * 1000
	}
	if io.WriteLatency > 0 {
		writeLatMs = io.WriteLatency * 1000
	}
	if io.BytesLimit > 0 && io.BytesUsed >= 0 {
		utilPct = float64(io.BytesUsed) / float64(io.BytesLimit) * 100
	}
	errRate = io.ErrorRate
	return readRate, writeRate, readLatMs, writeLatMs, utilPct, errRate
}

func maxStoragePeaksFromComponents(components map[string]ComponentMetrics) (readRate, writeRate, readLatMs, writeLatMs, utilPct, errRate float64) {
	for _, key := range []string{"db", "cache"} {
		c, ok := components[key]
		if !ok {
			continue
		}
		r, w, rl, wl, u, e := storagePeakFields(c)
		if r > readRate {
			readRate = r
		}
		if w > writeRate {
			writeRate = w
		}
		if rl > readLatMs {
			readLatMs = rl
		}
		if wl > writeLatMs {
			writeLatMs = wl
		}
		if u > utilPct {
			utilPct = u
		}
		if e > errRate {
			errRate = e
		}
	}
	return readRate, writeRate, readLatMs, writeLatMs, utilPct, errRate
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
	var maxStorageRead, maxStorageWrite, maxStorageReadLatMs, maxStorageWriteLatMs, maxStorageUtilPct, maxStorageErr float64
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
		sr, sw, srl, swl, su, se := maxStoragePeaksFromComponents(dm.Components)
		if sr > maxStorageRead {
			maxStorageRead = sr
		}
		if sw > maxStorageWrite {
			maxStorageWrite = sw
		}
		if srl > maxStorageReadLatMs {
			maxStorageReadLatMs = srl
		}
		if swl > maxStorageWriteLatMs {
			maxStorageWriteLatMs = swl
		}
		if su > maxStorageUtilPct {
			maxStorageUtilPct = su
		}
		if se > maxStorageErr {
			maxStorageErr = se
		}
	}
	if !haveHitRate {
		minHitRate = 0
	}
	return map[string]any{
		"peak_rps":                 peakRPS,
		"peak_p99_ms":              peakP99ms,
		"error_rate":               peakErr,
		"total_requests":           maxTotalReq,
		"total_cache_hits":         maxHits,
		"total_cache_misses":       maxMisses,
		"cache_hit_rate":           minHitRate,
		"db_query_rate":            maxDbRate,
		"db_total_queries":         maxDbTotal,
		"storage_read_rate":        maxStorageRead,
		"storage_write_rate":       maxStorageWrite,
		"storage_read_latency_ms":  maxStorageReadLatMs,
		"storage_write_latency_ms": maxStorageWriteLatMs,
		"storage_utilization_pct":  maxStorageUtilPct,
		"storage_error_rate":       maxStorageErr,
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
	sr, sw, srl, swl, su, se := maxStoragePeaksFromComponents(dm.Components)
	return map[string]any{
		"peak_rps":                 app.RequestRate,
		"peak_p99_ms":              app.P99Latency * 1000,
		"error_rate":               app.ErrorRate,
		"total_requests":           app.TotalRequests,
		"total_cache_hits":         cache.CacheHitsLast9m,
		"total_cache_misses":       cache.CacheMissesLast9m,
		"cache_hit_rate":           lifetimeHitRate(cache),
		"db_query_rate":            db.DbQueryRate,
		"db_total_queries":         db.DbTotalQueries,
		"storage_read_rate":        sr,
		"storage_write_rate":       sw,
		"storage_read_latency_ms":  srl,
		"storage_write_latency_ms": swl,
		"storage_utilization_pct":  su,
		"storage_error_rate":       se,
	}
}
