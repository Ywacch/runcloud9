// Package rules holds the static RuleID catalog for L1 detectors and L3 brain context.
package rules

// CatalogVersion bumps when static rule metadata changes.
const CatalogVersion = "1"

// Family groups detector rules for filtering and narration.
type Family string

const (
	FamilyThreshold   Family = "threshold"
	FamilyCascade     Family = "cascade"
	FamilyConsistency Family = "consistency"
	FamilyPartition   Family = "partition"
	FamilyFailover    Family = "failover"
	FamilyRuntime     Family = "runtime"
	FamilyInvariant   Family = "invariant"
)

// Rule describes one stable RuleID emitted by Go detectors or template invariants.
type Rule struct {
	ID             string
	Family         Family
	Kind           string   // template invariant kind; empty for static detector rules
	Component      string   // primary canvas role; empty when cross-cutting
	Audience       string   // "student" | "both" | "operator"
	Summary        string   // one line, student-readable
	CausedByConfig []string // design_config keys that make this rule possible
}

// Static catalog ids (template invariants overlay in CatalogFor, PR-3).
const (
	IDCacheHitLow           = "cache_hit_low"
	IDErrorRate             = "error_rate"
	IDDBLatency             = "db_latency"
	IDQueueDepth            = "queue_depth"
	IDDBConnPressure        = "db_conn_pressure"
	IDMQBackpressure        = "mq_backpressure"
	IDCascadeCacheDB        = "cascade_cache_db"
	IDCascadeQueueWorker    = "cascade_queue_worker"
	IDCascadeDBPoolErrors   = "cascade_dbpool_errors"
	IDCascadeLatencyMiss    = "cascade_latency_miss"
	IDCascadeMQBackpressure = "cascade_mq_backpressure"
	IDRYWViolation          = "ryw_violation"
	IDMonotonicViolation    = "monotonic_violation"
	IDCacheDBStale          = "cache_db_stale"
	IDPartitionStaleReads   = "partition_stale_reads"
	IDPartitionWriteStall   = "partition_write_stall"
	IDReplicaCannotCatchUp  = "replica_cannot_catch_up"
	IDFailoverWriteLoss     = "failover_write_loss"
	IDPodRestart            = "pod_restart"
	IDPodOOM                = "pod_oom"
)

var staticCatalog = []Rule{
	{
		ID: IDCacheHitLow, Family: FamilyThreshold, Component: "cache", Audience: "both",
		Summary:        "Cache hit rate fell below the session threshold",
		CausedByConfig: []string{"cacheStrategy", "evictionPolicy", "cacheTtlSeconds", "instanceSize", "resourceProfile"},
	},
	{
		ID: IDErrorRate, Family: FamilyThreshold, Component: "app-server", Audience: "both",
		Summary:        "HTTP error rate exceeded the session threshold",
		CausedByConfig: []string{"replicas", "instanceSize", "resourceProfile"},
	},
	{
		ID: IDDBLatency, Family: FamilyThreshold, Component: "db", Audience: "both",
		Summary:        "Database query latency exceeded the session threshold",
		CausedByConfig: []string{"readReplicas", "readConsistency", "replicationMode", "instanceSize", "resourceProfile"},
	},
	{
		ID: IDQueueDepth, Family: FamilyThreshold, Component: "mq", Audience: "both",
		Summary:        "Message queue depth exceeded the session threshold",
		CausedByConfig: []string{"workerPrefetch", "workerConcurrency", "replicas"},
	},
	{
		ID: IDDBConnPressure, Family: FamilyThreshold, Component: "db", Audience: "both",
		Summary:        "Database connection pressure exceeded the session threshold",
		CausedByConfig: []string{"replicas", "readReplicas", "instanceSize", "resourceProfile"},
	},
	{
		ID: IDMQBackpressure, Family: FamilyThreshold, Component: "mq", Audience: "both",
		Summary:        "Message queue publish backpressure exceeded the session threshold",
		CausedByConfig: []string{"workerPrefetch", "workerConcurrency", "replicas"},
	},
	{
		ID: IDCascadeCacheDB, Family: FamilyCascade, Component: "", Audience: "both",
		Summary:        "Cache miss pressure coincided with rising database query rate",
		CausedByConfig: []string{"cacheStrategy", "evictionPolicy", "readReplicas", "readConsistency"},
	},
	{
		ID: IDCascadeQueueWorker, Family: FamilyCascade, Component: "", Audience: "both",
		Summary:        "Queue depth rose while workers were saturated",
		CausedByConfig: []string{"workerPrefetch", "workerConcurrency", "replicas"},
	},
	{
		ID: IDCascadeDBPoolErrors, Family: FamilyCascade, Component: "", Audience: "both",
		Summary:        "Database pool errors rose with application error rate",
		CausedByConfig: []string{"replicas", "readReplicas", "instanceSize", "resourceProfile"},
	},
	{
		ID: IDCascadeLatencyMiss, Family: FamilyCascade, Component: "", Audience: "both",
		Summary:        "Latency rose together with cache misses",
		CausedByConfig: []string{"cacheStrategy", "cacheTtlSeconds", "evictionPolicy"},
	},
	{
		ID: IDCascadeMQBackpressure, Family: FamilyCascade, Component: "mq", Audience: "both",
		Summary:        "Queue backpressure coincided with worker or publish pressure",
		CausedByConfig: []string{"workerPrefetch", "workerConcurrency", "replicas"},
	},
	{
		ID: IDRYWViolation, Family: FamilyConsistency, Component: "db", Audience: "student",
		Summary:        "A read-your-writes guarantee was violated on real traffic",
		CausedByConfig: []string{"readConsistency", "readReplicas", "replicationMode"},
	},
	{
		ID: IDMonotonicViolation, Family: FamilyConsistency, Component: "db", Audience: "student",
		Summary:        "Monotonic read order was violated on real traffic",
		CausedByConfig: []string{"readConsistency", "readReplicas", "replicationMode"},
	},
	{
		ID: IDCacheDBStale, Family: FamilyConsistency, Component: "cache", Audience: "student",
		Summary:        "A cache hit served stale data relative to the database",
		CausedByConfig: []string{"cacheStrategy", "cacheTtlSeconds", "writeBehindIntervalSeconds"},
	},
	{
		ID: IDPartitionStaleReads, Family: FamilyPartition, Component: "db", Audience: "student",
		Summary:        "Partition or lag allowed stale reads from a replica",
		CausedByConfig: []string{"readConsistency", "readReplicas", "replicationMode"},
	},
	{
		ID: IDPartitionWriteStall, Family: FamilyPartition, Component: "db", Audience: "student",
		Summary:        "Writes stalled under partition or semi-sync replication pressure",
		CausedByConfig: []string{"replicationMode", "syncCommitLevel", "readReplicas"},
	},
	{
		ID: IDReplicaCannotCatchUp, Family: FamilyConsistency, Component: "db", Audience: "student",
		Summary:        "A replica could not catch up to the primary WAL stream",
		CausedByConfig: []string{"readReplicas", "replicationMode", "instanceSize", "resourceProfile"},
	},
	{
		ID: IDFailoverWriteLoss, Family: FamilyFailover, Component: "db", Audience: "student",
		Summary:        "Failover promoted a standby that had not received all committed writes",
		CausedByConfig: []string{"replicationMode", "syncCommitLevel", "readReplicas"},
	},
	{
		ID: IDPodRestart, Family: FamilyRuntime, Component: "", Audience: "both",
		Summary:        "A pod restarted during the session",
		CausedByConfig: nil,
	},
	{
		ID: IDPodOOM, Family: FamilyRuntime, Component: "", Audience: "both",
		Summary:        "A pod was terminated for out-of-memory",
		CausedByConfig: []string{"instanceSize", "resourceProfile"},
	},
}

var catalogByID map[string]Rule

func init() {
	catalogByID = make(map[string]Rule, len(staticCatalog))
	for _, r := range staticCatalog {
		catalogByID[r.ID] = r
	}
}

// All returns the static catalog rules in registration order.
func All() []Rule {
	out := make([]Rule, len(staticCatalog))
	copy(out, staticCatalog)
	return out
}

// Lookup returns the static rule for id, if present.
func Lookup(id string) (Rule, bool) {
	r, ok := catalogByID[id]
	return r, ok
}
