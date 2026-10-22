package rules

import (
	"fmt"
	"strconv"

	"github.com/ywacch/cloud9/pkg/deployer"
)

// Mechanism is an authored causal story keyed to catalog rules and design choices.
type Mechanism struct {
	ID           string       `json:"id"`
	TriggerRules []string     `json:"triggerRules"`
	Precondition Precondition `json:"precondition"`
	CausalChain  []string     `json:"causalChain"`
	Suggestion   string       `json:"suggestion"`
}

// Precondition is machine-checkable against a session design_config projection.
type Precondition struct {
	Role  string   `json:"role,omitempty"` // "cache" | "db" | ...
	Key   string   `json:"key,omitempty"`  // "cacheStrategy"
	AnyOf []string `json:"anyOf,omitempty"`
}

// Holds reports whether any instance under Role has Settings[Key] matching AnyOf.
// An empty precondition always holds.
func (p Precondition) Holds(dc deployer.DesignConfig) bool {
	if p.Role == "" && p.Key == "" && len(p.AnyOf) == 0 {
		return true
	}
	if p.Role == "" || p.Key == "" || len(p.AnyOf) == 0 {
		return false
	}
	instances := dc.Roles[p.Role]
	if len(instances) == 0 {
		return false
	}
	for _, inst := range instances {
		value, ok := inst.Settings[p.Key]
		if !ok {
			continue
		}
		if SettingMatchesAny(value, p.AnyOf) {
			return true
		}
	}
	return false
}

// MechanismsFor returns authored mechanisms whose trigger rule fired and whose
// precondition matches the session design.
func MechanismsFor(dc deployer.DesignConfig, firedRules []string) []Mechanism {
	if len(firedRules) == 0 {
		return nil
	}
	fired := make(map[string]bool, len(firedRules))
	for _, id := range firedRules {
		if id != "" {
			fired[id] = true
		}
	}
	var out []Mechanism
	for _, m := range allMechanisms {
		if !mechanismTriggered(m, fired) {
			continue
		}
		if !m.Precondition.Holds(dc) {
			continue
		}
		out = append(out, m)
	}
	return out
}

func mechanismTriggered(m Mechanism, fired map[string]bool) bool {
	for _, ruleID := range m.TriggerRules {
		if fired[ruleID] {
			return true
		}
	}
	return false
}

// SettingMatchesAny reports whether value stringifies to one of the allowed literals.
func SettingMatchesAny(value any, allowed []string) bool {
	got := formatSettingValue(value)
	for _, want := range allowed {
		if got == want {
			return true
		}
	}
	return false
}

func formatSettingValue(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", value)
	}
}

var allMechanisms = []Mechanism{
	{
		ID:           "cache_hit_low_fallthrough",
		TriggerRules: []string{IDCacheHitLow},
		Precondition: Precondition{Role: "cache", Key: "cacheStrategy", AnyOf: []string{"cache-aside", "write-through", "write-behind"}},
		CausalChain: []string{
			"working set exceeds effective cache capacity or TTL is too short",
			"hit rate falls as keys are evicted or expire",
			"more reads miss Redis and query Postgres directly",
		},
		Suggestion: "Add Redis memory or cache capacity, tune TTL/eviction, or scale the cache tier.",
	},
	{
		ID:           "error_rate_upstream_saturation",
		TriggerRules: []string{IDErrorRate},
		CausalChain: []string{
			"request rate rises or downstream latency increases",
			"app handlers time out or return 5xx under pressure",
			"error rate crosses the session threshold",
		},
		Suggestion: "Add app-server replicas behind the load balancer and relieve hot downstream tiers (cache, DB).",
	},
	{
		ID:           "db_latency_primary_pressure",
		TriggerRules: []string{IDDBLatency},
		Precondition: Precondition{Role: "db", Key: "readReplicas", AnyOf: []string{"0", "1", "2", "3"}},
		CausalChain: []string{
			"queries concentrate on the primary or a single replica",
			"Postgres CPU or I/O saturates",
			"average query latency rises above the threshold",
		},
		Suggestion: "Add a cache tier for hot reads or add read replicas to spread query load.",
	},
	{
		ID:           "queue_depth_consumer_lag",
		TriggerRules: []string{IDQueueDepth},
		Precondition: Precondition{Role: "worker", Key: "workerConcurrency", AnyOf: []string{"1", "2", "4", "8", "16", "32"}},
		CausalChain: []string{
			"publish rate exceeds aggregate consumer throughput",
			"RabbitMQ queue depth grows",
			"workers remain busy but backlog persists",
		},
		Suggestion: "Add worker replicas or raise worker concurrency/prefetch cautiously.",
	},
	{
		ID:           "db_conn_pool_exhaustion",
		TriggerRules: []string{IDDBConnPressure},
		CausalChain: []string{
			"each app replica opens sessions against Postgres",
			"active connections approach max_connections",
			"new queries block waiting for a pool slot",
		},
		Suggestion: "Add a cache tier to cut read load, or add read replicas so sessions spread across instances.",
	},
	{
		ID:           "mq_publish_backpressure",
		TriggerRules: []string{IDMQBackpressure},
		CausalChain: []string{
			"publish rate exceeds consume rate",
			"queue depth and saturation ratio rise together",
			"downstream writers fall behind visible backlog",
		},
		Suggestion: "Add worker replicas to raise aggregate consume throughput.",
	},
	{
		ID:           "cascade_cache_db_fallthrough",
		TriggerRules: []string{IDCascadeCacheDB},
		Precondition: Precondition{Role: "cache", Key: "cacheStrategy", AnyOf: []string{"cache-aside", "write-through", "write-behind"}},
		CausalChain: []string{
			"cache hit rate drops between ticks",
			"DB query rate rises in the same window",
			"Postgres absorbs work that Redis previously served",
		},
		Suggestion: "Enlarge the cache tier or tune eviction/TTL so hot keys stay resident.",
	},
	{
		ID:           "cascade_queue_worker_cpu",
		TriggerRules: []string{IDCascadeQueueWorker},
		CausalChain: []string{
			"queue depth rises while worker CPU climbs",
			"each message triggers substantial downstream work",
			"consumers stay busy without draining arrival rate",
		},
		Suggestion: "Add worker replicas so fan-out and queue draining run in parallel.",
	},
	{
		ID:           "cascade_dbpool_errors",
		TriggerRules: []string{IDCascadeDBPoolErrors},
		CausalChain: []string{
			"database connections approach the configured limit",
			"new sessions cannot acquire a connection",
			"the app-server returns HTTP errors to clients",
		},
		Suggestion: "Relieve primary read pressure with cache or read replicas.",
	},
	{
		ID:           "cascade_latency_cache_miss",
		TriggerRules: []string{IDCascadeLatencyMiss},
		CausalChain: []string{
			"cache miss share rises in the rolling window",
			"app p99 latency climbs in the same interval",
			"requests pay the full database latency stack",
		},
		Suggestion: "Add or enlarge the cache tier, or add read replicas if the database dominates latency.",
	},
	{
		ID:           "cascade_mq_consumer_gap",
		TriggerRules: []string{IDCascadeMQBackpressure},
		CausalChain: []string{
			"produce rate stays above consume rate across ticks",
			"queue depth and saturation rise together",
			"publish-side backpressure becomes visible",
		},
		Suggestion: "Add worker replicas to close the produce/consume gap.",
	},
	{
		ID:           "async_promote_write_loss",
		TriggerRules: []string{IDFailoverWriteLoss},
		Precondition: Precondition{Role: "db", Key: "replicationMode", AnyOf: []string{"async"}},
		CausalChain: []string{
			"async replication lets the primary acknowledge before all standbys apply WAL",
			"failover promotes a replica that lagged the fenced primary",
			"commits the client already saw may be absent on the new leader",
		},
		Suggestion: "Use semi-sync replication with remote_apply if you need fewer lost commits during failover drills.",
	},
	{
		ID:           "semi_sync_promote_gap",
		TriggerRules: []string{IDFailoverWriteLoss},
		Precondition: Precondition{Role: "db", Key: "replicationMode", AnyOf: []string{"semi-sync"}},
		CausalChain: []string{
			"a promoted standby was not the most caught-up replica",
			"semi-sync reduced but did not eliminate exposure during promotion",
			"failover surfaced bytes still missing on the new primary",
		},
		Suggestion: "Promote the most current replica and verify sync state before declaring the partition healed.",
	},
	{
		ID:           "partition_stale_reads_async",
		TriggerRules: []string{IDPartitionStaleReads},
		Precondition: Precondition{Role: "db", Key: "replicationMode", AnyOf: []string{"async", "semi-sync"}},
		CausalChain: []string{
			"network partition or replica lag separates readers from the latest primary writes",
			"replica reads continue while the primary keeps accepting writes",
			"clients observe data older than their last write",
		},
		Suggestion: "Tighten read consistency, reduce replica lag, or heal the partition before serving replica reads.",
	},
	{
		ID:           "partition_write_stall_semi_sync",
		TriggerRules: []string{IDPartitionWriteStall},
		Precondition: Precondition{Role: "db", Key: "replicationMode", AnyOf: []string{"semi-sync"}},
		CausalChain: []string{
			"synchronous replication waits for standby acknowledgment",
			"partition isolates the primary from a required replica",
			"commits block to preserve durability",
		},
		Suggestion: "Heal the partition or accept async exposure if write availability matters more than durability.",
	},
	{
		ID:           "replica_cannot_catch_up",
		TriggerRules: []string{IDReplicaCannotCatchUp},
		Precondition: Precondition{Role: "db", Key: "readReplicas", AnyOf: []string{"1", "2", "3", "4", "5"}},
		CausalChain: []string{
			"primary WAL generation outpaces replica apply capacity",
			"replication lag grows without recovering",
			"replica reads stay stale or replicas disconnect",
		},
		Suggestion: "Scale replica instance size, reduce write pressure, or remove overloaded replicas.",
	},
	{
		ID:           "ryw_violation_replica_route",
		TriggerRules: []string{IDRYWViolation},
		Precondition: Precondition{Role: "db", Key: "readConsistency", AnyOf: []string{"replica", "ryw", "ryw+monotonic"}},
		CausalChain: []string{
			"reads are routed to replicas for scale",
			"a write has not replayed to the replica yet",
			"the client reads older data than it just wrote",
		},
		Suggestion: "Use read-your-writes routing, leader-only reads, or wait for replication before serving reads.",
	},
	{
		ID:           "reply_before_parent_staggered_replicas",
		TriggerRules: []string{IDReplyBeforeParent},
		Precondition: Precondition{Role: "db", Key: "readReplicas", AnyOf: []string{"2", "3", "4", "5"}},
		CausalChain: []string{
			"each thread read chooses a replica independently",
			"standbys replay with staggered apply delays",
			"the reply row is visible on the fresher standby while the parent row is still missing on the staler one",
		},
		Suggestion: "Pin each client to one replica (readConsistency=sticky), or read the thread from one standby in one query.",
	},
	{
		ID:           "monotonic_violation_replica_route",
		TriggerRules: []string{IDMonotonicViolation},
		Precondition: Precondition{Role: "db", Key: "readConsistency", AnyOf: []string{"monotonic", "ryw+monotonic"}},
		CausalChain: []string{
			"replica reads observe WAL positions out of order",
			"a later read returns an earlier version than a prior read",
			"monotonic read order is broken on real traffic",
		},
		Suggestion: "Pin monotonic clients to a single replica or strengthen routing guarantees.",
	},
	{
		ID:           "cache_db_stale_proxy",
		TriggerRules: []string{IDCacheDBStale},
		Precondition: Precondition{Role: "cache", Key: "cacheStrategy", AnyOf: []string{"write-through", "write-behind"}},
		CausalChain: []string{
			"cache proxy serves a value from Redis",
			"Postgres holds a newer truth for the same key",
			"the client reads stale cache data relative to the database",
		},
		Suggestion: "Verify write-through ordering, shorten write-behind flush intervals, or invalidate hot keys after writes.",
	},
	{
		ID:           "pod_oom_limit",
		TriggerRules: []string{IDPodOOM},
		CausalChain: []string{
			"container memory exceeds its cgroup limit",
			"the kernel OOM killer terminates the process",
			"Kubernetes restarts the pod under memory pressure",
		},
		Suggestion: "Raise instance size or reduce per-pod memory working set.",
	},
	{
		ID:           "pod_restart_runtime",
		TriggerRules: []string{IDPodRestart},
		CausalChain: []string{
			"the container exits or fails health checks",
			"Kubernetes increments restart count",
			"traffic shifts while the pod recovers",
		},
		Suggestion: "Inspect pod logs and relieve CPU, memory, or upstream health pressure.",
	},
}
