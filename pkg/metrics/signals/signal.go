// Package signals declares PromQL signals and renders cluster and namespace forms.
// It does not query Prometheus.
package signals

type ID string

const (
	AppRequestRate     ID = "app_request_rate"
	AppErrorRate       ID = "app_error_rate"
	AppLatencyP50      ID = "app_latency_p50"
	AppLatencyP95      ID = "app_latency_p95"
	AppLatencyP99      ID = "app_latency_p99"
	AppProcessCPU      ID = "app_process_cpu"
	AppProcessMemory   ID = "app_process_memory"
	AppCacheHitsRate   ID = "app_cache_hits_rate"
	AppCacheMissesRate ID = "app_cache_misses_rate"
	AppCacheErrorsRate ID = "app_cache_errors_rate"

	PgDatabaseXactCommitRate      ID = "pg_database_xact_commit_rate"
	PgDatabaseXactRollbackRate    ID = "pg_database_xact_rollback_rate"
	PgDatabaseXactCommitRatePod   ID = "pg_database_xact_commit_rate_pod"
	PgDatabaseXactRollbackRatePod ID = "pg_database_xact_rollback_rate_pod"
	PgDatabaseXactCommitTotal     ID = "pg_database_xact_commit_total"
	PgDatabaseXactRollbackTotal   ID = "pg_database_xact_rollback_total"
	PgUserTableSeqTupReadRate     ID = "pg_user_table_seq_tup_read_rate"
	PgUserTableIdxTupFetchRate    ID = "pg_user_table_idx_tup_fetch_rate"
	PgUserTableNTupInsRate        ID = "pg_user_table_n_tup_ins_rate"
	PgUserTableNTupUpdRate        ID = "pg_user_table_n_tup_upd_rate"
	PgUserTableNTupDelRate        ID = "pg_user_table_n_tup_del_rate"
	PgUserTableSeqTupReadTotal    ID = "pg_user_table_seq_tup_read_total"
	PgUserTableIdxTupFetchTotal   ID = "pg_user_table_idx_tup_fetch_total"
	PgUserTableNTupInsTotal       ID = "pg_user_table_n_tup_ins_total"
	PgUserTableNTupUpdTotal       ID = "pg_user_table_n_tup_upd_total"
	PgUserTableNTupDelTotal       ID = "pg_user_table_n_tup_del_total"

	RedisCPUUserRate                         ID = "redis_cpu_user_rate"
	RedisCPUSysRate                          ID = "redis_cpu_sys_rate"
	RedisMemoryUsedBytes                     ID = "redis_memory_used_bytes"
	RedisMemoryMaxBytes                      ID = "redis_memory_max_bytes"
	RedisEvictedKeysRate                     ID = "redis_evicted_keys_rate"
	RedisExpiredKeysRate                     ID = "redis_expired_keys_rate"
	RedisEvictedKeysTotal                    ID = "redis_evicted_keys_total"
	RedisExpiredKeysTotal                    ID = "redis_expired_keys_total"
	MemcachedCommandsRate                    ID = "memcached_commands_rate"
	MemcachedGetHitRate                      ID = "memcached_get_hit_rate"
	MemcachedGetMissRate                     ID = "memcached_get_miss_rate"
	MemcachedCurrentConnections              ID = "memcached_current_connections"
	MemcachedConnectionsListenerDisabledRate ID = "memcached_connections_listener_disabled_rate"
	MemcachedItemsEvictedRate                ID = "memcached_items_evicted_rate"
	MemcachedItemsReclaimedRate              ID = "memcached_items_reclaimed_rate"
	MemcachedItemsEvictedTotal               ID = "memcached_items_evicted_total"
	MemcachedItemsReclaimedTotal             ID = "memcached_items_reclaimed_total"

	ContainerCPULimit    ID = "container_cpu_limit"
	ContainerMemoryLimit ID = "container_memory_limit"
)

type Shape int

const (
	PerNamespace Shape = iota
	PerApp
	PerPod
	PerBucket
)

type Missing int

const (
	MissingIsZero Missing = iota
	MissingIsUnknown
)

// Signal describes one PromQL value the metrics collector may read via the hub or direct query.
type Signal struct {
	ID      ID
	Expr    string
	Shape   Shape
	Missing Missing
	Metrics []string
}
