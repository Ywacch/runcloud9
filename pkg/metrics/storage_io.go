package metrics

import "context"

// StorageEngineID identifies a persistence engine for metrics collection.
type StorageEngineID string

const (
	StorageEnginePostgreSQL StorageEngineID = "postgresql"
	StorageEngineRedis      StorageEngineID = "redis"
	StorageEngineCassandra  StorageEngineID = "cassandra"
)

// Storage role labels describe how the canvas uses a storage component.
const (
	StorageRolePrimary    = "primary"
	StorageRoleCacheAside = "cache-aside"
	StorageRoleReplica    = "replica"
	StorageRoleWriteThrough = "write-through"
)

// PromQLBuilder returns a namespace-scoped PromQL query string.
type PromQLBuilder func(ns string) string

// AppPromQLBuilder receives namespace and app-metrics label selector (for histogram splits).
type AppPromQLBuilder func(ns, nsSel string) string

// StorageIOProvider defines PromQL templates for one engine. Phase 2+ calls these from collectStorageIO.
type StorageIOProvider struct {
	Engine StorageEngineID
	Unit   string
	Source string

	ReadQuery           PromQLBuilder
	WriteQuery          PromQLBuilder
	ReadLatencyQuery    AppPromQLBuilder
	WriteLatencyQuery   AppPromQLBuilder
	BytesUsedQuery      PromQLBuilder
	BytesLimitQuery     PromQLBuilder
	ErrorRateQuery      PromQLBuilder
}

var storageIORegistry = map[StorageEngineID]StorageIOProvider{
	StorageEnginePostgreSQL: {
		Engine: StorageEnginePostgreSQL,
		Unit:   "rows/s",
		Source: "postgres_exporter",
		ReadQuery: func(ns string) string {
			return `sum(rate(pg_stat_database_tup_returned{namespace="` + ns + `",datname!=""}[30s]))`
		},
		WriteQuery: func(ns string) string {
			return `sum(rate(pg_stat_database_tup_inserted{namespace="` + ns + `",datname!=""}[30s]))` +
				` + sum(rate(pg_stat_database_tup_updated{namespace="` + ns + `",datname!=""}[30s]))` +
				` + sum(rate(pg_stat_database_tup_deleted{namespace="` + ns + `",datname!=""}[30s]))`
		},
		ReadLatencyQuery: func(_, nsSel string) string {
			return `sum(rate(db_query_duration_seconds_sum{` + nsSel + `,operation=~"get_.*"}[30s]))` +
				` / sum(rate(db_query_duration_seconds_count{` + nsSel + `,operation=~"get_.*"}[30s]))`
		},
		WriteLatencyQuery: func(_, nsSel string) string {
			return `sum(rate(db_query_duration_seconds_sum{` + nsSel + `,operation=~"insert_.*"}[30s]))` +
				` / sum(rate(db_query_duration_seconds_count{` + nsSel + `,operation=~"insert_.*"}[30s]))`
		},
		BytesUsedQuery: func(ns string) string {
			return `sum(pg_database_size_bytes{namespace="` + ns + `",datname="syscrack"})`
		},
		ErrorRateQuery: func(ns string) string {
			return `sum(rate(pg_stat_database_xact_rollback{namespace="` + ns + `",datname!=""}[30s]))`
		},
	},
	StorageEngineRedis: {
		Engine: StorageEngineRedis,
		Unit:   "keys/s",
		Source: "redis_exporter",
		ReadQuery: func(ns string) string {
			return `sum(rate(redis_commands_total{namespace="` + ns + `",cmd=~"get|mget|hget|hmget"}[30s]))`
		},
		WriteQuery: func(ns string) string {
			return `sum(rate(redis_commands_total{namespace="` + ns + `",cmd=~"set|del|mset|hset|hdel"}[30s]))`
		},
		ReadLatencyQuery: func(ns, _ string) string {
			return `histogram_quantile(0.99, sum(rate(redis_commands_latencies_usec_bucket{namespace="` + ns +
				`",cmd=~"get|mget"}[30s])) by (le)) / 1e6`
		},
		WriteLatencyQuery: func(ns, _ string) string {
			return `histogram_quantile(0.99, sum(rate(redis_commands_latencies_usec_bucket{namespace="` + ns +
				`",cmd=~"set|del"}[30s])) by (le)) / 1e6`
		},
		BytesUsedQuery: func(ns string) string {
			return `sum(redis_memory_used_bytes{namespace="` + ns + `"})`
		},
		BytesLimitQuery: func(ns string) string {
			return `max(redis_memory_max_bytes{namespace="` + ns + `"})`
		},
		ErrorRateQuery: func(ns string) string {
			return `sum(rate(redis_rejected_connections_total{namespace="` + ns + `"}[30s]))`
		},
	},
	StorageEngineCassandra: {
		Engine: StorageEngineCassandra,
		Unit:   "ops/s",
		Source: "cassandra_exporter",
		// PromQL wired when cassandra deploy + exporter land (phase 3).
	},
}

// LookupStorageProvider returns the metrics provider for an engine id, if registered.
func LookupStorageProvider(engine StorageEngineID) (StorageIOProvider, bool) {
	p, ok := storageIORegistry[engine]
	return p, ok
}

// ApplyStorageProfile sets storageEngine, storageRole, and a storageIo stub (unit/source only).
// Live rates are filled by collectStorageIO in phase 2+.
func ApplyStorageProfile(cm *ComponentMetrics, engine StorageEngineID, role string) {
	if cm == nil {
		return
	}
	prov, ok := LookupStorageProvider(engine)
	if !ok {
		cm.StorageEngine = string(engine)
		cm.StorageRole = role
		return
	}
	cm.StorageEngine = string(prov.Engine)
	cm.StorageRole = role
	cm.StorageIO = &StorageIO{
		Unit:   prov.Unit,
		Source: prov.Source,
	}
}

// collectStorageIO runs registry PromQL and populates cm.StorageIO. Not called in phase 1.
func (c *MetricsCollector) collectStorageIO(ctx context.Context, ns, nsSel string, engine StorageEngineID) *StorageIO {
	prov, ok := LookupStorageProvider(engine)
	if !ok || c == nil || c.prometheusURL == "" {
		return nil
	}
	io := &StorageIO{Unit: prov.Unit, Source: prov.Source}
	if prov.ReadQuery != nil {
		io.ReadRate, _ = c.query(ctx, prov.ReadQuery(ns))
	}
	if prov.WriteQuery != nil {
		io.WriteRate, _ = c.query(ctx, prov.WriteQuery(ns))
	}
	if prov.ReadLatencyQuery != nil {
		io.ReadLatency, _ = c.query(ctx, prov.ReadLatencyQuery(ns, nsSel))
	}
	if prov.WriteLatencyQuery != nil {
		io.WriteLatency, _ = c.query(ctx, prov.WriteLatencyQuery(ns, nsSel))
	}
	if prov.BytesUsedQuery != nil {
		if v, _ := c.query(ctx, prov.BytesUsedQuery(ns)); v > 0 {
			io.BytesUsed = int64(v)
		}
	}
	if prov.BytesLimitQuery != nil {
		if v, _ := c.query(ctx, prov.BytesLimitQuery(ns)); v > 0 {
			io.BytesLimit = int64(v)
		}
	}
	if prov.ErrorRateQuery != nil {
		io.ErrorRate, _ = c.query(ctx, prov.ErrorRateQuery(ns))
	}
	return io
}
