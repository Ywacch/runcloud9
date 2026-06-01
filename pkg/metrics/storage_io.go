package metrics

import (
	"context"
	"strings"
)

// StorageEngineID identifies a persistence engine for metrics collection.
type StorageEngineID string

const (
	StorageEnginePostgreSQL StorageEngineID = "postgresql"
	StorageEngineRedis      StorageEngineID = "redis"
	StorageEngineCassandra  StorageEngineID = "cassandra"
)

// Storage role labels describe how the canvas uses a storage component.
const (
	StorageRolePrimary      = "primary"
	StorageRoleCacheAside   = "cache-aside"
	StorageRoleReplica      = "replica"
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

	ReadQuery         PromQLBuilder
	WriteQuery        PromQLBuilder
	ReadLatencyQuery  AppPromQLBuilder
	WriteLatencyQuery AppPromQLBuilder
	BytesUsedQuery    PromQLBuilder
	BytesLimitQuery   PromQLBuilder
	ErrorRateQuery    PromQLBuilder
}

var storageIORegistry = map[StorageEngineID]StorageIOProvider{
	StorageEnginePostgreSQL: {
		Engine: StorageEnginePostgreSQL,
		Unit:   "rows/s",
		Source: "postgres_exporter",
		// User-table counters exclude postgres_exporter catalog queries (pg_stat_database.tup_returned
		// counts every row returned to any client, including sidecar stats scrapes).
		ReadQuery: func(ns string) string {
			return postgresUserTableReadRateQuery(ns)
		},
		WriteQuery: func(ns string) string {
			return postgresUserTableWriteRateQuery(ns)
		},
		// Latency split deferred until templates use a shared operation="read"|"write" convention.
		BytesUsedQuery: func(ns string) string {
			return `sum(pg_database_size_bytes{namespace="` + ns + `",datname!~"template0|template1"})`
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

// postgresUserTableReadRateQuery sums live rows read from user tables (seq + index heap fetches).
func postgresUserTableReadRateQuery(ns string) string {
	filter := `namespace="` + ns + `",datname!~"template0|template1"`
	return `sum(rate(pg_stat_user_tables_seq_tup_read{` + filter + `}[30s]))` +
		` + sum(rate(pg_stat_user_tables_idx_tup_fetch{` + filter + `}[30s]))`
}

// postgresUserTableWriteRateQuery sums row inserts, updates, and deletes on user tables.
func postgresUserTableWriteRateQuery(ns string) string {
	filter := `namespace="` + ns + `",datname!~"template0|template1"`
	return `sum(rate(pg_stat_user_tables_n_tup_ins{` + filter + `}[30s]))` +
		` + sum(rate(pg_stat_user_tables_n_tup_upd{` + filter + `}[30s]))` +
		` + sum(rate(pg_stat_user_tables_n_tup_del{` + filter + `}[30s]))`
}

// LookupStorageProvider returns the metrics provider for an engine id, if registered.
func LookupStorageProvider(engine StorageEngineID) (StorageIOProvider, bool) {
	p, ok := storageIORegistry[engine]
	return p, ok
}

// mergeStorageIO overlays live exporter metrics onto the design-time stub (unit/source).
func mergeStorageIO(stub, live *StorageIO) *StorageIO {
	if stub == nil && live == nil {
		return nil
	}
	out := StorageIO{}
	if stub != nil {
		out.Unit = stub.Unit
		out.Source = stub.Source
	}
	if live == nil {
		return &out
	}
	if live.Unit != "" {
		out.Unit = live.Unit
	}
	if live.Source != "" {
		out.Source = live.Source
	}
	out.ReadRate = live.ReadRate
	out.WriteRate = live.WriteRate
	out.ReadLatency = live.ReadLatency
	out.WriteLatency = live.WriteLatency
	out.BytesUsed = live.BytesUsed
	out.BytesLimit = live.BytesLimit
	out.ErrorRate = live.ErrorRate
	if live.ReadLabel != "" {
		out.ReadLabel = live.ReadLabel
	}
	if live.WriteLabel != "" {
		out.WriteLabel = live.WriteLabel
	}
	if live.Scope != "" {
		out.Scope = live.Scope
	}
	if live.Footnote != "" {
		out.Footnote = live.Footnote
	}
	return &out
}

// finalizeStorageIO sets presentation fields (labels, scope, footnote) for the inspector.
func finalizeStorageIO(io *StorageIO, engine StorageEngineID, topo TopologyFlags, template string) {
	if io == nil {
		return
	}
	switch engine {
	case StorageEnginePostgreSQL:
		if io.ReadLabel == "" {
			io.ReadLabel = "Rows scanned/s"
		}
		if io.WriteLabel == "" {
			io.WriteLabel = "Rows written/s"
		}
		if io.Scope == "" {
			io.Scope = postgresStorageScope(topo)
		}
		if io.Footnote == "" {
			io.Footnote = postgresStorageFootnote(topo, template)
		}
	case StorageEngineRedis:
		if io.ReadLabel == "" {
			io.ReadLabel = "GET commands/s"
		}
		if io.WriteLabel == "" {
			io.WriteLabel = "SET/DEL commands/s"
		}
		if io.Scope == "" {
			io.Scope = "engine-native"
		}
	default:
		if io.Scope == "" {
			io.Scope = "engine-native"
		}
	}
}

func postgresStorageScope(topo TopologyFlags) string {
	if topo.HasWorker {
		return "all-db-clients (app-server, worker)"
	}
	return "all-db-clients"
}

func postgresStorageFootnote(topo TopologyFlags, template string) string {
	if !topo.HasWorker {
		return ""
	}
	if strings.HasPrefix(template, "social-feed") {
		return "Row scans include fanout-worker follower lookups on each new post. Feed reads use Redis when push mode is enabled."
	}
	return "Row scans include all database clients (e.g. workers), not only the app-server."
}

// ApplyStorageProfile sets storageEngine, storageRole, and a storageIo stub (unit/source only).
// Live rates are filled by collectStorageIO after applyStorageProfiles.
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

// collectStorageIO runs registry PromQL and returns engine-native storage metrics.
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
