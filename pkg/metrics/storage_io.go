package metrics

import (
	"context"
	"fmt"
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
		// relname!~"_c9_.*" drops Cloud9 probe tables (_c9_probe); app hit/miss and db_query_rate
		// come from app-pod PromQL and are unaffected.
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
			return fmt.Sprintf(`sum(rate(pg_stat_database_xact_rollback{namespace=%q,datname!=""}%s))`, ns, promRateRange())
		},
	},
	StorageEngineRedis: {
		Engine: StorageEngineRedis,
		Unit:   "keys/s",
		Source: "redis_exporter",
		ReadQuery: func(ns string) string {
			return fmt.Sprintf(`sum(rate(redis_commands_total{namespace=%q,cmd=~"get|mget|hget|hmget"}%s))`, ns, promRateRange())
		},
		WriteQuery: func(ns string) string {
			return fmt.Sprintf(`sum(rate(redis_commands_total{namespace=%q,cmd=~"set|del|mset|hset|hdel"}%s))`, ns, promRateRange())
		},
		ReadLatencyQuery: func(ns, _ string) string {
			return fmt.Sprintf(
				`histogram_quantile(0.99, sum(rate(redis_commands_latencies_usec_bucket{namespace=%q,cmd=~"get|mget"}%s)) by (le)) / 1e6`,
				ns, promRateRange(),
			)
		},
		WriteLatencyQuery: func(ns, _ string) string {
			return fmt.Sprintf(
				`histogram_quantile(0.99, sum(rate(redis_commands_latencies_usec_bucket{namespace=%q,cmd=~"set|del"}%s)) by (le)) / 1e6`,
				ns, promRateRange(),
			)
		},
		BytesUsedQuery: func(ns string) string {
			return `sum(redis_memory_used_bytes{namespace="` + ns + `"})`
		},
		BytesLimitQuery: func(ns string) string {
			return `max(redis_memory_max_bytes{namespace="` + ns + `"})`
		},
		ErrorRateQuery: func(ns string) string {
			return fmt.Sprintf(`sum(rate(redis_rejected_connections_total{namespace=%q}%s))`, ns, promRateRange())
		},
	},
	StorageEngineCassandra: {
		Engine: StorageEngineCassandra,
		Unit:   "ops/s",
		Source: "cassandra_exporter",
		// PromQL wired when cassandra deploy + exporter land (phase 3).
	},
}

// postgresUserTableMetricFilter scopes pg_stat_user_tables to student-visible tables in a namespace.
func postgresUserTableMetricFilter(ns string) string {
	return `namespace="` + ns + `",datname!~"template0|template1",relname!~"_c9_.*"`
}

// postgresUserTableReadRateQuery sums live rows read from user tables (seq + index heap fetches).
func postgresUserTableReadRateQuery(ns string) string {
	filter := postgresUserTableMetricFilter(ns)
	rw := promRateRange()
	return `sum(rate(pg_stat_user_tables_seq_tup_read{` + filter + `}` + rw + `))` +
		` + sum(rate(pg_stat_user_tables_idx_tup_fetch{` + filter + `}` + rw + `))`
}

// postgresUserTableWriteRateQuery sums row inserts, updates, and deletes on user tables.
func postgresUserTableWriteRateQuery(ns string) string {
	filter := postgresUserTableMetricFilter(ns)
	rw := promRateRange()
	return `sum(rate(pg_stat_user_tables_n_tup_ins{` + filter + `}` + rw + `))` +
		` + sum(rate(pg_stat_user_tables_n_tup_upd{` + filter + `}` + rw + `))` +
		` + sum(rate(pg_stat_user_tables_n_tup_del{` + filter + `}` + rw + `))`
}

// postgresUserTableReadTotal9mQuery sums rows read from user tables over the rolling 9m window.
func postgresUserTableReadTotal9mQuery(ns string) string {
	filter := postgresUserTableMetricFilter(ns)
	return `sum(increase(pg_stat_user_tables_seq_tup_read{` + filter + `}[9m]))` +
		` + sum(increase(pg_stat_user_tables_idx_tup_fetch{` + filter + `}[9m]))`
}

// postgresUserTableWriteTotal9mQuery sums row writes on user tables over the rolling 9m window.
func postgresUserTableWriteTotal9mQuery(ns string) string {
	filter := postgresUserTableMetricFilter(ns)
	return `sum(increase(pg_stat_user_tables_n_tup_ins{` + filter + `}[9m]))` +
		` + sum(increase(pg_stat_user_tables_n_tup_upd{` + filter + `}[9m]))` +
		` + sum(increase(pg_stat_user_tables_n_tup_del{` + filter + `}[9m]))`
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
	out.ReadTotal9m = live.ReadTotal9m
	out.WriteTotal9m = live.WriteTotal9m
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
		switch template {
		case "social-feed":
			if io.ReadLabel == "" {
				io.ReadLabel = "ZREVRANGE commands/s"
			}
			if io.WriteLabel == "" {
				io.WriteLabel = "ZADD/ZREMRANGE commands/s"
			}
			if io.Footnote == "" {
				io.Footnote = "Feed timelines use sorted-set commands (ZREVRANGE/ZADD), not keyspace GET/SET."
			}
		default:
			if io.ReadLabel == "" {
				io.ReadLabel = "GET commands/s"
			}
			if io.WriteLabel == "" {
				io.WriteLabel = "SET/DEL commands/s"
			}
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

func redisStorageReadRateQuery(ns, template string) string {
	cmd := redisStorageReadCmdPattern(template)
	return fmt.Sprintf(`sum(rate(redis_commands_total{namespace=%q,cmd=~%q}%s))`, ns, cmd, promRateRange())
}

func redisStorageWriteRateQuery(ns, template string) string {
	cmd := redisStorageWriteCmdPattern(template)
	return fmt.Sprintf(`sum(rate(redis_commands_total{namespace=%q,cmd=~%q}%s))`, ns, cmd, promRateRange())
}

func redisStorageReadTotal9mQuery(ns, template string) string {
	cmd := redisStorageReadCmdPattern(template)
	return fmt.Sprintf(`sum(increase(redis_commands_total{namespace=%q,cmd=~%q}[9m]))`, ns, cmd)
}

func redisStorageWriteTotal9mQuery(ns, template string) string {
	cmd := redisStorageWriteCmdPattern(template)
	return fmt.Sprintf(`sum(increase(redis_commands_total{namespace=%q,cmd=~%q}[9m]))`, ns, cmd)
}

func redisStorageReadLatencyQuery(ns, template string) string {
	cmd := redisStorageReadLatencyCmdPattern(template)
	return fmt.Sprintf(
		`histogram_quantile(0.99, sum(rate(redis_commands_latencies_usec_bucket{namespace=%q,cmd=~%q}%s)) by (le)) / 1e6`,
		ns, cmd, promRateRange(),
	)
}

func redisStorageWriteLatencyQuery(ns, template string) string {
	cmd := redisStorageWriteLatencyCmdPattern(template)
	return fmt.Sprintf(
		`histogram_quantile(0.99, sum(rate(redis_commands_latencies_usec_bucket{namespace=%q,cmd=~%q}%s)) by (le)) / 1e6`,
		ns, cmd, promRateRange(),
	)
}

// collectStorageIO runs registry PromQL and returns engine-native storage metrics.
// template adjusts Redis read/write command filters (e.g. social-feed sorted-set ops).
func (c *MetricsCollector) collectStorageIO(ctx context.Context, ns, nsSel string, engine StorageEngineID, template string) *StorageIO {
	prov, ok := LookupStorageProvider(engine)
	if !ok || c == nil || c.prometheusURL == "" {
		return nil
	}
	io := &StorageIO{Unit: prov.Unit, Source: prov.Source}
	switch engine {
	case StorageEngineRedis:
		io.ReadRate, _ = c.query(ctx, redisStorageReadRateQuery(ns, template))
		io.WriteRate, _ = c.query(ctx, redisStorageWriteRateQuery(ns, template))
		io.ReadTotal9m, _ = c.query(ctx, redisStorageReadTotal9mQuery(ns, template))
		io.WriteTotal9m, _ = c.query(ctx, redisStorageWriteTotal9mQuery(ns, template))
		io.ReadLatency, _ = c.query(ctx, redisStorageReadLatencyQuery(ns, template))
		io.WriteLatency, _ = c.query(ctx, redisStorageWriteLatencyQuery(ns, template))
	default:
		if prov.ReadQuery != nil {
			io.ReadRate, _ = c.query(ctx, prov.ReadQuery(ns))
		}
		if prov.WriteQuery != nil {
			io.WriteRate, _ = c.query(ctx, prov.WriteQuery(ns))
		}
		if engine == StorageEnginePostgreSQL {
			io.ReadTotal9m, _ = c.query(ctx, postgresUserTableReadTotal9mQuery(ns))
			io.WriteTotal9m, _ = c.query(ctx, postgresUserTableWriteTotal9mQuery(ns))
		}
		if prov.ReadLatencyQuery != nil {
			io.ReadLatency, _ = c.query(ctx, prov.ReadLatencyQuery(ns, nsSel))
		}
		if prov.WriteLatencyQuery != nil {
			io.WriteLatency, _ = c.query(ctx, prov.WriteLatencyQuery(ns, nsSel))
		}
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
