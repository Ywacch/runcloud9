package metrics

import (
	"strings"
	"testing"
)

func TestLookupStorageProvider_PostgreSQL(t *testing.T) {
	t.Parallel()
	prov, ok := LookupStorageProvider(StorageEnginePostgreSQL)
	if !ok {
		t.Fatal("expected postgresql provider")
	}
	if prov.Unit != "rows/s" || prov.Source != "postgres_exporter" {
		t.Fatalf("provider: %+v", prov)
	}
	q := prov.ReadQuery("ns1")
	if !strings.Contains(q, `namespace="ns1"`) || !strings.Contains(q, "pg_stat_user_tables_seq_tup_read") {
		t.Fatalf("read query: %s", q)
	}
	if !strings.Contains(q, "pg_stat_user_tables_idx_tup_fetch") {
		t.Fatalf("read query missing idx_tup_fetch: %s", q)
	}
}

func TestPostgreSQLUserTableWriteQuery(t *testing.T) {
	t.Parallel()
	prov, ok := LookupStorageProvider(StorageEnginePostgreSQL)
	if !ok {
		t.Fatal("missing postgresql provider")
	}
	q := prov.WriteQuery("ns1")
	for _, want := range []string{
		"pg_stat_user_tables_n_tup_ins",
		"pg_stat_user_tables_n_tup_upd",
		"pg_stat_user_tables_n_tup_del",
		`datname!~"template0|template1"`,
		`relname!~"_c9_.*"`,
	} {
		if !strings.Contains(q, want) {
			t.Fatalf("write query missing %q: %s", want, q)
		}
	}
}

func TestApplyStorageProfile(t *testing.T) {
	t.Parallel()
	var cm ComponentMetrics
	ApplyStorageProfile(&cm, StorageEnginePostgreSQL, StorageRolePrimary)
	if cm.StorageEngine != "postgresql" || cm.StorageRole != StorageRolePrimary {
		t.Fatalf("metadata: engine=%q role=%q", cm.StorageEngine, cm.StorageRole)
	}
	if cm.StorageIO == nil || cm.StorageIO.Unit != "rows/s" {
		t.Fatalf("storageIo: %+v", cm.StorageIO)
	}
	if cm.StorageIO.ReadRate != 0 || cm.StorageIO.WriteRate != 0 {
		t.Fatalf("phase 1 stub should have zero rates, got read=%v write=%v", cm.StorageIO.ReadRate, cm.StorageIO.WriteRate)
	}
}

func TestApplyStorageProfile_UnknownEngine(t *testing.T) {
	t.Parallel()
	var cm ComponentMetrics
	ApplyStorageProfile(&cm, StorageEngineID("unknown-db"), "primary")
	if cm.StorageEngine != "unknown-db" || cm.StorageIO != nil {
		t.Fatalf("unknown engine: io=%+v engine=%q", cm.StorageIO, cm.StorageEngine)
	}
}

func TestMergeStorageIO(t *testing.T) {
	t.Parallel()
	stub := &StorageIO{Unit: "rows/s", Source: "postgres_exporter"}
	live := &StorageIO{
		Unit:         "rows/s",
		Source:       "postgres_exporter",
		ReadRate:     400,
		WriteRate:    100,
		ReadTotal9m:  5940,
		WriteTotal9m: 1200,
		BytesUsed:    1_000_000,
		ErrorRate:    0.2,
	}
	got := mergeStorageIO(stub, live)
	if got.ReadRate != 400 || got.WriteRate != 100 || got.Unit != "rows/s" {
		t.Fatalf("merge: %+v", got)
	}
	if got.ReadTotal9m != 5940 || got.WriteTotal9m != 1200 {
		t.Fatalf("9m totals: %+v", got)
	}
	if got.ReadLatency != 0 || got.WriteLatency != 0 {
		t.Fatalf("latency should stay zero until wired: %+v", got)
	}
}

func TestPostgreSQLBytesUsedQuery_ExcludesTemplates(t *testing.T) {
	t.Parallel()
	prov, ok := LookupStorageProvider(StorageEnginePostgreSQL)
	if !ok {
		t.Fatal("missing postgresql provider")
	}
	q := prov.BytesUsedQuery("ns-test")
	if !strings.Contains(q, `datname!~"template0|template1"`) {
		t.Fatalf("bytes query: %s", q)
	}
	if strings.Contains(q, "syscrack") {
		t.Fatalf("should not hardcode datname: %s", q)
	}
}

func TestFinalizeStorageIO_Postgres(t *testing.T) {
	t.Parallel()
	io := &StorageIO{Unit: "rows/s", Source: "postgres_exporter"}
	finalizeStorageIO(io, StorageEnginePostgreSQL, TopologyFlags{}, "url-shortener")
	if io.ReadLabel != "Rows scanned/s" || io.WriteLabel != "Rows written/s" {
		t.Fatalf("labels: %+v", io)
	}
	if io.Scope != "all-db-clients" {
		t.Fatalf("scope: %q", io.Scope)
	}
	if io.Footnote != "" {
		t.Fatalf("unexpected footnote: %q", io.Footnote)
	}
}

func TestFinalizeStorageIO_PostgresWorkerSocialFeed(t *testing.T) {
	t.Parallel()
	io := &StorageIO{}
	topo := TopologyFlags{HasWorker: true, HasDB: true}
	finalizeStorageIO(io, StorageEnginePostgreSQL, topo, "social-feed")
	if !strings.Contains(io.Scope, "worker") {
		t.Fatalf("scope: %q", io.Scope)
	}
	if !strings.Contains(io.Footnote, "fanout-worker") {
		t.Fatalf("footnote: %q", io.Footnote)
	}
}

func TestFinalizeStorageIO_Redis(t *testing.T) {
	t.Parallel()
	io := &StorageIO{Unit: "keys/s", Source: "redis_exporter"}
	finalizeStorageIO(io, StorageEngineRedis, TopologyFlags{HasCache: true}, "url-shortener")
	if io.ReadLabel != "GET commands/s" || io.Scope != "engine-native" {
		t.Fatalf("redis presentation: %+v", io)
	}
}

func TestFinalizeStorageIO_RedisSocialFeed(t *testing.T) {
	t.Parallel()
	io := &StorageIO{Unit: "keys/s", Source: "redis_exporter"}
	finalizeStorageIO(io, StorageEngineRedis, TopologyFlags{HasCache: true}, "social-feed")
	if io.ReadLabel != "ZREVRANGE commands/s" {
		t.Fatalf("readLabel: %q", io.ReadLabel)
	}
	if io.WriteLabel != "ZADD/ZREMRANGE commands/s" {
		t.Fatalf("writeLabel: %q", io.WriteLabel)
	}
	if !strings.Contains(io.Footnote, "sorted-set") {
		t.Fatalf("footnote: %q", io.Footnote)
	}
}

func TestRedisStorageReadRateQuery_SocialFeed(t *testing.T) {
	t.Parallel()
	q := redisStorageReadRateQuery("ns-feed", "social-feed")
	if !strings.Contains(q, `namespace="ns-feed"`) {
		t.Fatalf("namespace: %s", q)
	}
	if !strings.Contains(q, "zrevrange") {
		t.Fatalf("want zrevrange: %s", q)
	}
	if strings.Contains(q, "get|mget") {
		t.Fatalf("should not use GET pattern: %s", q)
	}
}

func TestRedisStorageWriteRateQuery_SocialFeed(t *testing.T) {
	t.Parallel()
	q := redisStorageWriteRateQuery("ns-feed", "social-feed")
	if !strings.Contains(q, "zadd|zremrangebyrank") {
		t.Fatalf("want zadd|zremrangebyrank: %s", q)
	}
}

func TestRedisStorageReadRateQuery_UrlShortener(t *testing.T) {
	t.Parallel()
	q := redisStorageReadRateQuery("ns-us", "url-shortener")
	if !strings.Contains(q, "get|mget|hget|hmget") {
		t.Fatalf("want GET pattern: %s", q)
	}
}

func TestMaxStoragePeaksFromComponents(t *testing.T) {
	t.Parallel()
	components := map[string]ComponentMetrics{
		"db": {
			StorageIO: &StorageIO{
				ReadRate:     100,
				WriteRate:    20,
				ReadLatency:  0.01,
				WriteLatency: 0.05,
				BytesUsed:    800,
				BytesLimit:   1000,
				ErrorRate:    0.5,
			},
		},
		"cache": {
			StorageIO: &StorageIO{WriteRate: 30},
		},
	}
	r, w, rl, wl, u, e := maxStoragePeaksFromComponents(components)
	if r != 100 || w != 30 || rl != 10 || wl != 50 || u != 80 || e != 0.5 {
		t.Fatalf("peaks: r=%v w=%v rl=%v wl=%v u=%v e=%v", r, w, rl, wl, u, e)
	}
}

func TestPostgreSQLUserTableReadTotal9mQuery(t *testing.T) {
	t.Parallel()
	q := postgresUserTableReadTotal9mQuery("ns1")
	for _, want := range []string{
		`namespace="ns1"`,
		"pg_stat_user_tables_seq_tup_read",
		"pg_stat_user_tables_idx_tup_fetch",
		`sum(pg_stat_user_tables_seq_tup_read`,
	} {
		if !strings.Contains(q, want) {
			t.Fatalf("read total query missing %q: %s", want, q)
		}
	}
	if strings.Contains(q, "increase(") || strings.Contains(q, "[9m]") {
		t.Fatalf("read total must not use increase window: %s", q)
	}
}

func TestPostgreSQLUserTableWriteTotal9mQuery(t *testing.T) {
	t.Parallel()
	q := postgresUserTableWriteTotal9mQuery("ns1")
	for _, want := range []string{
		"pg_stat_user_tables_n_tup_ins",
		"pg_stat_user_tables_n_tup_upd",
		"pg_stat_user_tables_n_tup_del",
	} {
		if !strings.Contains(q, want) {
			t.Fatalf("write total query missing %q: %s", want, q)
		}
	}
	if strings.Contains(q, "increase(") || strings.Contains(q, "[9m]") {
		t.Fatalf("write total must not use increase window: %s", q)
	}
}

func TestRedisStorageTotal9mQueries_SocialFeed(t *testing.T) {
	t.Parallel()
	readQ := redisStorageReadTotal9mQuery("ns-feed", "social-feed")
	writeQ := redisStorageWriteTotal9mQuery("ns-feed", "social-feed")
	if !strings.Contains(readQ, `cmd=~"zrevrange"`) || !strings.Contains(readQ, "sum by (pod)") {
		t.Fatalf("read total: %s", readQ)
	}
	if !strings.Contains(writeQ, `cmd=~"zadd|zremrangebyrank"`) || !strings.Contains(writeQ, "sum by (pod)") {
		t.Fatalf("write total: %s", writeQ)
	}
	for _, q := range []string{readQ, writeQ} {
		if strings.Contains(q, "increase(") || strings.Contains(q, "[9m]") {
			t.Fatalf("redis total must not use increase window: %s", q)
		}
	}
}

func TestRedisStorageTotal9mQueries_UrlShortener(t *testing.T) {
	t.Parallel()
	readQ := redisStorageReadTotal9mQuery("ns-us", "url-shortener")
	writeQ := redisStorageWriteTotal9mQuery("ns-us", "url-shortener")
	if !strings.Contains(readQ, "get|mget|hget|hmget") {
		t.Fatalf("read total cmd filter: %s", readQ)
	}
	if !strings.Contains(writeQ, "set|del|mset|hset|hdel") {
		t.Fatalf("write total cmd filter: %s", writeQ)
	}
	if !strings.Contains(readQ, "sum by (pod)") {
		t.Fatalf("read total must sum by pod: %s", readQ)
	}
}
