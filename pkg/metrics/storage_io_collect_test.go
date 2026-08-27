package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCollect_StorageIO_PostgresLive(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		var val string
		switch {
		case strings.Contains(q, "pg_stat_user_tables_seq_tup_read"):
			val = "412.5"
		case strings.Contains(q, "pg_stat_user_tables_n_tup_ins"):
			val = "105"
		case strings.Contains(q, "pg_database_size_bytes"):
			val = "52428800"
		case strings.Contains(q, "xact_rollback"):
			val = "0.1"
		default:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(promVectorJSON(val))
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	topo := TopologyFlags{HasDB: true, DBEngine: "postgresql", DBRole: "primary"}
	dm, err := c.Collect(context.Background(), "ns-pg-io", "", topo)
	if err != nil {
		t.Fatal(err)
	}
	io := dm.Components["db"].StorageIO
	if io == nil {
		t.Fatal("expected storageIo on db")
	}
	if io.ReadRate != 412.5 {
		t.Fatalf("readRate: want 412.5, got %v", io.ReadRate)
	}
	if io.WriteRate != 105 {
		t.Fatalf("writeRate: want 105 (80+20+5), got %v", io.WriteRate)
	}
	if io.BytesUsed != 52428800 {
		t.Fatalf("bytesUsed: got %v", io.BytesUsed)
	}
	if io.ErrorRate != 0.1 {
		t.Fatalf("errorRate: got %v", io.ErrorRate)
	}
	if io.ReadLatency != 0 || io.WriteLatency != 0 {
		t.Fatalf("latency should not be populated yet: %+v", io)
	}
	if io.Unit != "rows/s" || io.Source != "postgres_exporter" {
		t.Fatalf("metadata: %+v", io)
	}
}

func TestCollect_StorageIOSessionTotalsUseCounterSum(t *testing.T) {
	t.Parallel()

	var redisReadQ string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "redis_commands_total") &&
			strings.Contains(q, "sum by (pod)") &&
			strings.Contains(q, "get|mget|hget|hmget") &&
			!strings.Contains(q, "rate("):
			if strings.Contains(q, "increase(") {
				t.Errorf("redis read total must not use increase(): %s", q)
			}
			redisReadQ = q
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", map[string]string{
				"redis-0": "50",
			}))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	topo := TopologyFlags{HasCache: true, CacheEngine: "redis", CacheRole: "cache-aside"}
	dm, err := c.Collect(context.Background(), "ns-redis-total", "url-shortener", topo)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(redisReadQ, "sum by (pod)") {
		t.Fatalf("want sum by (pod) counter, got %q", redisReadQ)
	}
	if strings.Contains(redisReadQ, "[9m]") {
		t.Fatalf("redis total still windowed: %s", redisReadQ)
	}
	io := dm.Components["cache"].StorageIO
	if io == nil {
		t.Fatal("expected storageIo on cache")
	}
	if io.ReadTotal9m != 0 {
		t.Fatalf("ReadTotal9m: want 0 (baseline), got %v", io.ReadTotal9m)
	}
}

func TestCollect_StorageIOSessionAccumulatorSurvivesPodKill(t *testing.T) {
	t.Parallel()

	scrape := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "redis_commands_total") &&
			strings.Contains(q, "sum by (pod)") &&
			strings.Contains(q, "get|mget|hget|hmget") &&
			!strings.Contains(q, "rate("):
			if strings.Contains(q, "increase(") {
				t.Errorf("redis read total must not use increase(): %s", q)
			}
			scrape++
			var pods map[string]string
			switch scrape {
			case 1:
				pods = map[string]string{"redis-a": "100"}
			case 2:
				pods = map[string]string{"redis-b": "0"}
			default:
				pods = map[string]string{"redis-b": "20"}
			}
			_ = json.NewEncoder(w).Encode(promMultiVectorJSON("pod", pods))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	ctx := context.Background()
	topo := TopologyFlags{HasCache: true, CacheEngine: "redis", CacheRole: "cache-aside"}

	dm1, err := c.Collect(ctx, "ns-redis-kill", "url-shortener", topo)
	if err != nil {
		t.Fatal(err)
	}
	if got := dm1.Components["cache"].StorageIO.ReadTotal9m; got != 0 {
		t.Fatalf("collect 1 ReadTotal9m: want 0 (baseline), got %v", got)
	}

	dm2, err := c.Collect(ctx, "ns-redis-kill", "url-shortener", topo)
	if err != nil {
		t.Fatal(err)
	}
	if got := dm2.Components["cache"].StorageIO.ReadTotal9m; got != 0 {
		t.Fatalf("collect 2 ReadTotal9m: want 0 (new pod baseline), got %v", got)
	}

	dm3, err := c.Collect(ctx, "ns-redis-kill", "url-shortener", topo)
	if err != nil {
		t.Fatal(err)
	}
	if got := dm3.Components["cache"].StorageIO.ReadTotal9m; got != 20 {
		t.Fatalf("collect 3 ReadTotal9m: want 20 (delta on redis-b), got %v", got)
	}
}

func TestCollect_StorageIO_PostgresSessionBaselinesSeed(t *testing.T) {
	t.Parallel()

	writeScrapes := 0
	var lastWriteTotalQ string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "pg_stat_user_tables_n_tup_ins") && !strings.Contains(q, "rate("):
			writeScrapes++
			lastWriteTotalQ = q
			val := "1000000"
			if writeScrapes >= 2 {
				val = "1000008"
			}
			_ = json.NewEncoder(w).Encode(promVectorJSON(val))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	ctx := context.Background()
	topo := TopologyFlags{HasDB: true, DBEngine: "postgresql", DBRole: "primary"}

	dm1, err := c.Collect(ctx, "ns-pg-seed", "url-shortener", topo)
	if err != nil {
		t.Fatal(err)
	}
	if got := dm1.Components["db"].StorageIO.WriteTotal9m; got != 0 {
		t.Fatalf("collect 1 WriteTotal9m: want 0 (seed baseline), got %v", got)
	}
	if !strings.Contains(lastWriteTotalQ, `pod=~"postgres.*"`) || !strings.Contains(lastWriteTotalQ, `pod!~"postgres-replica.*"`) {
		t.Fatalf("write total query must exclude replicas: %s", lastWriteTotalQ)
	}

	dm2, err := c.Collect(ctx, "ns-pg-seed", "url-shortener", topo)
	if err != nil {
		t.Fatal(err)
	}
	if got := dm2.Components["db"].StorageIO.WriteTotal9m; got != 8 {
		t.Fatalf("collect 2 WriteTotal9m: want 8 (post-seed delta), got %v", got)
	}
}

func TestCollect_StorageIO_PostgresRebaselineAfterSeed(t *testing.T) {
	t.Parallel()

	var writeTotal = "0"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "pg_stat_user_tables_n_tup_ins") && !strings.Contains(q, "rate("):
			_ = json.NewEncoder(w).Encode(promVectorJSON(writeTotal))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	ctx := context.Background()
	topo := TopologyFlags{HasDB: true, DBEngine: "postgresql", DBRole: "primary"}
	const ns = "ns-pg-rebaseline"

	dm1, err := c.Collect(ctx, ns, "url-shortener", topo)
	if err != nil {
		t.Fatal(err)
	}
	if got := dm1.Components["db"].StorageIO.WriteTotal9m; got != 0 {
		t.Fatalf("collect 1 WriteTotal9m: want 0 (pre-seed baseline), got %v", got)
	}

	writeTotal = "1000000"
	dm2, err := c.Collect(ctx, ns, "url-shortener", topo)
	if err != nil {
		t.Fatal(err)
	}
	if got := dm2.Components["db"].StorageIO.WriteTotal9m; got != 1_000_000 {
		t.Fatalf("collect 2 WriteTotal9m: want 1000000 (seed in flight), got %v", got)
	}

	c.RebaselinePostgresSession(ctx, ns)

	writeTotal = "1000008"
	dm3, err := c.Collect(ctx, ns, "url-shortener", topo)
	if err != nil {
		t.Fatal(err)
	}
	if got := dm3.Components["db"].StorageIO.WriteTotal9m; got != 8 {
		t.Fatalf("collect 3 WriteTotal9m: want 8 (post-rebaseline), got %v", got)
	}
}

func TestRebaselinePostgresSession_NilSafe(t *testing.T) {
	t.Parallel()
	var c *MetricsCollector
	c.RebaselinePostgresSession(context.Background(), "ns")
	c.HoldPostgresSession("ns")
	c.OpenPostgresSession("ns")
	c.WaitAndRebaselinePostgresSession(context.Background(), "ns")
}

func TestCollect_StorageIO_PostgresHoldUntilStable(t *testing.T) {
	oldInterval := pgSessionStableInterval
	oldTimeout := pgSessionStableTimeout
	pgSessionStableInterval = time.Millisecond
	pgSessionStableTimeout = 2 * time.Second
	t.Cleanup(func() {
		pgSessionStableInterval = oldInterval
		pgSessionStableTimeout = oldTimeout
	})

	var writeTotal = "1000000"
	var readTotal = "2000000"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "pg_stat_user_tables_n_tup_ins") && !strings.Contains(q, "rate("):
			_ = json.NewEncoder(w).Encode(promVectorJSON(writeTotal))
		case strings.Contains(q, "pg_stat_user_tables_seq_tup_read") && !strings.Contains(q, "rate("):
			_ = json.NewEncoder(w).Encode(promVectorJSON(readTotal))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	ctx := context.Background()
	topo := TopologyFlags{HasDB: true, DBEngine: "postgresql", DBRole: "primary"}
	const ns = "ns-pg-hold"

	c.HoldPostgresSession(ns)

	dm1, err := c.Collect(ctx, ns, "url-shortener", topo)
	if err != nil {
		t.Fatal(err)
	}
	if got := dm1.Components["db"].StorageIO.WriteTotal9m; got != 0 {
		t.Fatalf("held WriteTotal9m: want 0, got %v", got)
	}
	if got := dm1.Components["db"].StorageIO.ReadTotal9m; got != 0 {
		t.Fatalf("held ReadTotal9m: want 0, got %v", got)
	}

	c.WaitAndRebaselinePostgresSession(ctx, ns)

	writeTotal = "1000008"
	readTotal = "2000012"
	dm2, err := c.Collect(ctx, ns, "url-shortener", topo)
	if err != nil {
		t.Fatal(err)
	}
	if got := dm2.Components["db"].StorageIO.WriteTotal9m; got != 8 {
		t.Fatalf("after open WriteTotal9m: want 8, got %v", got)
	}
	if got := dm2.Components["db"].StorageIO.ReadTotal9m; got != 12 {
		t.Fatalf("after open ReadTotal9m: want 12, got %v", got)
	}
}
