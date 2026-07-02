package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
