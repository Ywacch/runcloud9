package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const collectParityNS = "ns-collect-parity"

// collectParitySnapshot holds stable app/db fields for golden and sample-vs-direct parity.
type collectParitySnapshot struct {
	AppRequestRate float64 `json:"appRequestRate"`
	AppErrorRate   float64 `json:"appErrorRate"`
	DbQueryRate    float64 `json:"dbQueryRate"`
}

func snapshotFromCollect(dm *DeploymentMetrics) collectParitySnapshot {
	var out collectParitySnapshot
	if dm == nil {
		return out
	}
	if app, ok := dm.Components["app-server"]; ok {
		out.AppRequestRate = app.RequestRate
		out.AppErrorRate = app.ErrorRate
	}
	if db, ok := dm.Components["db"]; ok {
		out.DbQueryRate = db.DbQueryRate
	}
	return out
}

func clusterParityPromHandler(w http.ResponseWriter, q string) {
	w.Header().Set("Content-Type", "application/json")

	if strings.Contains(q, "sum by (namespace, le)") && strings.Contains(q, "http_request_duration_seconds_bucket") {
		_ = json.NewEncoder(w).Encode(promNSBucketJSON(collectParityNS, map[string]string{
			"0.05": "10",
			"0.1":  "50",
			"0.25": "100",
			"+Inf": "100",
		}))
		return
	}
	if strings.Contains(q, "sum by (namespace)") {
		switch {
		case strings.Contains(q, "http_requests_total") && strings.Contains(q, `status=~"5.."`):
			_ = json.NewEncoder(w).Encode(promNSValueJSON(map[string]string{collectParityNS: "0.5"}))
		case strings.Contains(q, "http_requests_total"):
			_ = json.NewEncoder(w).Encode(promNSValueJSON(map[string]string{collectParityNS: "15"}))
		case strings.Contains(q, "db_query_duration_seconds_count"):
			_ = json.NewEncoder(w).Encode(promNSValueJSON(map[string]string{collectParityNS: "8"}))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
		return
	}
	if !strings.Contains(q, collectParityNS) {
		_ = json.NewEncoder(w).Encode(promEmptyJSON())
		return
	}
	switch {
	case strings.Contains(q, "rate(http_requests_total") && strings.Contains(q, `status=~"5.."`):
		_ = json.NewEncoder(w).Encode(promVectorJSON("0.5"))
		return
	case strings.Contains(q, "rate(http_requests_total"):
		_ = json.NewEncoder(w).Encode(promVectorJSON("15"))
		return
	case strings.Contains(q, "rate(db_query_duration_seconds_count"):
		_ = json.NewEncoder(w).Encode(promVectorJSON("8"))
		return
	}
	_ = json.NewEncoder(w).Encode(promEmptyJSON())
}

func TestCollect_ClusterSampleMatchesDirectCollect(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clusterParityPromHandler(w, r.URL.Query().Get("query"))
	}))
	t.Cleanup(srv.Close)

	topo := TopologyFlags{HasDB: true}
	ctx := context.Background()

	direct := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	direct.SetClusterSampleEnabled(false)
	dmDirect, err := direct.Collect(ctx, collectParityNS, "url-shortener", topo)
	if err != nil {
		t.Fatal(err)
	}

	viaSample := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	viaSample.SetClusterSampleEnabled(true)
	dmSample, err := viaSample.Collect(ctx, collectParityNS, "url-shortener", topo)
	if err != nil {
		t.Fatal(err)
	}

	want := snapshotFromCollect(dmDirect)
	got := snapshotFromCollect(dmSample)
	if got != want {
		t.Fatalf("cluster sample vs direct:\n direct: %+v\n sample: %+v", want, got)
	}
	if want.AppRequestRate != 15 {
		t.Fatalf("app request rate: want 15, got %v", want.AppRequestRate)
	}
	if want.DbQueryRate != 8 {
		t.Fatalf("db query rate: want 8, got %v", want.DbQueryRate)
	}
}

func TestCollect_ClusterSampleParityGolden(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clusterParityPromHandler(w, r.URL.Query().Get("query"))
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	c.SetClusterSampleEnabled(true)
	dm, err := c.Collect(context.Background(), collectParityNS, "url-shortener", TopologyFlags{HasDB: true})
	if err != nil {
		t.Fatal(err)
	}
	got := snapshotFromCollect(dm)
	gotBytes, err := json.MarshalIndent(got, "", "  ")
	if err != nil {
		t.Fatal(err)
	}

	goldenPath := filepath.Join("testdata", "collect_cluster_sample_parity.golden.json")
	if os.Getenv("UPDATE_GOLDEN") != "" {
		if err := os.MkdirAll(filepath.Dir(goldenPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(goldenPath, gotBytes, 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("updated golden %s", goldenPath)
		return
	}

	wantBytes, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read golden %s: %v (set UPDATE_GOLDEN=1)", goldenPath, err)
	}
	var want collectParitySnapshot
	if err := json.Unmarshal(wantBytes, &want); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("parity snapshot differs from %s:\n got: %+v\nwant: %+v", goldenPath, got, want)
	}
}
