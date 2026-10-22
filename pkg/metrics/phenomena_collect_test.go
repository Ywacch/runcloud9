package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCollect_ReplyBeforeParentPhenomenon(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "social_thread_reads_total"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("100"))
		case strings.Contains(q, "social_thread_orphan_reads_total"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("5"))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	topo := TopologyFlags{
		HasDB:                 true,
		DBReadReplicas:        2,
		DBReplicaApplyDelayMs: []int{0, 1500},
		DBReadConsistency:     "replica",
	}
	dm, err := c.Collect(context.Background(), "ns-phenom", "social-feed", topo)
	if err != nil {
		t.Fatal(err)
	}
	if len(dm.Phenomena) != 1 {
		t.Fatalf("phenomena: %+v", dm.Phenomena)
	}
	if dm.Phenomena[0].Phenomenon != "reply_before_parent" {
		t.Fatalf("phenomenon id: %q", dm.Phenomena[0].Phenomenon)
	}
	if dm.Phenomena[0].BaseRate.Reads != 100 {
		t.Fatalf("base reads: %+v", dm.Phenomena[0].BaseRate)
	}
}

func TestCollect_ReplyBeforeParentMissingCountersNoEntry(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(promEmptyJSON())
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	topo := TopologyFlags{HasDB: true, DBReadReplicas: 2}
	dm, err := c.Collect(context.Background(), "ns-no-phenom", "social-feed", topo)
	if err != nil {
		t.Fatal(err)
	}
	if len(dm.Phenomena) != 0 {
		t.Fatalf("expected no phenomena, got %+v", dm.Phenomena)
	}
}
