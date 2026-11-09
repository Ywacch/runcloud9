package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ywacch/cloud9/pkg/templateapp/jobs/timeline"
)

func TestCollect_TimelineOpsRedis(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, timeline.MetricAppendTotal) && strings.Contains(q, `engine="redis"`) && strings.Contains(q, "rate("):
			_ = json.NewEncoder(w).Encode(promVectorJSON("2.5"))
		case strings.Contains(q, timeline.MetricAppendDuration+"_bucket") && strings.Contains(q, "histogram_quantile(0.99"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.04"))
		case strings.Contains(q, timeline.MetricRangeDuration+"_bucket") && strings.Contains(q, "histogram_quantile(0.99"):
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		case strings.Contains(q, timeline.MetricOpErrorsTotal):
			_ = json.NewEncoder(w).Encode(promVectorJSON("0.1"))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	dm, err := c.Collect(context.Background(), "ns-timeline", "social-feed", TopologyFlags{HasDB: true, HasCache: true})
	if err != nil {
		t.Fatal(err)
	}
	if dm.Timeline == nil {
		t.Fatal("expected timeline ops")
	}
	if dm.Timeline.Engine != "redis" {
		t.Fatalf("engine: got %q want redis", dm.Timeline.Engine)
	}
	if dm.Timeline.AppendRate == nil || *dm.Timeline.AppendRate != 2.5 {
		t.Fatalf("appendRate: %+v", dm.Timeline.AppendRate)
	}
	if dm.Timeline.AppendP99Ms == nil || *dm.Timeline.AppendP99Ms != 40 {
		t.Fatalf("appendP99Ms: %+v", dm.Timeline.AppendP99Ms)
	}
}

func TestCollect_TimelineOpsMissingSeriesNilTimeline(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(promEmptyJSON())
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	dm, err := c.Collect(context.Background(), "ns-empty", "social-feed", TopologyFlags{})
	if err != nil {
		t.Fatal(err)
	}
	if dm.Timeline != nil {
		t.Fatalf("expected nil timeline, got %+v", dm.Timeline)
	}
}

func TestCollect_PostgresTimelineTableStats(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(q, "c9_timeline_relation_bytes"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("4096"))
		case strings.Contains(q, "c9_timeline_dead_tuples"):
			_ = json.NewEncoder(w).Encode(promVectorJSON("12"))
		default:
			_ = json.NewEncoder(w).Encode(promEmptyJSON())
		}
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	dm, err := c.Collect(context.Background(), "ns-pg", "social-feed", TopologyFlags{HasDB: true})
	if err != nil {
		t.Fatal(err)
	}
	db := dm.Components["db"]
	if db.TimelineRelationBytes == nil || *db.TimelineRelationBytes != 4096 {
		t.Fatalf("relation bytes: %+v", db.TimelineRelationBytes)
	}
	if db.TimelineDeadTuples == nil || *db.TimelineDeadTuples != 12 {
		t.Fatalf("dead tuples: %+v", db.TimelineDeadTuples)
	}
}

func TestDetectTimelineEngine_prefersPostgresqlRate(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(q, `engine="postgresql"`) && strings.Contains(q, timeline.MetricAppendTotal) {
			_ = json.NewEncoder(w).Encode(promVectorJSON("1"))
			return
		}
		_ = json.NewEncoder(w).Encode(promEmptyJSON())
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	engine := c.detectTimelineEngine(context.Background(), "ns", `namespace="ns"`)
	if engine != "postgresql" {
		t.Fatalf("engine: got %q want postgresql", engine)
	}
}
