package api

import (
	"context"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/inference"
	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/store"
)

func TestEnrichTeardownSummary_NoOpForNonInternalOwner(t *testing.T) {
	t.Parallel()
	stub := &stubInferenceService{}
	st := &fakeDeploymentStore{
		users: map[string]store.UserRecord{
			"eng-user": {},
		},
	}
	h := NewHandler(nil, st, nil, nil, nil, nil, nil, nil, nil)
	h.inferenceClient = stub

	summary := map[string]any{
		"events_summary": []map[string]any{{"rule_id": "cpu_high"}},
	}
	hist := []metrics.DeploymentMetrics{{Timestamp: time.Now().UTC()}}
	row := store.DeploymentRow{UserID: "eng-user", Template: "url-shortener"}

	h.EnrichTeardownSummary(context.Background(), "ns-1", row, summary, hist, &hist[0])
	if stub.reelCalls.Load() != 0 {
		t.Fatalf("reelCalls: got %d want 0", stub.reelCalls.Load())
	}
}

func TestEnrichTeardownSummary_NoOpWithoutHistory(t *testing.T) {
	t.Parallel()
	stub := &stubInferenceService{}
	st := &fakeDeploymentStore{
		users: map[string]store.UserRecord{
			"dev-user": {IsInternal: true},
		},
	}
	h := NewHandler(nil, st, nil, nil, nil, nil, nil, nil, nil)
	h.inferenceClient = stub

	summary := map[string]any{
		"events_summary": []map[string]any{{"rule_id": "cpu_high"}},
	}
	row := store.DeploymentRow{UserID: "dev-user", Template: "url-shortener"}

	h.EnrichTeardownSummary(context.Background(), "ns-2", row, summary, nil, nil)
	if stub.reelCalls.Load() != 0 {
		t.Fatalf("reelCalls: got %d want 0", stub.reelCalls.Load())
	}
}

func TestEnrichTeardownSummary_InternalOwnerCallsReel(t *testing.T) {
	t.Parallel()
	stub := &stubInferenceService{
		reelResp: &inference.ReelCaptionsResponse{
			Usage: inference.TokenUsage{TotalTokens: 1},
		},
	}
	st := &fakeDeploymentStore{
		users: map[string]store.UserRecord{
			"dev-user": {IsInternal: true},
		},
	}
	collector := metrics.NewMetricsCollector("", nil, nil, nil, nil)
	ns := "ns-enrich-reel"
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	collector.AppendSessionSnapshots(ns, metrics.DeploymentMetrics{
		Timestamp: t0,
		Components: map[string]metrics.ComponentMetrics{
			"db": {Component: "db", CPUUsage: 0.5, CPULimit: 1, CPUSource: metrics.ResourceSourceMetricsAPI},
		},
	})
	hist := collector.DownsampledSessionHistory(ns)
	latest := hist[len(hist)-1]

	h := NewHandler(nil, st, nil, nil, collector, nil, nil, nil, nil)
	h.inferenceClient = stub

	summary := map[string]any{
		"events_summary": []map[string]any{{"rule_id": "cpu_high", "caption": "CPU rose"}},
	}
	row := store.DeploymentRow{
		Namespace: ns,
		UserID:    "dev-user",
		Template:  "url-shortener",
		Design: map[string]any{
			"template": "url-shortener",
			"nodes": []any{
				map[string]any{
					"id":   "db1",
					"type": "db",
					"config": map[string]any{
						"readConsistency": "replica",
					},
				},
			},
		},
	}

	h.EnrichTeardownSummary(context.Background(), ns, row, summary, hist, &latest)
	if stub.reelCalls.Load() != 1 {
		t.Fatalf("reelCalls: got %d want 1", stub.reelCalls.Load())
	}
}
