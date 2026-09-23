package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/rules"
	"github.com/ywacch/cloud9/pkg/store"
)

func TestHandleAIContext_LiveSession(t *testing.T) {
	t.Parallel()

	ns := "ns-live-ai"
	st := &fakeDeploymentStore{
		byNS: map[string]store.DeploymentRow{
			ns: {
				Namespace: ns,
				UserID:    "dev-user",
				Template:  "social-feed",
				Status:    "running",
				Design: map[string]any{
					"template": "social-feed",
					"nodes": []any{
						map[string]any{
							"id":   "db1",
							"type": "db",
							"config": map[string]any{
								"readConsistency": "replica",
								"replicationMode": "async",
								"readReplicas":    2,
							},
						},
					},
				},
			},
		},
	}
	collector := metrics.NewMetricsCollector("", nil, nil, nil, nil)
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	collector.AppendSessionSnapshots(ns, metrics.DeploymentMetrics{
		Timestamp: t0,
		Components: map[string]metrics.ComponentMetrics{
			"db": {Component: "db", CPUUsage: 0.5, CPULimit: 1, CPUSource: metrics.ResourceSourceMetricsAPI},
		},
		Events: []metrics.Event{{
			Timestamp: t0,
			Type:      "invariant",
			RuleID:    rules.IDRYWViolation,
			Title:     "Read-your-writes violation",
		}},
	})

	h := NewHandler(nil, st, nil, nil, collector, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/deployments/"+ns+"/ai-context", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp AIContext
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.SessionID != ns {
		t.Fatalf("sessionId: %q", resp.SessionID)
	}
	if resp.SchemaVersion != AIContextSchemaVersion {
		t.Fatalf("schemaVersion: %d", resp.SchemaVersion)
	}
	if len(resp.Runs) != 1 || resp.Runs[0].Role != RunRoleBaseline {
		t.Fatalf("runs: %+v", resp.Runs)
	}
	if len(resp.Runs[0].Facts) == 0 {
		t.Fatal("expected facts from live ring buffer")
	}
	if len(resp.Runs[0].Violations) != 1 {
		t.Fatalf("violations: %+v", resp.Runs[0].Violations)
	}
}

func TestHandleAIContext_TerminatedSessionFromStore(t *testing.T) {
	t.Parallel()

	ns := "ns-terminated-ai"
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	hist := []metrics.DeploymentMetrics{{
		Timestamp: t0,
		Components: map[string]metrics.ComponentMetrics{
			"db": {Component: "db", CPUUsage: 0.4, CPULimit: 1, CPUSource: metrics.ResourceSourceMetricsAPI},
		},
		Events: []metrics.Event{{
			Timestamp: t0,
			Type:      "threshold",
			RuleID:    rules.IDCacheHitLow,
			Title:     "Low cache hit rate",
		}},
	}}
	st := &fakeDeploymentStore{
		byNS: map[string]store.DeploymentRow{
			ns: {
				Namespace: ns,
				UserID:    "dev-user",
				Template:  "social-feed",
				Status:    "terminated",
				Design: map[string]any{
					"template": "social-feed",
					"nodes": []any{
						map[string]any{
							"id":   "db1",
							"type": "db",
							"config": map[string]any{
								"readConsistency": "replica",
								"replicationMode": "async",
							},
						},
					},
				},
				MetricsSummary: map[string]any{
					"session_history": hist,
					"terminated_at":   t0.Add(time.Minute).Format(time.RFC3339),
				},
			},
		},
	}

	h := NewHandler(nil, st, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/deployments/"+ns+"/ai-context", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp AIContext
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Runs) != 1 {
		t.Fatalf("runs: %+v", resp.Runs)
	}
	if len(resp.Runs[0].Events) == 0 {
		t.Fatal("expected events from persisted session_history")
	}
	if len(resp.Runs[0].Facts) == 0 {
		t.Fatal("expected facts from persisted history")
	}
}

func TestHandleAIContext_RejectsOtherUser(t *testing.T) {
	t.Parallel()

	ns := "ns-other-user"
	st := &fakeDeploymentStore{
		byNS: map[string]store.DeploymentRow{
			ns: {
				Namespace: ns,
				UserID:    "owner-user",
				Template:  "social-feed",
				Status:    "running",
			},
		},
	}
	h := NewHandler(nil, st, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/deployments/"+ns+"/ai-context", nil)
	req = req.WithContext(context.WithValue(req.Context(), userIDKey, "intruder-user"))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status: want 403, got %d body=%s", rec.Code, rec.Body.String())
	}
}
