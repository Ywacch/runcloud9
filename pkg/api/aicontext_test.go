package api

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/rules"
	"github.com/ywacch/cloud9/pkg/templates"
)

func TestBuildAIContext_GoldenFile(t *testing.T) {
	t.Parallel()

	ctx := BuildAIContext(aiContextTestInput())
	got, err := json.MarshalIndent(ctx, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, '\n')

	goldenPath := filepath.Join("testdata", "aicontext_baseline.golden.json")
	if os.Getenv("UPDATE_GOLDEN") == "1" {
		if err := os.MkdirAll(filepath.Dir(goldenPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(goldenPath, got, 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("updated golden file %s", goldenPath)
		return
	}

	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read golden %s: %v (set UPDATE_GOLDEN=1 to create)", goldenPath, err)
	}

	var wantVal, gotVal any
	if err := json.Unmarshal(want, &wantVal); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	if err := json.Unmarshal(got, &gotVal); err != nil {
		t.Fatalf("unmarshal got: %v", err)
	}
	if !reflect.DeepEqual(wantVal, gotVal) {
		t.Fatalf("ai-context JSON differs from %s (set UPDATE_GOLDEN=1 to refresh)", goldenPath)
	}
}

func TestBuildAIContext_BaselineRunOnly(t *testing.T) {
	t.Parallel()

	ctx := BuildAIContext(aiContextTestInput())
	if ctx.SchemaVersion != AIContextSchemaVersion {
		t.Fatalf("schemaVersion: %d", ctx.SchemaVersion)
	}
	if ctx.CatalogVersion != rules.CatalogVersion {
		t.Fatalf("catalogVersion: %q", ctx.CatalogVersion)
	}
	if ctx.SessionID != "sess-golden-1" {
		t.Fatalf("sessionId: %q", ctx.SessionID)
	}
	if len(ctx.Runs) != 1 {
		t.Fatalf("runs: want 1, got %d", len(ctx.Runs))
	}
	run := ctx.Runs[0]
	if run.Role != RunRoleBaseline {
		t.Fatalf("role: %q", run.Role)
	}
	if len(run.Facts) == 0 {
		t.Fatal("expected facts")
	}
	if len(run.Descriptors) == 0 {
		t.Fatal("expected descriptors")
	}
	if len(run.Violations) != 1 || run.Violations[0].ID != rules.IDRYWViolation {
		t.Fatalf("violations: %+v", run.Violations)
	}
	if len(run.Mechanisms) == 0 {
		t.Fatal("expected mechanism priors")
	}
	if len(run.Events) == 0 {
		t.Fatal("expected events summary")
	}
}

func aiContextTestInput() AIContextInput {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Minute)

	dc := deployer.DesignConfig{
		Template: "social-feed",
		Roles: map[string][]deployer.InstanceConfig{
			"db": {{
				ID:   "db1",
				Type: "db",
				Settings: map[string]any{
					"readConsistency": "replica",
					"replicationMode": "async",
					"readReplicas":    2,
				},
			}},
		},
	}

	hist := []metrics.DeploymentMetrics{
		{
			Timestamp: t0,
			Components: map[string]metrics.ComponentMetrics{
				"app-server": {Component: "app-server", RequestRate: 50, P99Latency: 0.05},
				"db":         {Component: "db", CPUUsage: 0.3, CPULimit: 1, CPUSource: metrics.ResourceSourceMetricsAPI},
			},
			Events: []metrics.Event{{
				Timestamp: t0,
				Type:      "threshold",
				RuleID:    rules.IDCacheHitLow,
				Title:     "Low cache hit rate",
			}},
		},
		{
			Timestamp: t1,
			Components: map[string]metrics.ComponentMetrics{
				"app-server": {Component: "app-server", RequestRate: 120, P99Latency: 0.12, ErrorRate: 0.5},
				"cache":      {Component: "cache", CacheHitsLast9m: 10, CacheMissesLast9m: 90},
				"db": {
					Component: "db",
					CPUUsage:  0.6,
					CPULimit:  1,
					CPUSource: metrics.ResourceSourceMetricsAPI,
					Consistency: &metrics.ConsistencyStatus{
						RYWViolations: 1,
					},
				},
			},
			Events: []metrics.Event{{
				Timestamp:  t1,
				Type:       "invariant",
				RuleID:     rules.IDRYWViolation,
				Visibility: metrics.VisibilityStudent,
				Title:      "Read-your-writes violation",
			}},
		},
	}

	return AIContextInput{
		SessionID:    "sess-golden-1",
		DesignConfig: dc,
		Design:       aiContextGoldenDesign(),
		Hist:         hist,
		Final:        &hist[1],
		Template:     templates.GetTemplate("social-feed"),
	}
}

func aiContextGoldenDesign() *deployer.DeploymentDesign {
	return &deployer.DeploymentDesign{
		Template: "social-feed",
		Nodes: []deployer.DeploymentNode{
			{ID: "app1", Type: "app-server", Name: "API"},
			{ID: "db1", Type: "db", Name: "PostgreSQL", Config: map[string]any{
				"readReplicas":    float64(2),
				"readConsistency": "replica",
				"replicationMode": "async",
			}},
		},
		Connections: []deployer.DeploymentConn{
			{SourceID: "app1", TargetID: "db1", Protocol: "TCP"},
		},
	}
}
