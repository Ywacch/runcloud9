//go:build ignore

// One-off helper to refresh pkg/api/testdata/aicontext_baseline.golden.json.
// Run from ganymede/: go run scripts/gen_aicontext_golden.go
package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/ywacch/cloud9/pkg/api"
	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/rules"
)

func main() {
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

	ctx := api.BuildAIContext(api.AIContextInput{
		SessionID:    "sess-golden-1",
		DesignConfig: dc,
		Hist:         hist,
		Final:        &hist[1],
	})

	out, err := json.MarshalIndent(ctx, "", "  ")
	if err != nil {
		panic(err)
	}
	out = append(out, '\n')

	path := filepath.Join("pkg", "api", "testdata", "aicontext_baseline.golden.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		panic(err)
	}
	if err := os.WriteFile(path, out, 0o644); err != nil {
		panic(err)
	}
}
