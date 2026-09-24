//go:build ignore

// Generate labeled AIContext fixtures for runcloud9-ai eval corpus.
// Run from ganymede/: go run scripts/gen_aicontext_fixtures.go
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ywacch/cloud9/pkg/api"
	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/rules"
)

type fixtureSpec struct {
	name  string
	input api.AIContextInput
}

func main() {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Minute)

	specs := []fixtureSpec{
		{
			name: "clean",
			input: api.AIContextInput{
				SessionID:    "clean",
				DesignConfig: socialFeedDesign(),
				Hist: []metrics.DeploymentMetrics{{
					Timestamp: t1,
					Components: map[string]metrics.ComponentMetrics{
						"app-server": {Component: "app-server", RequestRate: 80, P99Latency: 0.04, ErrorRate: 0.01},
						"cache":      {Component: "cache", CacheHitsLast9m: 800, CacheMissesLast9m: 200},
						"db":         {Component: "db", CPUUsage: 0.25, CPULimit: 1, CPUSource: metrics.ResourceSourceMetricsAPI},
					},
				}},
				Final: snapshotPtr(metrics.DeploymentMetrics{
					Timestamp: t1,
					Components: map[string]metrics.ComponentMetrics{
						"app-server": {Component: "app-server", RequestRate: 80, P99Latency: 0.04, ErrorRate: 0.01},
						"cache":      {Component: "cache", CacheHitsLast9m: 800, CacheMissesLast9m: 200},
						"db":         {Component: "db", CPUUsage: 0.25, CPULimit: 1, CPUSource: metrics.ResourceSourceMetricsAPI},
					},
				}),
			},
		},
		{
			name: "pod_kill",
			input: api.AIContextInput{
				SessionID:    "pod_kill",
				DesignConfig: socialFeedDesign(),
				Hist: []metrics.DeploymentMetrics{
					{Timestamp: t0, Components: map[string]metrics.ComponentMetrics{
						"app-server": {Component: "app-server", RequestRate: 60},
					}},
					{
						Timestamp: t1,
						Components: map[string]metrics.ComponentMetrics{
							"app-server": {Component: "app-server", RequestRate: 40, ErrorRate: 0.2},
						},
						Events: []metrics.Event{{
							Timestamp: t1,
							Type:      "invariant",
							RuleID:    rules.IDPodRestart,
							Title:     "App pod restarted",
						}},
					},
				},
				Final: snapshotPtr(metrics.DeploymentMetrics{
					Timestamp: t1,
					Components: map[string]metrics.ComponentMetrics{
						"app-server": {Component: "app-server", RequestRate: 40, ErrorRate: 0.2},
					},
				}),
			},
		},
		{
			name: "cache_flush",
			input: api.AIContextInput{
				SessionID:    "cache_flush",
				DesignConfig: socialFeedDesign(),
				Hist: []metrics.DeploymentMetrics{
					{Timestamp: t0, Components: map[string]metrics.ComponentMetrics{
						"cache": {Component: "cache", CacheHitsLast9m: 500, CacheMissesLast9m: 50},
					}},
					{
						Timestamp: t1,
						Components: map[string]metrics.ComponentMetrics{
							"app-server": {Component: "app-server", RequestRate: 100, P99Latency: 0.15},
							"cache":      {Component: "cache", CacheHitsLast9m: 10, CacheMissesLast9m: 400},
							"db":         {Component: "db", CPUUsage: 0.7, CPULimit: 1, CPUSource: metrics.ResourceSourceMetricsAPI, DbQueryRate: 120},
						},
						Events: []metrics.Event{
							{Timestamp: t0, Type: "threshold", RuleID: rules.IDCacheHitLow, Title: "Low cache hit rate"},
							{Timestamp: t1, Type: "cascade", RuleID: rules.IDCascadeCacheDB, Title: "Cache miss storm hit the database"},
						},
					},
				},
				Final: snapshotPtr(metrics.DeploymentMetrics{
					Timestamp: t1,
					Components: map[string]metrics.ComponentMetrics{
						"db": {Component: "db", CPUUsage: 0.7, CPULimit: 1, CPUSource: metrics.ResourceSourceMetricsAPI, DbQueryRate: 120},
					},
				}),
			},
		},
		{
			name: "stall_replica",
			input: api.AIContextInput{
				SessionID:    "stall_replica",
				DesignConfig: asyncReplicaDesign(),
				Hist: []metrics.DeploymentMetrics{{
					Timestamp: t1,
					Components: map[string]metrics.ComponentMetrics{
						"db": {Component: "db", CPUUsage: 0.5, CPULimit: 1, CPUSource: metrics.ResourceSourceMetricsAPI},
					},
					Events: []metrics.Event{{
						Timestamp: t1,
						Type:      "invariant",
						RuleID:    rules.IDReplicaCannotCatchUp,
						Title:     "Replica cannot catch up",
					}},
				}},
				Final: snapshotPtr(metrics.DeploymentMetrics{
					Timestamp: t1,
					Components: map[string]metrics.ComponentMetrics{
						"db": {Component: "db", CPUUsage: 0.5, CPULimit: 1, CPUSource: metrics.ResourceSourceMetricsAPI},
					},
				}),
			},
		},
		{
			name: "split_primary_replica",
			input: api.AIContextInput{
				SessionID:    "split_primary_replica",
				DesignConfig: asyncReplicaDesign(),
				Hist: []metrics.DeploymentMetrics{{
					Timestamp: t1,
					Components: map[string]metrics.ComponentMetrics{
						"db": {Component: "db", CPUUsage: 0.4, CPULimit: 1, CPUSource: metrics.ResourceSourceMetricsAPI},
					},
					Events: []metrics.Event{{
						Timestamp: t1,
						Type:      "invariant",
						RuleID:    rules.IDPartitionStaleReads,
						Title:     "Partition stale reads",
					}},
				}},
				Final: snapshotPtr(metrics.DeploymentMetrics{
					Timestamp: t1,
					Components: map[string]metrics.ComponentMetrics{
						"db": {Component: "db", CPUUsage: 0.4, CPULimit: 1, CPUSource: metrics.ResourceSourceMetricsAPI},
					},
				}),
			},
		},
		{
			name: "split_app_store",
			input: api.AIContextInput{
				SessionID:    "split_app_store",
				DesignConfig: writeThroughCacheDesign(),
				Hist: []metrics.DeploymentMetrics{{
					Timestamp: t1,
					Components: map[string]metrics.ComponentMetrics{
						"cache": {Component: "cache", CacheHitsLast9m: 200, CacheMissesLast9m: 20},
						"db":    {Component: "db", CPUUsage: 0.35, CPULimit: 1, CPUSource: metrics.ResourceSourceMetricsAPI},
					},
					Events: []metrics.Event{{
						Timestamp: t1,
						Type:      "invariant",
						RuleID:    rules.IDCacheDBStale,
						Title:     "Cache served stale value",
					}},
				}},
				Final: snapshotPtr(metrics.DeploymentMetrics{
					Timestamp: t1,
					Components: map[string]metrics.ComponentMetrics{
						"cache": {Component: "cache", CacheHitsLast9m: 200, CacheMissesLast9m: 20},
						"db":    {Component: "db", CPUUsage: 0.35, CPULimit: 1, CPUSource: metrics.ResourceSourceMetricsAPI},
					},
				}),
			},
		},
		{
			name: "promote_replica",
			input: api.AIContextInput{
				SessionID:    "promote_replica",
				DesignConfig: asyncReplicaDesign(),
				Hist: []metrics.DeploymentMetrics{{
					Timestamp: t1,
					Components: map[string]metrics.ComponentMetrics{
						"db": {Component: "db", CPUUsage: 0.45, CPULimit: 1, CPUSource: metrics.ResourceSourceMetricsAPI},
					},
					Events: []metrics.Event{{
						Timestamp: t1,
						Type:      "invariant",
						RuleID:    rules.IDFailoverWriteLoss,
						Title:     "Failover write loss",
						Detail:    map[string]string{"bytesLost": "4096"},
					}},
				}},
				Final: snapshotPtr(metrics.DeploymentMetrics{
					Timestamp: t1,
					Components: map[string]metrics.ComponentMetrics{
						"db": {Component: "db", CPUUsage: 0.45, CPULimit: 1, CPUSource: metrics.ResourceSourceMetricsAPI},
					},
				}),
			},
		},
		{
			name: "promote_lagging_replica",
			input: api.AIContextInput{
				SessionID:    "promote_lagging_replica",
				DesignConfig: asyncReplicaDesign(),
				Hist: []metrics.DeploymentMetrics{{
					Timestamp: t1,
					Components: map[string]metrics.ComponentMetrics{
						"db": {Component: "db", CPUUsage: 0.55, CPULimit: 1, CPUSource: metrics.ResourceSourceMetricsAPI},
					},
					Events: []metrics.Event{{
						Timestamp: t1,
						Type:      "invariant",
						RuleID:    rules.IDFailoverWriteLoss,
						Title:     "Failover write loss on lagging replica",
						Detail:    map[string]string{"bytesLost": "8192", "scenario": "promote_lagging_replica"},
					}},
				}},
				Final: snapshotPtr(metrics.DeploymentMetrics{
					Timestamp: t1,
					Components: map[string]metrics.ComponentMetrics{
						"db": {Component: "db", CPUUsage: 0.55, CPULimit: 1, CPUSource: metrics.ResourceSourceMetricsAPI},
					},
				}),
			},
		},
		{
			name: "kill_replica",
			input: api.AIContextInput{
				SessionID:    "kill_replica",
				DesignConfig: asyncReplicaDesign(),
				Hist: []metrics.DeploymentMetrics{{
					Timestamp: t1,
					Components: map[string]metrics.ComponentMetrics{
						"db": {Component: "db", CPUUsage: 0.48, CPULimit: 1, CPUSource: metrics.ResourceSourceMetricsAPI},
					},
					Events: []metrics.Event{{
						Timestamp: t1,
						Type:      "invariant",
						RuleID:    rules.IDReplicaCannotCatchUp,
						Title:     "Replica cannot catch up after kill",
					}},
				}},
				Final: snapshotPtr(metrics.DeploymentMetrics{
					Timestamp: t1,
					Components: map[string]metrics.ComponentMetrics{
						"db": {Component: "db", CPUUsage: 0.48, CPULimit: 1, CPUSource: metrics.ResourceSourceMetricsAPI},
					},
				}),
			},
		},
	}

	outDir := fixturesOutDir()
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		panic(err)
	}
	for _, spec := range specs {
		ctx := api.BuildAIContext(spec.input)
		body, err := json.MarshalIndent(ctx, "", "  ")
		if err != nil {
			panic(err)
		}
		body = append(body, '\n')
		path := filepath.Join(outDir, spec.name+".json")
		if err := os.WriteFile(path, body, 0o644); err != nil {
			panic(err)
		}
		fmt.Println("wrote", path)
	}
}

func snapshotPtr(dm metrics.DeploymentMetrics) *metrics.DeploymentMetrics {
	return &dm
}

// fixturesOutDir is the sibling Python repo (runcloud9-ai on GitHub).
func fixturesOutDir() string {
	for _, name := range []string{"runcloud9-ai", "ganymede-ai"} {
		dir := filepath.Join("..", name, "fixtures")
		if _, err := os.Stat(filepath.Join("..", name)); err == nil {
			return dir
		}
	}
	return filepath.Join("..", "runcloud9-ai", "fixtures")
}

func socialFeedDesign() deployer.DesignConfig {
	return deployer.DesignConfig{
		Template: "social-feed",
		Roles: map[string][]deployer.InstanceConfig{
			"cache": {{
				ID:   "cache1",
				Type: "cache",
				Settings: map[string]any{
					"cacheStrategy": "cache-aside",
				},
			}},
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
}

func asyncReplicaDesign() deployer.DesignConfig {
	return deployer.DesignConfig{
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
}

func writeThroughCacheDesign() deployer.DesignConfig {
	dc := socialFeedDesign()
	dc.Roles["cache"] = []deployer.InstanceConfig{{
		ID:   "cache1",
		Type: "cache",
		Settings: map[string]any{
			"cacheStrategy": "write-through",
		},
	}}
	return dc
}
