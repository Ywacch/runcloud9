package metrics

import (
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/rules"
)

func TestBuildLedger_RefsAreUniqueAndStable(t *testing.T) {
	t.Parallel()

	input := ledgerTestInput()
	first := BuildLedger(input)
	second := BuildLedger(input)

	if first == nil || second == nil {
		t.Fatal("expected non-nil ledger")
	}
	facts1 := first.Facts()
	facts2 := second.Facts()
	if len(facts1) == 0 {
		t.Fatal("expected facts")
	}
	if len(facts1) != len(facts2) {
		t.Fatalf("fact count mismatch: %d vs %d", len(facts1), len(facts2))
	}

	seen := make(map[FactRef]bool)
	for i, f := range facts1 {
		if f.Ref == "" {
			t.Fatalf("empty ref at %d", i)
		}
		if seen[f.Ref] {
			t.Fatalf("duplicate ref %q", f.Ref)
		}
		seen[f.Ref] = true
		if facts2[i].Ref != f.Ref {
			t.Fatalf("ref order changed: %q vs %q", f.Ref, facts2[i].Ref)
		}
	}
}

func TestLedger_ResolveUnknownRefFails(t *testing.T) {
	t.Parallel()

	l := BuildLedger(ledgerTestInput())
	if l == nil {
		t.Fatal("expected ledger")
	}
	if _, ok := l.Resolve(FactRef("fact:config:missing.key")); ok {
		t.Fatal("unknown ref should not resolve")
	}
	if _, ok := l.Resolve(FactRef("")); ok {
		t.Fatal("empty ref should not resolve")
	}
}

func TestBuildLedger_AttachesFreshnessToMetricFacts(t *testing.T) {
	t.Parallel()

	l := BuildLedger(ledgerTestInput())
	ref := FactRef("fact:metric:db.cpuUsage.last")
	fact, ok := l.Resolve(ref)
	if !ok {
		t.Fatalf("missing %q", ref)
	}
	if fact.Kind != FactKindMetric {
		t.Fatalf("kind: %q", fact.Kind)
	}
	if fact.Freshness == nil {
		t.Fatal("expected freshness on metric fact")
	}
	if fact.Freshness.Tier != FreshnessMetricsAPI {
		t.Fatalf("freshness tier: %+v", fact.Freshness)
	}
}

func ledgerTestInput() LedgerInput {
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

	hist := []DeploymentMetrics{
		{
			Timestamp: t0,
			Components: map[string]ComponentMetrics{
				"app-server": {Component: "app-server", RequestRate: 50, P99Latency: 0.05},
				"db":         {Component: "db", CPUUsage: 0.3, CPULimit: 1, CPUSource: ResourceSourceMetricsAPI},
			},
			Events: []Event{{
				Timestamp: t0,
				Type:      "threshold",
				RuleID:    rules.IDCacheHitLow,
				Title:     "Low cache hit rate",
			}},
		},
		{
			Timestamp: t1,
			Components: map[string]ComponentMetrics{
				"app-server": {Component: "app-server", RequestRate: 120, P99Latency: 0.12, ErrorRate: 0.5},
				"cache":      {Component: "cache", CacheHitsLast9m: 10, CacheMissesLast9m: 90},
				"db": {
					Component: "db",
					CPUUsage:  0.6,
					CPULimit:  1,
					CPUSource: ResourceSourceMetricsAPI,
					Consistency: &ConsistencyStatus{
						RYWViolations: 1,
					},
				},
			},
			Events: []Event{{
				Timestamp:  t1,
				Type:       "invariant",
				RuleID:     rules.IDRYWViolation,
				Visibility: VisibilityStudent,
				Title:      "Read-your-writes violation",
			}},
		},
	}

	return LedgerInput{
		DesignConfig: dc,
		Hist:         hist,
		Final:        &hist[1],
	}
}
