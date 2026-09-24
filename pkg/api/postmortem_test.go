package api

import (
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/inference"
	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/rules"
)

func TestFilterVerifiedPostmortemClaims_DropsRejected(t *testing.T) {
	t.Parallel()

	dc := deployer.DesignConfig{
		Roles: map[string][]deployer.InstanceConfig{
			"db": {{
				ID: "db1",
				Settings: map[string]any{
					"replicationMode": "async",
				},
			}},
		},
	}
	input := postmortemLedgerInput(dc)
	ledger := metrics.BuildLedger(input)
	fired := metrics.FiredRuleIDs(input.Hist, nil)

	wire := []inference.WireClaim{
		{
			Scope:     "component",
			Aspect:    "what",
			Text:      "CPU hit 99 percent.",
			FactRefs:  []string{"fact:metric:db.cpuUsage.last"},
			Grounding: "inferred",
		},
		{
			Scope:     "component",
			Aspect:    "what",
			Text:      "Database CPU rose.",
			FactRefs:  []string{"fact:metric:db.cpuUsage.last"},
			Grounding: "inferred",
		},
	}

	got := FilterVerifiedPostmortemClaims(wire, ledger, dc, fired)
	if len(got) != 1 {
		t.Fatalf("claims: %+v", got)
	}
	if got[0].Text != "Database CPU rose." {
		t.Fatalf("claim: %+v", got[0])
	}
}

func TestFilterVerifiedPostmortemClaims_PreservesRequestID(t *testing.T) {
	t.Parallel()

	dc := deployer.DesignConfig{
		Roles: map[string][]deployer.InstanceConfig{
			"db": {{ID: "db1", Settings: map[string]any{"replicationMode": "async"}}},
		},
	}
	input := postmortemLedgerInput(dc)
	ledger := metrics.BuildLedger(input)
	fired := metrics.FiredRuleIDs(input.Hist, nil)

	wire := []inference.WireClaim{{
		Scope:     "component",
		Aspect:    "what",
		Text:      "Database CPU rose.",
		FactRefs:  []string{"fact:metric:db.cpuUsage.last"},
		Grounding: "inferred",
		RequestID: "req-trace-1",
	}}

	got := FilterVerifiedPostmortemClaims(wire, ledger, dc, fired)
	if len(got) != 1 || got[0].RequestID != "req-trace-1" {
		t.Fatalf("claim: %+v", got)
	}
}

func postmortemLedgerInput(dc deployer.DesignConfig) metrics.LedgerInput {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Minute)
	hist := []metrics.DeploymentMetrics{
		{Timestamp: t0, Events: []metrics.Event{{Timestamp: t0, Type: "threshold", RuleID: rules.IDCacheHitLow}}},
		{
			Timestamp: t1,
			Components: map[string]metrics.ComponentMetrics{
				"db": {
					Component: "db",
					CPUUsage:  0.6,
					CPULimit:  1,
					CPUSource: metrics.ResourceSourceMetricsAPI,
				},
			},
			Events: []metrics.Event{{Timestamp: t1, Type: "cascade", RuleID: rules.IDCascadeCacheDB}},
		},
	}
	return metrics.LedgerInput{DesignConfig: dc, Hist: hist, Final: &hist[1]}
}
