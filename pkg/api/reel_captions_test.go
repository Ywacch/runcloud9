package api

import (
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/inference"
	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/rules"
)

func TestReelCaptions_RejectedCaptionFallsBackToAuthored(t *testing.T) {
	t.Parallel()

	dc := deployer.DesignConfig{
		Roles: map[string][]deployer.InstanceConfig{
			"db": {{
				ID:   "db1",
				Type: "db",
				Settings: map[string]any{
					"replicationMode": "async",
				},
			}},
		},
	}
	input := reelTestLedgerInput(dc)
	ledger := metrics.BuildLedger(input)
	fired := metrics.FiredRuleIDs(input.Hist, nil)

	highlights := []map[string]any{{
		"ruleId":    rules.IDCascadeCacheDB,
		"narrative": "Cache misses drove database load.",
		"type":      "cascade",
	}}

	resp := &inference.ReelCaptionsResponse{
		Throughline: "Stormy session",
		Captions: []inference.ReelCaptionItem{{
			RuleID: rules.IDCascadeCacheDB,
			Claim: rules.Claim{
				Scope:     rules.ScopeComponent,
				Aspect:    rules.AspectWhy,
				Text:      "Invented number 99 percent load.",
				FactRefs:  []string{"fact:metric:db.cpuUsage.last"},
				Grounding: rules.GroundingInferred,
			},
		}},
	}

	got, throughline := ApplyReelCaptions(highlights, resp, ledger, dc, fired)
	if throughline != "Stormy session" {
		t.Fatalf("throughline: %q", throughline)
	}
	if _, ok := got[0]["caption"]; ok {
		t.Fatalf("expected no caption after rejection, got %+v", got[0])
	}
	if got[0]["narrative"] != "Cache misses drove database load." {
		t.Fatalf("narrative: %+v", got[0])
	}
}

func TestReelCaptions_AcceptsVerifiedCaption(t *testing.T) {
	t.Parallel()

	dc := deployer.DesignConfig{
		Roles: map[string][]deployer.InstanceConfig{
			"db": {{
				ID:   "db1",
				Type: "db",
				Settings: map[string]any{
					"replicationMode": "async",
				},
			}},
		},
	}
	input := reelTestLedgerInput(dc)
	ledger := metrics.BuildLedger(input)
	fired := metrics.FiredRuleIDs(input.Hist, nil)

	highlights := []map[string]any{{
		"ruleId":    rules.IDCascadeCacheDB,
		"narrative": "Authored fallback",
	}}

	resp := &inference.ReelCaptionsResponse{
		Captions: []inference.ReelCaptionItem{{
			RuleID: rules.IDCascadeCacheDB,
			Claim: rules.Claim{
				Scope:      rules.ScopeComponent,
				Components: []string{"db"},
				Aspect:     rules.AspectWhat,
				Text:       "Database CPU rose.",
				FactRefs:   []string{"fact:metric:db.cpuUsage.last"},
				Grounding:  rules.GroundingInferred,
			},
		}},
	}

	got, _ := ApplyReelCaptions(highlights, resp, ledger, dc, fired)
	cap, ok := got[0]["caption"].(ReelCaptionView)
	if !ok {
		t.Fatalf("caption type: %T", got[0]["caption"])
	}
	if cap.Text != "Database CPU rose." || cap.Grounding != rules.GroundingInferred {
		t.Fatalf("caption: %+v", cap)
	}
}

func reelTestLedgerInput(dc deployer.DesignConfig) metrics.LedgerInput {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Minute)
	hist := []metrics.DeploymentMetrics{
		{
			Timestamp: t0,
			Events: []metrics.Event{{
				Timestamp: t0,
				Type:      "threshold",
				RuleID:    rules.IDCacheHitLow,
			}},
		},
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
			Events: []metrics.Event{{
				Timestamp: t1,
				Type:      "cascade",
				RuleID:    rules.IDCascadeCacheDB,
			}},
		},
	}
	return metrics.LedgerInput{
		DesignConfig: dc,
		Hist:         hist,
		Final:        &hist[1],
	}
}
