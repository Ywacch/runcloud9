package verify

import (
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/rules"
)

func TestVerify_UnresolvableRef(t *testing.T) {
	t.Parallel()

	ledger := metrics.BuildLedger(verifierLedgerInput())
	claim := rules.Claim{
		Scope:     rules.ScopeComponent,
		Aspect:    rules.AspectWhat,
		Text:      "Cache hit rate fell.",
		FactRefs:  []string{"fact:config:missing.key"},
		Grounding: rules.GroundingInferred,
	}
	_, rej := Verify(claim, ledger, verifierDesign(), []string{rules.IDCacheHitLow})
	if len(rej) != 1 || rej[0].Reason != rules.RejectionUnresolvableRef {
		t.Fatalf("rejections: %+v", rej)
	}
}

func TestVerify_RuleNotFired(t *testing.T) {
	t.Parallel()

	ledger := metrics.BuildLedger(verifierLedgerInput())
	claim := rules.Claim{
		Scope:     rules.ScopeComponent,
		Aspect:    rules.AspectWhy,
		Text:      "Cascade hit the database.",
		FactRefs:  []string{"fact:rule:cascade_cache_db"},
		Grounding: rules.GroundingInferred,
	}
	_, rej := Verify(claim, ledger, verifierDesign(), []string{rules.IDCacheHitLow})
	if len(rej) != 1 || rej[0].Reason != rules.RejectionRuleNotFired {
		t.Fatalf("rejections: %+v", rej)
	}
}

func TestVerify_NumberMismatch(t *testing.T) {
	t.Parallel()

	ledger := metrics.BuildLedger(verifierLedgerInput())
	ref := "fact:metric:db.cpuUsage.last"
	claim := rules.Claim{
		Scope:      rules.ScopeComponent,
		Components: []string{"db"},
		Aspect:     rules.AspectWhat,
		Text:       "Database CPU peaked at 99 percent.",
		FactRefs:   []string{ref},
		Grounding:  rules.GroundingInferred,
	}
	_, rej := Verify(claim, ledger, verifierDesign(), []string{rules.IDCacheHitLow})
	if len(rej) != 1 || rej[0].Reason != rules.RejectionNumberMismatch {
		t.Fatalf("rejections: %+v", rej)
	}
}

func TestVerify_CausationLagDowngradesWhy(t *testing.T) {
	t.Parallel()

	ledger := metrics.BuildLedger(verifierLedgerInput())
	gap := 5.0
	claim := rules.Claim{
		Scope:              rules.ScopeComponent,
		Aspect:             rules.AspectWhy,
		Text:               "CPU rose after the cache miss.",
		FactRefs:           []string{"fact:metric:db.cpuUsage.last"},
		Grounding:          rules.GroundingInferred,
		ObservedGapSeconds: &gap,
	}
	got, rej := Verify(claim, ledger, verifierDesign(), []string{rules.IDCacheHitLow})
	if len(rej) != 0 {
		t.Fatalf("rejections: %+v", rej)
	}
	if got.Aspect != rules.AspectCorrelation {
		t.Fatalf("aspect: %q", got.Aspect)
	}
}

func TestVerify_PairInsufficientScope(t *testing.T) {
	t.Parallel()

	ledger := metrics.BuildLedger(verifierLedgerInput())
	claim := rules.Claim{
		Scope:     rules.ScopePair,
		Aspect:    rules.AspectWhy,
		Text:      "Cache and database diverged.",
		FactRefs:  []string{"fact:metric:db.cpuUsage.last"},
		Grounding: rules.GroundingInferred,
	}
	_, rej := Verify(claim, ledger, verifierDesign(), []string{rules.IDCacheHitLow})
	if len(rej) != 1 || rej[0].Reason != rules.RejectionInsufficientScope {
		t.Fatalf("rejections: %+v", rej)
	}
}

func TestVerify_AuthoredMechanismPreconditionFails(t *testing.T) {
	t.Parallel()

	dc := deployer.DesignConfig{
		Roles: map[string][]deployer.InstanceConfig{
			"db": {{
				ID:       "db1",
				Settings: map[string]any{"replicationMode": "semi-sync"},
			}},
		},
	}
	input := verifierLedgerInput()
	input.DesignConfig = dc
	input.Hist[1].Events = []metrics.Event{{
		Timestamp: input.Hist[1].Timestamp,
		Type:      "invariant",
		RuleID:    rules.IDFailoverWriteLoss,
		Title:     "Failover write loss",
	}}
	ledger := metrics.BuildLedger(input)

	claim := rules.Claim{
		Scope:       rules.ScopeComponent,
		Aspect:      rules.AspectWhy,
		Text:        "Async replication lost commits during promotion.",
		FactRefs:    []string{"fact:rule:failover_write_loss"},
		Grounding:   rules.GroundingAuthored,
		MechanismID: "async_promote_write_loss",
	}
	_, rej := Verify(claim, ledger, dc, []string{rules.IDFailoverWriteLoss})
	if len(rej) != 1 || rej[0].Reason != rules.RejectionMechanismPrecondition {
		t.Fatalf("rejections: %+v", rej)
	}
}

func TestVerify_InferredPreconditionContradictsConfig(t *testing.T) {
	t.Parallel()

	dc := verifierDesign()
	ledger := metrics.BuildLedger(verifierLedgerInput())
	claim := rules.Claim{
		Scope:     rules.ScopeComponent,
		Aspect:    rules.AspectWhy,
		Text:      "Async replication exposed the failure mode.",
		FactRefs:  []string{"fact:config:db1.replicationMode"},
		Grounding: rules.GroundingInferred,
		ProposedMechanism: &rules.ProposedMechanism{
			Trigger: "failover",
			Precondition: rules.Precondition{
				Role:  "db",
				Key:   "replicationMode",
				AnyOf: []string{"sync"},
			},
		},
	}
	_, rej := Verify(claim, ledger, dc, []string{rules.IDFailoverWriteLoss})
	if len(rej) != 1 || rej[0].Reason != rules.RejectionInferredPrecondition {
		t.Fatalf("rejections: %+v", rej)
	}
}

func TestVerify_AuthoredAcceptsValidClaim(t *testing.T) {
	t.Parallel()

	dc := deployer.DesignConfig{
		Roles: map[string][]deployer.InstanceConfig{
			"cache": {{
				ID:       "cache1",
				Settings: map[string]any{"cacheStrategy": "cache-aside"},
			}},
		},
	}
	input := verifierLedgerInput()
	input.DesignConfig = dc
	ledger := metrics.BuildLedger(input)

	claim := rules.Claim{
		Scope:      rules.ScopePair,
		Components: []string{"cache", "db"},
		Aspect:     rules.AspectWhy,
		Text:       "Cache misses drove database load.",
		FactRefs: []string{
			"fact:rule:cascade_cache_db",
			"fact:config:cache1.cacheStrategy",
			"fact:metric:db.cpuUsage.last",
		},
		Grounding:   rules.GroundingAuthored,
		MechanismID: "cascade_cache_db_fallthrough",
	}
	got, rej := Verify(claim, ledger, dc, []string{rules.IDCascadeCacheDB, rules.IDCacheHitLow})
	if len(rej) != 0 {
		t.Fatalf("rejections: %+v", rej)
	}
	if got.MechanismID != "cascade_cache_db_fallthrough" {
		t.Fatalf("claim: %+v", got)
	}
}

func verifierDesign() deployer.DesignConfig {
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

func verifierLedgerInput() metrics.LedgerInput {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Minute)

	hist := []metrics.DeploymentMetrics{
		{
			Timestamp: t0,
			Components: map[string]metrics.ComponentMetrics{
				"db": {Component: "db", CPUUsage: 0.3, CPULimit: 1, CPUSource: metrics.ResourceSourceMetricsAPI},
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
				"cache": {Component: "cache", CacheHitsLast9m: 10, CacheMissesLast9m: 90},
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
				Title:     "Cache miss storm hit the database",
			}},
		},
	}

	return metrics.LedgerInput{
		DesignConfig: verifierDesign(),
		Hist:         hist,
		Final:        &hist[1],
	}
}
