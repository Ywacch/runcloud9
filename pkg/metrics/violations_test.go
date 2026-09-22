package metrics

import (
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/rules"
	"github.com/ywacch/cloud9/pkg/templates"
)

func TestBuildViolations_JoinsCausedByConfig(t *testing.T) {
	t.Parallel()

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
	hist := []DeploymentMetrics{{
		Components: map[string]ComponentMetrics{
			"db": {
				Consistency: &ConsistencyStatus{RYWViolations: 3},
			},
		},
		Events: []Event{{
			Timestamp: time.Now(),
			Type:      "invariant",
			RuleID:    rules.IDRYWViolation,
			Detail: map[string]string{
				"kind":  "ryw",
				"delta": "1",
			},
		}},
	}}

	got := BuildViolations(hist, dc, nil)
	if len(got) != 1 {
		t.Fatalf("want 1 violation, got %d: %+v", len(got), got)
	}
	v := got[0]
	if v.ID != rules.IDRYWViolation || v.Count != 3 {
		t.Fatalf("rollup: %+v", v)
	}
	if v.Detail["kind"] != "ryw" || v.Detail["value"] != "3" {
		t.Fatalf("detail: %+v", v.Detail)
	}
	if len(v.CausedBy) != 3 {
		t.Fatalf("causedBy: %+v", v.CausedBy)
	}
	wantKeys := map[string]any{
		"readConsistency": "replica",
		"replicationMode": "async",
		"readReplicas":    2,
	}
	for _, cause := range v.CausedBy {
		if cause.Role != "db" || cause.Instance != "db1" {
			t.Fatalf("cause role/instance: %+v", cause)
		}
		want, ok := wantKeys[cause.Key]
		if !ok {
			t.Fatalf("unexpected causedBy key %q", cause.Key)
		}
		if cause.Value != want {
			t.Fatalf("cause %s value: got %v want %v", cause.Key, cause.Value, want)
		}
	}
}

func TestBuildViolations_SkipsRulesThatDidNotFire(t *testing.T) {
	t.Parallel()

	dc := deployer.DesignConfig{
		Template: "ticket-drop",
		Roles: map[string][]deployer.InstanceConfig{
			"db": {{
				ID: "db1",
				Settings: map[string]any{
					"readConsistency": "replica",
					"replicationMode": "async",
				},
			}},
		},
	}
	tmpl := &templates.TemplateSpec{
		InvariantCheckers: []templates.InvariantRule{{
			ID:   "oversell",
			Kind: "oversell",
		}},
	}
	hist := []DeploymentMetrics{{
		Events: []Event{{
			Timestamp: time.Now(),
			Type:      "invariant",
			RuleID:    "oversell",
			Detail: map[string]string{
				"kind":  "oversell",
				"value": "2.0000",
			},
		}},
	}}

	got := BuildViolations(hist, dc, tmpl)
	if len(got) != 1 {
		t.Fatalf("want 1 violation, got %d: %+v", len(got), got)
	}
	if got[0].ID != "oversell" {
		t.Fatalf("id: %+v", got[0])
	}
	for _, skip := range []string{rules.IDRYWViolation, rules.IDCacheHitLow, rules.IDPartitionStaleReads} {
		for _, v := range got {
			if v.ID == skip {
				t.Fatalf("rule %q should not appear: %+v", skip, got)
			}
		}
	}
}
