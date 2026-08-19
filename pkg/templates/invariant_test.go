package templates

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestValidateInvariantRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		rules   []InvariantRule
		wantErr string
	}{
		{
			name: "valid",
			rules: []InvariantRule{{
				ID: "oversell", Kind: "oversell", Metric: "ticket.oversell", Op: ">", Threshold: 0,
			}},
		},
		{
			name:    "missing id",
			rules:   []InvariantRule{{Metric: "m", Op: ">"}},
			wantErr: "missing id",
		},
		{
			name:    "missing metric",
			rules:   []InvariantRule{{ID: "x", Op: ">"}},
			wantErr: "metric is required",
		},
		{
			name:    "bad op",
			rules:   []InvariantRule{{ID: "x", Metric: "m", Op: "<"}},
			wantErr: "unsupported op",
		},
		{
			name: "duplicate id",
			rules: []InvariantRule{
				{ID: "dup", Metric: "a", Op: ">"},
				{ID: "dup", Metric: "b", Op: ">"},
			},
			wantErr: "duplicate id",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := validateInvariantRules(tt.rules)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("want error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestDefToSpec_InvariantCheckersYAML(t *testing.T) {
	t.Parallel()
	const doc = `
id: ticket-drop-test
name: Ticket Drop Test
available: false
deployable: false
simulatable: false
imageKey: ticket-drop
invariantCheckers:
  - id: oversell
    kind: oversell
    metric: ticket.oversell
    op: ">"
    threshold: 0
    title: Oversell detected
    narrative: Sold exceeded capacity
`
	var def templateDef
	if err := yaml.Unmarshal([]byte(doc), &def); err != nil {
		t.Fatalf("yaml: %v", err)
	}
	spec := defToSpec(&def)
	if len(spec.InvariantCheckers) != 1 {
		t.Fatalf("want 1 rule, got %d", len(spec.InvariantCheckers))
	}
	r := spec.InvariantCheckers[0]
	if r.ID != "oversell" || r.Metric != "ticket.oversell" || r.Op != ">" {
		t.Fatalf("rule: %+v", r)
	}
}

func TestDefToSpec_InvalidInvariantCheckersPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for invalid invariant rules")
		}
	}()
	def := &templateDef{
		ID: "bad-template",
		InvariantCheckers: []InvariantRule{{
			ID: "", Metric: "m", Op: ">",
		}},
	}
	_ = defToSpec(def)
}
