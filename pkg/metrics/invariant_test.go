package metrics

import (
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/templates"
)

func TestInvariantChecker_Eval(t *testing.T) {
	t.Parallel()

	tmpl := &templates.TemplateSpec{
		InvariantCheckers: []templates.InvariantRule{{
			ID:        "oversell",
			Kind:      "oversell",
			Metric:    "ticket.oversell",
			Op:        ">",
			Threshold: 0,
			Title:     "Tickets oversold",
			Narrative: "Sold exceeded capacity",
		}},
	}
	ic := NewInvariantChecker()
	ns := "ns-invariant-test"
	dm := &DeploymentMetrics{
		TemplateMetrics: map[string]float64{"ticket.oversell": 1},
	}

	ev := ic.Eval(ns, dm, TopologyFlags{}, tmpl)
	if len(ev) != 1 {
		t.Fatalf("want 1 event, got %d", len(ev))
	}
	if ev[0].Type != "invariant" || ev[0].RuleID != "oversell" {
		t.Fatalf("event: %+v", ev[0])
	}
	if ev[0].Detail["kind"] != "oversell" || ev[0].Detail["value"] != "1.0000" {
		t.Fatalf("detail: %+v", ev[0].Detail)
	}

	// Suppress while condition remains true.
	if again := ic.Eval(ns, dm, TopologyFlags{}, tmpl); len(again) != 0 {
		t.Fatalf("want suppression, got %d events", len(again))
	}

	// Clear latch when metric recovers.
	dm.TemplateMetrics["ticket.oversell"] = 0
	if clear := ic.Eval(ns, dm, TopologyFlags{}, tmpl); len(clear) != 0 {
		t.Fatalf("recovery tick should not fire: %+v", clear)
	}
	dm.TemplateMetrics["ticket.oversell"] = 2
	if refire := ic.Eval(ns, dm, TopologyFlags{}, tmpl); len(refire) != 1 {
		t.Fatalf("want refire after recovery, got %d", len(refire))
	}
}

func TestInvariantChecker_Ops(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		op        string
		threshold float64
		value     float64
		wantFire  bool
	}{
		{name: "gt true", op: ">", threshold: 0, value: 0.5, wantFire: true},
		{name: "gt false", op: ">", threshold: 1, value: 0.5, wantFire: false},
		{name: "gte equal", op: ">=", threshold: 1, value: 1, wantFire: true},
		{name: "neq", op: "!=", threshold: 0, value: 0.01, wantFire: true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ic := NewInvariantChecker()
			ns := "ns-op-" + tt.name
			tmpl := &templates.TemplateSpec{
				InvariantCheckers: []templates.InvariantRule{{
					ID:        "rule",
					Metric:    "m",
					Op:        tt.op,
					Threshold: tt.threshold,
				}},
			}
			dm := &DeploymentMetrics{TemplateMetrics: map[string]float64{"m": tt.value}}
			ev := ic.Eval(ns, dm, TopologyFlags{}, tmpl)
			if tt.wantFire && len(ev) != 1 {
				t.Fatalf("want fire, got %d", len(ev))
			}
			if !tt.wantFire && len(ev) != 0 {
				t.Fatalf("want no fire, got %+v", ev)
			}
		})
	}
}

func TestInvariantChecker_Cleanup(t *testing.T) {
	t.Parallel()
	ic := NewInvariantChecker()
	ns := "ns-cleanup"
	tmpl := &templates.TemplateSpec{
		InvariantCheckers: []templates.InvariantRule{{
			ID: "oversell", Metric: "ticket.oversell", Op: ">", Threshold: 0,
		}},
	}
	dm := &DeploymentMetrics{TemplateMetrics: map[string]float64{"ticket.oversell": 1}}
	if ev := ic.Eval(ns, dm, TopologyFlags{}, tmpl); len(ev) != 1 {
		t.Fatalf("initial fire: %d", len(ev))
	}
	ic.Cleanup(ns)
	if ev := ic.Eval(ns, dm, TopologyFlags{}, tmpl); len(ev) != 1 {
		t.Fatalf("want refire after cleanup, got %d", len(ev))
	}
	_ = time.Now()
}
