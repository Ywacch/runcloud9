package metrics

import (
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/templates"
)

func TestEventVisibility_KnownValuesOnly(t *testing.T) {
	t.Parallel()

	for _, bad := range []string{"user", "ai"} {
		if ValidVisibility(bad) {
			t.Fatalf("%q must not be a valid visibility", bad)
		}
	}
	for _, good := range []string{"", VisibilityStudent, VisibilityAll, VisibilityOperator, "both"} {
		if !ValidVisibility(good) {
			t.Fatalf("%q should be valid", good)
		}
	}

	for _, vis := range emitterVisibilityLiterals() {
		if !ValidVisibility(vis) {
			t.Fatalf("emitter literal %q is not valid", vis)
		}
	}

	for _, ev := range sampleEmitterEvents(t) {
		if !ValidVisibility(ev.Visibility) {
			t.Fatalf("invalid visibility %q on event type=%s rule=%s", ev.Visibility, ev.Type, ev.RuleID)
		}
	}
}

func TestNormalizeVisibility(t *testing.T) {
	t.Parallel()

	if NormalizeVisibility("") != VisibilityAll {
		t.Fatalf("empty: %q", NormalizeVisibility(""))
	}
	if NormalizeVisibility("both") != VisibilityAll {
		t.Fatalf("legacy both: %q", NormalizeVisibility("both"))
	}
	if NormalizeVisibility(VisibilityStudent) != VisibilityStudent {
		t.Fatalf("student: %q", NormalizeVisibility(VisibilityStudent))
	}
}

// emitterVisibilityLiterals lists every non-empty Visibility value set in
// pkg/metrics production emitters.
func emitterVisibilityLiterals() []string {
	return []string{VisibilityStudent}
}

func sampleEmitterEvents(t *testing.T) []Event {
	now := time.Now()
	var out []Event

	if ev := FailoverWriteLossEvent(now, "promote_replica", "postgres-replica-0", "async", 512, "async", ""); ev != nil {
		out = append(out, *ev)
	}

	cd := NewConsistencyDetector(&stubConsistencyProbeReader{ok: false})
	ns := "visibility-test"
	topo := TopologyFlags{HasDB: true}
	dm := &DeploymentMetrics{Components: map[string]ComponentMetrics{"db": {Component: "db"}}}
	cd.NoteAppTrafficConsistency(ns, appTrafficConsistency{RYWTotal: 1, ReplicaReads: 1})
	out = append(out, cd.Eval(ns, dm, topo, nil)...)

	td := NewThresholdDetector()
	cacheDM := &DeploymentMetrics{Components: map[string]ComponentMetrics{
		"cache": {CacheHitsLast9m: 1, CacheMissesLast9m: 9},
	}}
	out = append(out, td.Eval(ns, cacheDM, TopologyFlags{}, nil)...)

	cascade := NewCascadeDetector()
	cascadeTopo := TopologyFlags{HasCache: true, HasDB: true}
	out = append(out, cascade.Eval(ns, &DeploymentMetrics{Components: map[string]ComponentMetrics{
		"cache": {CacheHitsLast9m: 100, CacheMissesLast9m: 0},
		"db":    {DbQueryRate: 10},
	}}, cascadeTopo, nil)...)
	out = append(out, cascade.Eval(ns, &DeploymentMetrics{Components: map[string]ComponentMetrics{
		"cache": {CacheHitsLast9m: 10, CacheMissesLast9m: 90},
		"db":    {DbQueryRate: 50},
	}}, cascadeTopo, nil)...)

	ic := NewInvariantChecker()
	tmpl := &templates.TemplateSpec{InvariantCheckers: []templates.InvariantRule{{
		ID: "oversell", Kind: "oversell", Metric: "ticket.oversell", Op: ">", Threshold: 0,
	}}}
	invDM := &DeploymentMetrics{TemplateMetrics: map[string]float64{"ticket.oversell": 1}}
	out = append(out, ic.Eval(ns, invDM, TopologyFlags{}, tmpl)...)

	if len(out) == 0 {
		t.Fatal("expected sample emitter events")
	}
	return out
}
