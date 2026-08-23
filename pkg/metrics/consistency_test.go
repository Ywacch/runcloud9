package metrics

import (
	"strings"
	"testing"

	"github.com/ywacch/cloud9/pkg/probes"
	"github.com/ywacch/cloud9/pkg/templates"
)

type stubConsistencyProbeReader struct {
	result probes.ProbeResult
	ok     bool
}

func (s *stubConsistencyProbeReader) Latest(string) (probes.ProbeResult, bool) {
	return s.result, s.ok
}

func TestConsistencyDetector_RYWViolation(t *testing.T) {
	t.Parallel()

	reader := &stubConsistencyProbeReader{
		ok: true,
		result: probes.ProbeResult{
			WrittenVersion:       5,
			ReplicaVersion:       3,
			RYWStalenessVersions: 2,
		},
	}
	cd := NewConsistencyDetector(reader)
	ns := "ns-ryw"
	topo := TopologyFlags{HasDB: true}
	dm := &DeploymentMetrics{Components: map[string]ComponentMetrics{"db": {Component: "db"}}}

	ev := cd.Eval(ns, dm, topo, nil)
	if len(ev) != 1 {
		t.Fatalf("want 1 event, got %d", len(ev))
	}
	if ev[0].RuleID != ruleRYWViolation || ev[0].Type != "invariant" {
		t.Fatalf("event: %+v", ev[0])
	}
	if ev[0].Detail["stalenessVersions"] != "2" {
		t.Fatalf("detail: %+v", ev[0].Detail)
	}
	if dm.Components["db"].Consistency == nil || dm.Components["db"].Consistency.RYWViolations != 1 {
		t.Fatalf("consistency: %+v", dm.Components["db"].Consistency)
	}
	if dm.Components["db"].Consistency.ReplicaStalenessVersions != 2 {
		t.Fatalf("staleness: %+v", dm.Components["db"].Consistency)
	}

	if again := cd.Eval(ns, dm, topo, nil); len(again) != 0 {
		t.Fatalf("want suppression, got %d events", len(again))
	}

	reader.result.RYWStalenessVersions = 0
	if clear := cd.Eval(ns, dm, topo, nil); len(clear) != 0 {
		t.Fatalf("recovery tick should not fire: %+v", clear)
	}
	if dm.Components["db"].Consistency.ReplicaStalenessVersions != 0 {
		t.Fatalf("expected zero staleness after recovery, got %+v", dm.Components["db"].Consistency)
	}

	reader.result.RYWStalenessVersions = 1
	if refire := cd.Eval(ns, dm, topo, nil); len(refire) != 1 {
		t.Fatalf("want refire after recovery, got %d", len(refire))
	}
	if dm.Components["db"].Consistency.RYWViolations != 2 {
		t.Fatalf("counter: %+v", dm.Components["db"].Consistency)
	}
}

func TestConsistencyDetector_MonotonicViolation(t *testing.T) {
	t.Parallel()

	reader := &stubConsistencyProbeReader{
		ok: true,
		result: probes.ProbeResult{
			WrittenVersion:     8,
			ReplicaVersion:     4,
			MonotonicViolation: true,
		},
	}
	cd := NewConsistencyDetector(reader)
	ns := "ns-mono"
	topo := TopologyFlags{HasDB: true}
	dm := &DeploymentMetrics{Components: map[string]ComponentMetrics{"db": {Component: "db"}}}

	ev := cd.Eval(ns, dm, topo, &templates.TemplateSpec{})
	if len(ev) != 1 {
		t.Fatalf("want 1 event, got %d", len(ev))
	}
	if ev[0].RuleID != ruleMonotonicViolation {
		t.Fatalf("event: %+v", ev[0])
	}
	if dm.Components["db"].Consistency.MonotonicViolations != 1 {
		t.Fatalf("counter: %+v", dm.Components["db"].Consistency)
	}
}

func TestConsistencyDetector_NoProbeResult(t *testing.T) {
	t.Parallel()

	cd := NewConsistencyDetector(&stubConsistencyProbeReader{ok: false})
	ns := "ns-empty"
	dm := &DeploymentMetrics{Components: map[string]ComponentMetrics{"db": {Component: "db"}}}

	if ev := cd.Eval(ns, dm, TopologyFlags{HasDB: true}, nil); len(ev) != 0 {
		t.Fatalf("want no events without probe result, got %d", len(ev))
	}
	if dm.Components["db"].Consistency == nil {
		t.Fatal("expected consistency block with zero counters")
	}
	if dm.Components["db"].Consistency.RYWViolations != 0 {
		t.Fatalf("consistency: %+v", dm.Components["db"].Consistency)
	}
}

func TestConsistencyDetector_Cleanup(t *testing.T) {
	t.Parallel()

	reader := &stubConsistencyProbeReader{
		ok:     true,
		result: probes.ProbeResult{RYWStalenessVersions: 1},
	}
	cd := NewConsistencyDetector(reader)
	ns := "ns-cleanup"
	topo := TopologyFlags{HasDB: true}
	dm := &DeploymentMetrics{Components: map[string]ComponentMetrics{"db": {Component: "db"}}}

	if ev := cd.Eval(ns, dm, topo, nil); len(ev) != 1 {
		t.Fatalf("initial fire: %d", len(ev))
	}
	cd.Cleanup(ns)
	if ev := cd.Eval(ns, dm, topo, nil); len(ev) != 1 {
		t.Fatalf("want refire after cleanup, got %d", len(ev))
	}
	if dm.Components["db"].Consistency.RYWViolations != 1 {
		t.Fatalf("counter after cleanup refire: %+v", dm.Components["db"].Consistency)
	}
}

func TestConsistencyDetector_SkipsWithoutDB(t *testing.T) {
	t.Parallel()

	cd := NewConsistencyDetector(&stubConsistencyProbeReader{ok: true, result: probes.ProbeResult{RYWStalenessVersions: 1}})
	dm := &DeploymentMetrics{Components: map[string]ComponentMetrics{"db": {Component: "db"}}}
	if ev := cd.Eval("ns", dm, TopologyFlags{}, nil); len(ev) != 0 {
		t.Fatalf("want no events without db topology, got %d", len(ev))
	}
}

func TestConsistencyDetector_CacheDbStale(t *testing.T) {
	t.Parallel()

	reader := &stubConsistencyProbeReader{
		ok: true,
		result: probes.ProbeResult{
			WrittenVersion: 10,
			CacheHit:       true,
			CacheDbStale:   true,
		},
	}
	cd := NewConsistencyDetector(reader)
	ns := "ns-cache-stale"
	topo := TopologyFlags{HasDB: true, HasCache: true}
	dm := &DeploymentMetrics{Components: map[string]ComponentMetrics{"db": {Component: "db"}}}

	ev := cd.Eval(ns, dm, topo, nil)
	if len(ev) != 0 {
		t.Fatalf("probe CacheDbStale must not emit student events, got %d", len(ev))
	}
	if dm.Components["db"].Consistency == nil || dm.Components["db"].Consistency.CacheDbStale != 0 {
		t.Fatalf("consistency: %+v", dm.Components["db"].Consistency)
	}

	if again := cd.Eval(ns, dm, topo, nil); len(again) != 0 {
		t.Fatalf("want no events on repeat eval, got %d", len(again))
	}
	if dm.Components["db"].Consistency.CacheDbStale != 0 {
		t.Fatalf("counter must stay 0: %+v", dm.Components["db"].Consistency)
	}
}

func TestPostgresUserTableMetricFilter_ExcludesPlatformTables(t *testing.T) {
	t.Parallel()

	filter := postgresUserTableMetricFilter("ns1")
	for _, want := range []string{
		`namespace="ns1"`,
		`datname!~"template0|template1"`,
		`relname!~"_c9_.*"`,
	} {
		if !strings.Contains(filter, want) {
			t.Fatalf("filter missing %q: %s", want, filter)
		}
	}
}
