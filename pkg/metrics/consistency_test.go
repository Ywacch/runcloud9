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

func TestConsistencyDetector_ReplicaStaleReadsOnStatus(t *testing.T) {
	t.Parallel()

	cd := NewConsistencyDetector(&stubConsistencyProbeReader{ok: false})
	ns := "ns-stale"
	topo := TopologyFlags{HasDB: true}
	dm := &DeploymentMetrics{Components: map[string]ComponentMetrics{"db": {Component: "db"}}}

	cd.NoteAppTrafficConsistency(ns, appTrafficConsistency{
		ReplicaStaleTotal: 3,
		ReplicaReads:      10,
		LeaderReads:       2,
	})
	cd.Eval(ns, dm, topo, nil)

	cons := dm.Components["db"].Consistency
	if cons == nil || cons.ReplicaStaleReads != 3 {
		t.Fatalf("replica stale reads: %+v", cons)
	}
	if cons.RYWViolations != 0 {
		t.Fatalf("stale reads must not emit RYW violations: %+v", cons)
	}
}

func TestConsistencyDetector_RYWFromTraffic(t *testing.T) {
	t.Parallel()

	// Probe reports staleness, but student RYW must follow app counters only.
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

	cd.NoteAppTrafficConsistency(ns, appTrafficConsistency{RYWTotal: 1, ReplicaReads: 4, LeaderReads: 2})
	ev := cd.Eval(ns, dm, topo, nil)
	if len(ev) != 1 {
		t.Fatalf("want 1 event, got %d", len(ev))
	}
	if ev[0].RuleID != ruleRYWViolation || ev[0].Type != "invariant" {
		t.Fatalf("event: %+v", ev[0])
	}
	if ev[0].Detail["source"] != "traffic" {
		t.Fatalf("detail: %+v", ev[0].Detail)
	}
	cons := dm.Components["db"].Consistency
	if cons == nil || cons.RYWViolations != 1 {
		t.Fatalf("consistency: %+v", cons)
	}
	if cons.ReplicaStalenessVersions != 2 {
		t.Fatalf("probe lag should still surface: %+v", cons)
	}
	if cons.LeaderReads != 2 || cons.ReplicaReads != 4 {
		t.Fatalf("route split: %+v", cons)
	}
	if cons.GuaranteeHeldBy != guaranteeHeldByNone {
		t.Fatalf("guarantee without redirects: %+v", cons)
	}

	cd.NoteAppTrafficConsistency(ns, appTrafficConsistency{RYWTotal: 1, ReplicaReads: 4, LeaderReads: 2})
	if again := cd.Eval(ns, dm, topo, nil); len(again) != 0 {
		t.Fatalf("want suppression, got %d events", len(again))
	}

	cd.NoteAppTrafficConsistency(ns, appTrafficConsistency{RYWTotal: 1, ReplicaReads: 5, LeaderReads: 2})
	if clear := cd.Eval(ns, dm, topo, nil); len(clear) != 0 {
		t.Fatalf("stable total should clear latch without firing: %+v", clear)
	}

	cd.NoteAppTrafficConsistency(ns, appTrafficConsistency{
		RYWTotal:        2,
		ReplicaReads:    5,
		LeaderReads:     3,
		LeaderRedirects: 1,
	})
	if refire := cd.Eval(ns, dm, topo, nil); len(refire) != 1 {
		t.Fatalf("want refire after delta, got %d", len(refire))
	}
	cons = dm.Components["db"].Consistency
	if cons.RYWViolations != 2 {
		t.Fatalf("counter: %+v", cons)
	}
	if cons.GuaranteeHeldBy != guaranteeHeldByApp {
		t.Fatalf("want app guarantee after redirects, got %+v", cons)
	}
}

func TestConsistencyDetector_GuaranteeHeldByDB_remoteApply(t *testing.T) {
	t.Parallel()

	cd := NewConsistencyDetector(&stubConsistencyProbeReader{ok: false})
	ns := "ns-db-ryw"
	topo := TopologyFlags{
		HasDB:            true,
		DBReadReplicas:   2,
		ReplicationMode:  "semi-sync",
		SyncCommitLevel:  "remote_apply",
		SyncStandbyNames: "ANY 1 (*)",
	}
	dm := &DeploymentMetrics{Components: map[string]ComponentMetrics{"db": {Component: "db"}}}

	cd.NoteAppTrafficConsistency(ns, appTrafficConsistency{
		ReplicaReads:    10,
		LeaderReads:     3,
		LeaderRedirects: 3,
	})
	cd.NotePostgresSyncActive(ns, true)
	cd.Eval(ns, dm, topo, nil)

	cons := dm.Components["db"].Consistency
	if cons == nil || cons.GuaranteeHeldBy != guaranteeHeldByDB {
		t.Fatalf("remote_apply should attribute guarantee to db, got %+v", cons)
	}
}

func TestConsistencyDetector_ProbeDoesNotEmitStudentRYW(t *testing.T) {
	t.Parallel()

	reader := &stubConsistencyProbeReader{
		ok: true,
		result: probes.ProbeResult{
			RYWStalenessVersions: 9,
			MonotonicViolation:   true,
		},
	}
	cd := NewConsistencyDetector(reader)
	ns := "ns-probe-only"
	topo := TopologyFlags{HasDB: true}
	dm := &DeploymentMetrics{Components: map[string]ComponentMetrics{"db": {Component: "db"}}}

	ev := cd.Eval(ns, dm, topo, nil)
	if len(ev) != 0 {
		t.Fatalf("probe must not emit student RYW/monotonic, got %d", len(ev))
	}
	cons := dm.Components["db"].Consistency
	if cons == nil || cons.RYWViolations != 0 || cons.MonotonicViolations != 0 {
		t.Fatalf("student counters must stay 0: %+v", cons)
	}
	if cons.ReplicaStalenessVersions != 9 {
		t.Fatalf("canary lag missing: %+v", cons)
	}
}

func TestConsistencyDetector_MonotonicFromTraffic(t *testing.T) {
	t.Parallel()

	cd := NewConsistencyDetector(&stubConsistencyProbeReader{ok: false})
	ns := "ns-mono"
	topo := TopologyFlags{HasDB: true}
	dm := &DeploymentMetrics{Components: map[string]ComponentMetrics{"db": {Component: "db"}}}

	cd.NoteAppTrafficConsistency(ns, appTrafficConsistency{MonoTotal: 1, ReplicaReads: 2})
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
		t.Fatalf("want no events without traffic, got %d", len(ev))
	}
	if dm.Components["db"].Consistency == nil {
		t.Fatal("expected consistency block with zero counters")
	}
	if dm.Components["db"].Consistency.RYWViolations != 0 {
		t.Fatalf("consistency: %+v", dm.Components["db"].Consistency)
	}
	if dm.Components["db"].Consistency.GuaranteeHeldBy != guaranteeHeldByNone {
		t.Fatalf("guarantee: %+v", dm.Components["db"].Consistency)
	}
}

func TestConsistencyDetector_Cleanup(t *testing.T) {
	t.Parallel()

	cd := NewConsistencyDetector(&stubConsistencyProbeReader{ok: false})
	ns := "ns-cleanup"
	topo := TopologyFlags{HasDB: true}
	dm := &DeploymentMetrics{Components: map[string]ComponentMetrics{"db": {Component: "db"}}}

	cd.NoteAppTrafficConsistency(ns, appTrafficConsistency{RYWTotal: 1})
	if ev := cd.Eval(ns, dm, topo, nil); len(ev) != 1 {
		t.Fatalf("initial fire: %d", len(ev))
	}
	cd.Cleanup(ns)
	cd.NoteAppTrafficConsistency(ns, appTrafficConsistency{RYWTotal: 1})
	if ev := cd.Eval(ns, dm, topo, nil); len(ev) != 1 {
		t.Fatalf("want refire after cleanup, got %d", len(ev))
	}
	if dm.Components["db"].Consistency.RYWViolations != 1 {
		t.Fatalf("counter after cleanup refire: %+v", dm.Components["db"].Consistency)
	}
}

func TestConsistencyDetector_SkipsWithoutDB(t *testing.T) {
	t.Parallel()

	cd := NewConsistencyDetector(&stubConsistencyProbeReader{ok: false})
	cd.NoteAppTrafficConsistency("ns", appTrafficConsistency{RYWTotal: 1})
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

func TestConsistencyDetector_AppCacheStale(t *testing.T) {
	t.Parallel()

	cd := NewConsistencyDetector(&stubConsistencyProbeReader{ok: false})
	ns := "ns-app-stale"
	topo := TopologyFlags{HasDB: true, HasCache: true}
	dm := &DeploymentMetrics{Components: map[string]ComponentMetrics{"db": {Component: "db"}}}
	sample := cacheStaleSample{
		Key:       "product:42",
		Served:    "3",
		Truth:     "1",
		RequestID: "req-abc",
	}

	cd.NoteAppCacheStale(ns, 1, sample)
	ev := cd.Eval(ns, dm, topo, nil)
	if len(ev) != 1 {
		t.Fatalf("want 1 event on 0→1, got %d", len(ev))
	}
	if ev[0].RuleID != ruleCacheDbStale {
		t.Fatalf("event: %+v", ev[0])
	}
	for _, key := range []string{"key", "served", "truth", "requestId"} {
		if ev[0].Detail[key] == "" {
			t.Fatalf("detail missing %q: %+v", key, ev[0].Detail)
		}
	}
	if ev[0].Detail["key"] != sample.Key || ev[0].Detail["requestId"] != sample.RequestID {
		t.Fatalf("detail: %+v", ev[0].Detail)
	}
	cons := dm.Components["db"].Consistency
	if cons == nil || cons.CacheDbStale != 1 {
		t.Fatalf("counter: %+v", cons)
	}
	if cons.LastCacheKey != sample.Key || cons.LastCacheRequestID != sample.RequestID {
		t.Fatalf("status evidence: %+v", cons)
	}

	cd.NoteAppCacheStale(ns, 1, sample)
	if again := cd.Eval(ns, dm, topo, nil); len(again) != 0 {
		t.Fatalf("same total should not refire, got %d", len(again))
	}

	cd.NoteAppCacheStale(ns, 2, sample)
	if refire := cd.Eval(ns, dm, topo, nil); len(refire) != 1 {
		t.Fatalf("want refire on 1→2, got %d", len(refire))
	}
	if dm.Components["db"].Consistency.CacheDbStale != 2 {
		t.Fatalf("counter after refire: %+v", dm.Components["db"].Consistency)
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
