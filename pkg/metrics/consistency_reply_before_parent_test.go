package metrics

import (
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/loadgen"
	"github.com/ywacch/cloud9/pkg/loadgen/vu"
	"github.com/ywacch/cloud9/pkg/rules"
)

func TestConsistency_ReplyBeforeParentFiresOnDelta(t *testing.T) {
	t.Parallel()

	cd := NewConsistencyDetector(&stubConsistencyProbeReader{ok: false})
	ns := "ns-rbp"
	topo := TopologyFlags{HasDB: true}
	dm := &DeploymentMetrics{
		Components: map[string]ComponentMetrics{"db": {Component: "db"}},
		LoadGen: &loadgen.LoadStats{
			VU: &vu.Stats{
				Observer: vu.ObserverStats{
					Examples: []vu.ObservationExample{{
						RequestID: "req-1",
					}},
				},
			},
		},
	}
	dm.LoadGen.VU.Observer.Examples[0].ServedBy.Replies = "replica-0"
	dm.LoadGen.VU.Observer.Examples[0].ServedBy.Parents = "replica-1"

	cd.NoteAppTrafficConsistency(ns, appTrafficConsistency{ThreadOrphanReadsTotal: 1})
	ev := cd.Eval(ns, dm, topo, nil)
	if len(ev) != 1 || ev[0].RuleID != rules.IDReplyBeforeParent {
		t.Fatalf("events: %+v", ev)
	}
	if ev[0].Detail["fidelity"] != rules.FidelitySimulated {
		t.Fatalf("detail: %+v", ev[0].Detail)
	}
	if ev[0].Detail["repliesHost"] != "replica-0" || ev[0].Detail["parentsHost"] != "replica-1" {
		t.Fatalf("detail: %+v", ev[0].Detail)
	}

	cd.NoteAppTrafficConsistency(ns, appTrafficConsistency{ThreadOrphanReadsTotal: 1})
	if ev2 := cd.Eval(ns, dm, topo, nil); len(ev2) != 0 {
		t.Fatalf("expected suppression, got %+v", ev2)
	}
}

func TestConsistency_ReplyBeforeParentClearsAfterQuiet(t *testing.T) {
	t.Parallel()

	cd := NewConsistencyDetector(&stubConsistencyProbeReader{ok: false})
	ns := "ns-rbp-clear"
	topo := TopologyFlags{HasDB: true}
	dm := &DeploymentMetrics{Components: map[string]ComponentMetrics{"db": {Component: "db"}}}

	cd.NoteAppTrafficConsistency(ns, appTrafficConsistency{ThreadOrphanReadsTotal: 2})
	if ev := cd.Eval(ns, dm, topo, nil); len(ev) != 1 {
		t.Fatalf("first fire: %+v", ev)
	}

	for i := 0; i < 3; i++ {
		cd.NoteAppTrafficConsistency(ns, appTrafficConsistency{ThreadOrphanReadsTotal: 2})
		cd.Eval(ns, dm, topo, nil)
	}
	cd.NoteAppTrafficConsistency(ns, appTrafficConsistency{ThreadOrphanReadsTotal: 3})
	ev := cd.Eval(ns, dm, topo, nil)
	if len(ev) != 1 {
		t.Fatalf("expected refire after quiet, got %+v", ev)
	}
}

func TestConsistency_CleanupRemovesThreadState(t *testing.T) {
	t.Parallel()

	cd := NewConsistencyDetector(&stubConsistencyProbeReader{ok: false})
	ns := "ns-rbp-cleanup"
	topo := TopologyFlags{HasDB: true}
	dm := &DeploymentMetrics{Components: map[string]ComponentMetrics{"db": {Component: "db"}}}

	cd.NoteAppTrafficConsistency(ns, appTrafficConsistency{ThreadOrphanReadsTotal: 1})
	cd.Eval(ns, dm, topo, nil)
	cd.Cleanup(ns)

	cd.NoteAppTrafficConsistency(ns, appTrafficConsistency{ThreadOrphanReadsTotal: 1})
	ev := cd.Eval(ns, dm, topo, nil)
	if len(ev) != 1 {
		t.Fatalf("expected fire after cleanup reset, got %+v", ev)
	}
	_ = time.Now()
}
