package metrics

import (
	"testing"
)

func eventForRule(ev []Event, ruleID string) (Event, bool) {
	for _, e := range ev {
		if e.RuleID == ruleID {
			return e, true
		}
	}
	return Event{}, false
}

func TestConsistencyDetector_PartitionCAP(t *testing.T) {
	t.Parallel()

	lag := 0.1
	scenarios := []string{"stall_replica", "split_primary_replica", "split_app_store"}

	tests := []struct {
		name       string
		mode       string
		replicas   int
		scenario   string
		setup      func(cd *ConsistencyDetector, ns string)
		db         ComponentMetrics
		wantRule   string
		wantCap    string
		wantAbsent string
	}{
		{
			name:     "async_ryw_delta",
			mode:     "async",
			scenario: "stall_replica",
			setup: func(cd *ConsistencyDetector, ns string) {
				cd.NoteAppTrafficConsistency(ns, appTrafficConsistency{RYWTotal: 1})
				cd.Eval(ns, &DeploymentMetrics{Components: map[string]ComponentMetrics{"db": {Component: "db"}}}, TopologyFlags{HasDB: true}, nil)
				cd.NoteActivePartition(ns, &PartitionContext{Scenario: "stall_replica", TargetComponent: "db"})
				cd.NoteAppTrafficConsistency(ns, appTrafficConsistency{RYWTotal: 2})
			},
			wantRule: rulePartitionStaleReads,
			wantCap:  "availability_over_consistency",
		},
		{
			name:     "async_stale_reads_delta",
			mode:     "async",
			scenario: "split_primary_replica",
			setup: func(cd *ConsistencyDetector, ns string) {
				cd.NoteActivePartition(ns, &PartitionContext{Scenario: "split_primary_replica", TargetComponent: "db"})
				cd.NoteAppTrafficConsistency(ns, appTrafficConsistency{ReplicaStaleTotal: 4})
			},
			wantRule: rulePartitionStaleReads,
			wantCap:  "availability_over_consistency",
		},
		{
			name:     "async_replication_lag",
			mode:     "async",
			scenario: "stall_replica",
			setup: func(cd *ConsistencyDetector, ns string) {
				cd.NoteActivePartition(ns, &PartitionContext{Scenario: "stall_replica", TargetComponent: "db"})
			},
			db: ComponentMetrics{
				Component: "db",
				Layers:    []ComponentLayerMetrics{{ReplicationLagSeconds: &lag}},
			},
			wantRule: rulePartitionStaleReads,
			wantCap:  "availability_over_consistency",
		},
		{
			name:     "sync_write_errors",
			mode:     "sync",
			scenario: "stall_replica",
			setup: func(cd *ConsistencyDetector, ns string) {
				cd.NoteActivePartition(ns, &PartitionContext{Scenario: "stall_replica", TargetComponent: "db"})
			},
			db: ComponentMetrics{
				Component: "db",
				StorageIO: &StorageIO{ErrorRate: 0.02},
			},
			wantRule: rulePartitionWriteStall,
			wantCap:  "consistency_over_availability",
		},
		{
			name:     "sync_query_latency",
			mode:     "sync",
			scenario: "split_primary_replica",
			setup: func(cd *ConsistencyDetector, ns string) {
				cd.NoteActivePartition(ns, &PartitionContext{Scenario: "split_primary_replica", TargetComponent: "db"})
			},
			db: ComponentMetrics{
				Component:      "db",
				DbQueryLatency: 0.6,
			},
			wantRule: rulePartitionWriteStall,
			wantCap:  "consistency_over_availability",
		},
		{
			name:     "sync_skips_stale_reads_rule",
			mode:     "sync",
			scenario: "stall_replica",
			setup: func(cd *ConsistencyDetector, ns string) {
				cd.NoteAppTrafficConsistency(ns, appTrafficConsistency{RYWTotal: 3})
				cd.Eval(ns, &DeploymentMetrics{Components: map[string]ComponentMetrics{"db": {Component: "db"}}}, TopologyFlags{HasDB: true, ReplicationMode: "sync"}, nil)
				cd.NoteActivePartition(ns, &PartitionContext{Scenario: "stall_replica", TargetComponent: "db"})
				cd.NoteAppTrafficConsistency(ns, appTrafficConsistency{RYWTotal: 3})
			},
			wantAbsent: rulePartitionStaleReads,
		},
		{
			name:     "async_skips_write_stall_rule",
			mode:     "async",
			scenario: "stall_replica",
			setup: func(cd *ConsistencyDetector, ns string) {
				cd.NoteActivePartition(ns, &PartitionContext{Scenario: "stall_replica", TargetComponent: "db"})
			},
			db: ComponentMetrics{
				Component:      "db",
				DbQueryLatency: 0.9,
				StorageIO:      &StorageIO{ErrorRate: 0.05},
			},
			wantAbsent: rulePartitionWriteStall,
		},
		{
			name:     "semisync_stall_two_replicas_stale",
			mode:     "semi-sync",
			replicas: 2,
			scenario: "stall_replica",
			setup: func(cd *ConsistencyDetector, ns string) {
				cd.NoteActivePartition(ns, &PartitionContext{Scenario: "stall_replica", TargetComponent: "db"})
			},
			db: ComponentMetrics{
				Component: "db",
				Layers:    []ComponentLayerMetrics{{ReplicationLagSeconds: &lag}},
			},
			wantRule:   rulePartitionStaleReads,
			wantCap:    "availability_over_consistency",
			wantAbsent: rulePartitionWriteStall,
		},
		{
			name:     "semisync_split_all_replicas_write_stall",
			mode:     "semi-sync",
			replicas: 2,
			scenario: "split_primary_replica",
			setup: func(cd *ConsistencyDetector, ns string) {
				cd.NoteActivePartition(ns, &PartitionContext{Scenario: "split_primary_replica", TargetComponent: "db"})
			},
			db: ComponentMetrics{
				Component: "db",
				StorageIO: &StorageIO{ErrorRate: 0.02},
			},
			wantRule:   rulePartitionWriteStall,
			wantCap:    "consistency_over_availability",
			wantAbsent: rulePartitionStaleReads,
		},
		{
			name:     "semisync_one_replica_write_stall",
			mode:     "semi-sync",
			replicas: 1,
			scenario: "stall_replica",
			setup: func(cd *ConsistencyDetector, ns string) {
				cd.NoteActivePartition(ns, &PartitionContext{Scenario: "stall_replica", TargetComponent: "db"})
			},
			db: ComponentMetrics{
				Component:      "db",
				DbQueryLatency: 0.6,
			},
			wantRule:   rulePartitionWriteStall,
			wantCap:    "consistency_over_availability",
			wantAbsent: rulePartitionStaleReads,
		},
		{
			name:     "semisync_app_store_skips_write_stall",
			mode:     "semi-sync",
			replicas: 2,
			scenario: "split_app_store",
			setup: func(cd *ConsistencyDetector, ns string) {
				cd.NoteActivePartition(ns, &PartitionContext{Scenario: "split_app_store", TargetComponent: "cache"})
			},
			db: ComponentMetrics{
				Component:      "db",
				DbQueryLatency: 0.9,
				StorageIO:      &StorageIO{ErrorRate: 0.05},
			},
			wantAbsent: rulePartitionWriteStall,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cd := NewConsistencyDetector(&stubConsistencyProbeReader{ok: false})
			ns := "ns-partition-" + tc.name
			topo := TopologyFlags{HasDB: true, ReplicationMode: tc.mode, DBReadReplicas: tc.replicas}
			tc.setup(cd, ns)

			db := tc.db
			if db.Component == "" {
				db.Component = "db"
			}
			dm := &DeploymentMetrics{Components: map[string]ComponentMetrics{"db": db}}

			ev := cd.Eval(ns, dm, topo, nil)
			if tc.wantRule != "" {
				partEv, ok := eventForRule(ev, tc.wantRule)
				if !ok {
					t.Fatalf("want rule %q among %d events: %+v", tc.wantRule, len(ev), ev)
				}
				if partEv.Detail["capTradeoff"] != tc.wantCap {
					t.Fatalf("capTradeoff: %+v", partEv.Detail)
				}
				if partEv.Detail["scenario"] != tc.scenario {
					t.Fatalf("scenario detail: %+v", partEv.Detail)
				}
			}
			if tc.wantAbsent != "" {
				if _, ok := eventForRule(ev, tc.wantAbsent); ok {
					t.Fatalf("unexpected rule %q: %+v", tc.wantAbsent, ev)
				}
			}
			if tc.wantRule == "" && tc.wantAbsent == "" && len(ev) != 0 {
				t.Fatalf("want no events, got %d: %+v", len(ev), ev)
			}

			if tc.wantRule != "" {
				again := cd.Eval(ns, dm, topo, nil)
				if _, ok := eventForRule(again, tc.wantRule); ok {
					t.Fatalf("want partition latch suppression, got %+v", again)
				}
			}
		})
	}

	for _, scenario := range scenarios {
		t.Run("no_partition_"+scenario, func(t *testing.T) {
			t.Parallel()

			cd := NewConsistencyDetector(&stubConsistencyProbeReader{ok: false})
			ns := "ns-clear-" + scenario
			topo := TopologyFlags{HasDB: true, ReplicationMode: "async"}
			dm := &DeploymentMetrics{Components: map[string]ComponentMetrics{"db": {Component: "db"}}}

			cd.NoteAppTrafficConsistency(ns, appTrafficConsistency{RYWTotal: 5})
			ev := cd.Eval(ns, dm, topo, nil)
			if _, ok := eventForRule(ev, rulePartitionStaleReads); ok {
				t.Fatalf("without partition want no CAP events, got %+v", ev)
			}
			if _, ok := eventForRule(ev, rulePartitionWriteStall); ok {
				t.Fatalf("without partition want no CAP events, got %+v", ev)
			}
		})
	}
}

func TestConsistencyDetector_PartitionHealClearsLatch(t *testing.T) {
	t.Parallel()

	lag := 0.1
	cd := NewConsistencyDetector(&stubConsistencyProbeReader{ok: false})
	ns := "ns-heal"
	topo := TopologyFlags{HasDB: true, ReplicationMode: "async"}
	dm := &DeploymentMetrics{Components: map[string]ComponentMetrics{
		"db": {
			Component: "db",
			Layers:    []ComponentLayerMetrics{{ReplicationLagSeconds: &lag}},
		},
	}}

	cd.NoteActivePartition(ns, &PartitionContext{Scenario: "stall_replica", TargetComponent: "db"})
	ev := cd.Eval(ns, dm, topo, nil)
	if _, ok := eventForRule(ev, rulePartitionStaleReads); !ok {
		t.Fatalf("initial fire: %+v", ev)
	}

	cd.NoteActivePartition(ns, nil)
	ev = cd.Eval(ns, dm, topo, nil)
	if _, ok := eventForRule(ev, rulePartitionStaleReads); ok {
		t.Fatalf("heal should clear latch without refire, got %+v", ev)
	}

	cd.NoteActivePartition(ns, &PartitionContext{Scenario: "stall_replica", TargetComponent: "db"})
	ev = cd.Eval(ns, dm, topo, nil)
	if _, ok := eventForRule(ev, rulePartitionStaleReads); !ok {
		t.Fatalf("want refire after re-inject, got %+v", ev)
	}
}

func TestPartitionUsesWriteStallCAP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mode     string
		replicas int
		scenario string
		want     bool
	}{
		{name: "async_stall", mode: "async", replicas: 2, scenario: "stall_replica"},
		{name: "async_split", mode: "async", replicas: 2, scenario: "split_primary_replica"},
		{name: "sync_stall", mode: "sync", replicas: 2, scenario: "stall_replica", want: true},
		{name: "sync_split", mode: "sync", replicas: 1, scenario: "split_primary_replica", want: true},
		{name: "semisync_stall_two", mode: "semi-sync", replicas: 2, scenario: "stall_replica"},
		{name: "semisync_split", mode: "semi-sync", replicas: 2, scenario: "split_primary_replica", want: true},
		{name: "semisync_one_replica", mode: "semi-sync", replicas: 1, scenario: "stall_replica", want: true},
		{name: "semisync_app_store", mode: "semi-sync", replicas: 2, scenario: "split_app_store"},
		{name: "empty_mode_async", mode: "", replicas: 2, scenario: "stall_replica"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := partitionUsesWriteStallCAP(tc.mode, tc.replicas, tc.scenario)
			if got != tc.want {
				t.Fatalf("partitionUsesWriteStallCAP(%q, %d, %q)=%v want %v", tc.mode, tc.replicas, tc.scenario, got, tc.want)
			}
		})
	}
}
