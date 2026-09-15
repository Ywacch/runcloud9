package metrics

import (
	"testing"
)

func TestConsistencyDetector_ReplicaCannotCatchUp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		topo       TopologyFlags
		ready      map[string]bool
		connected  map[string]bool
		partition  *PartitionContext
		wantRule   string
		wantAbsent string
	}{
		{
			name: "ready_without_primary_row",
			topo: TopologyFlags{HasDB: true, DBReadReplicas: 1},
			ready: map[string]bool{
				"postgres-replica-0": true,
			},
			connected: map[string]bool{
				"postgres-replica-0": false,
			},
			wantRule: ruleReplicaCannotCatchUp,
		},
		{
			name: "ready_with_primary_row",
			topo: TopologyFlags{HasDB: true, DBReadReplicas: 1},
			ready: map[string]bool{
				"postgres-replica-0": true,
			},
			connected: map[string]bool{
				"postgres-replica-0": true,
			},
			wantAbsent: ruleReplicaCannotCatchUp,
		},
		{
			name: "not_ready_without_primary_row",
			topo: TopologyFlags{HasDB: true, DBReadReplicas: 1},
			ready: map[string]bool{
				"postgres-replica-0": false,
			},
			connected: map[string]bool{
				"postgres-replica-0": false,
			},
			wantAbsent: ruleReplicaCannotCatchUp,
		},
		{
			name: "partition_active_stays_silent",
			topo: TopologyFlags{HasDB: true, DBReadReplicas: 1},
			ready: map[string]bool{
				"postgres-replica-0": true,
			},
			connected: map[string]bool{
				"postgres-replica-0": false,
			},
			partition:  &PartitionContext{Scenario: "stall_replica", TargetComponent: "db"},
			wantAbsent: ruleReplicaCannotCatchUp,
		},
		{
			name:       "no_replicas_skips",
			topo:       TopologyFlags{HasDB: true, DBReadReplicas: 0},
			ready:      map[string]bool{"postgres-replica-0": true},
			connected:  map[string]bool{"postgres-replica-0": false},
			wantAbsent: ruleReplicaCannotCatchUp,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cd := NewConsistencyDetector(nil)
			ns := "ns-" + tt.name
			dm := &DeploymentMetrics{Components: map[string]ComponentMetrics{"db": {Component: "db"}}}

			if tt.partition != nil {
				cd.NoteActivePartition(ns, tt.partition)
			}
			cd.NoteDBReplicaPrimaryConnectivity(ns, tt.ready, tt.connected)

			ev := cd.Eval(ns, dm, tt.topo, nil)
			if tt.wantRule != "" {
				if e, ok := eventForRule(ev, tt.wantRule); !ok {
					t.Fatalf("want rule %q, got events: %+v", tt.wantRule, ev)
				} else if e.Detail["pods"] == "" {
					t.Fatalf("event missing pods detail: %+v", e)
				}
			}
			if tt.wantAbsent != "" {
				if _, ok := eventForRule(ev, tt.wantAbsent); ok {
					t.Fatalf("unexpected rule %q: %+v", tt.wantAbsent, ev)
				}
			}

			if again := cd.Eval(ns, dm, tt.topo, nil); len(again) != 0 {
				t.Fatalf("want latch suppression, got %d events", len(again))
			}

			if tt.wantRule != "" {
				cd.NoteDBReplicaPrimaryConnectivity(ns, tt.ready, map[string]bool{
					"postgres-replica-0": true,
				})
				if clear := cd.Eval(ns, dm, tt.topo, nil); len(clear) != 0 {
					t.Fatalf("connected replica should clear without refire: %+v", clear)
				}
				cd.NoteDBReplicaPrimaryConnectivity(ns, tt.ready, tt.connected)
				if refire := cd.Eval(ns, dm, tt.topo, nil); len(refire) != 1 {
					t.Fatalf("want refire after reconnect loss, got %d", len(refire))
				}
			}
		})
	}
}
