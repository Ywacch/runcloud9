package metrics

import "testing"

func TestConsistencyStatus_IncludesPartitionAndCatchupCounts(t *testing.T) {
	t.Parallel()

	t.Run("partitionStaleReads", func(t *testing.T) {
		t.Parallel()
		cd := NewConsistencyDetector(nil)
		ns := "ns-partition-stale"
		dm := &DeploymentMetrics{Components: map[string]ComponentMetrics{"db": {Component: "db"}}}
		cd.NoteActivePartition(ns, &PartitionContext{Scenario: "stall_replica", TargetComponent: "db"})
		cd.NoteAppTrafficConsistency(ns, appTrafficConsistency{RYWTotal: 1})
		cd.Eval(ns, dm, TopologyFlags{HasDB: true, ReplicationMode: "async"}, nil)
		cd.NoteAppTrafficConsistency(ns, appTrafficConsistency{RYWTotal: 2})
		cd.Eval(ns, dm, TopologyFlags{HasDB: true, ReplicationMode: "async"}, nil)

		cons := dm.Components["db"].Consistency
		if cons == nil || cons.PartitionStaleReads != 1 {
			t.Fatalf("partitionStaleReads: %+v", cons)
		}
	})

	t.Run("partitionWriteStall", func(t *testing.T) {
		t.Parallel()
		cd := NewConsistencyDetector(nil)
		ns := "ns-partition-stall"
		dm := &DeploymentMetrics{Components: map[string]ComponentMetrics{
			"db": {Component: "db", StorageIO: &StorageIO{ErrorRate: 0.02}},
		}}
		cd.NoteActivePartition(ns, &PartitionContext{Scenario: "stall_replica", TargetComponent: "db"})
		cd.Eval(ns, dm, TopologyFlags{HasDB: true, ReplicationMode: "sync"}, nil)

		cons := dm.Components["db"].Consistency
		if cons == nil || cons.PartitionWriteStall != 1 {
			t.Fatalf("partitionWriteStall: %+v", cons)
		}
	})

	t.Run("replicaCannotCatchUp", func(t *testing.T) {
		t.Parallel()
		cd := NewConsistencyDetector(nil)
		ns := "ns-replica-catchup"
		dm := &DeploymentMetrics{Components: map[string]ComponentMetrics{"db": {Component: "db"}}}
		cd.NoteDBReplicaPrimaryConnectivity(ns, map[string]bool{"postgres-replica-0": true}, map[string]bool{"postgres-replica-0": false})
		cd.Eval(ns, dm, TopologyFlags{HasDB: true, DBReadReplicas: 1}, nil)

		cons := dm.Components["db"].Consistency
		if cons == nil || cons.ReplicaCannotCatchUp != 1 {
			t.Fatalf("replicaCannotCatchUp: %+v", cons)
		}
	})
}
