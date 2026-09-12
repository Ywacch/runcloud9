package metrics

import (
	"testing"
)

func TestGuaranteeHeldByFrom_requiresLiveSyncGUCs(t *testing.T) {
	t.Parallel()

	topo := TopologyFlags{
		HasDB:            true,
		DBReadReplicas:   2,
		ReplicationMode:  "semi-sync",
		SyncCommitLevel:  "remote_apply",
		SyncStandbyNames: "ANY 1 (*)",
	}
	traffic := appTrafficConsistency{ReplicaReads: 10}

	if got := guaranteeHeldByFrom(traffic, topo, false); got != guaranteeHeldByNone {
		t.Fatalf("without live GUCs want none, got %q", got)
	}
	if got := guaranteeHeldByFrom(traffic, topo, true); got != guaranteeHeldByDB {
		t.Fatalf("with live GUCs want db, got %q", got)
	}
}

func TestTopologyRequiresPostgresSyncGUCs(t *testing.T) {
	t.Parallel()

	if topologyRequiresPostgresSyncGUCs(TopologyFlags{
		HasDB:            true,
		DBReadReplicas:   2,
		ReplicationMode:  "semi-sync",
		SyncCommitLevel:  "remote_apply",
		SyncStandbyNames: "ANY 1 (*)",
	}) {
		return
	}
	t.Fatal("expected semi-sync remote_apply to require sync GUCs")
}
