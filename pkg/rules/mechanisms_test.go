package rules

import (
	"testing"

	"github.com/ywacch/cloud9/pkg/deployer"
)

func TestPrecondition_HoldsOnMatchingDesign(t *testing.T) {
	t.Parallel()

	p := Precondition{Role: "db", Key: "replicationMode", AnyOf: []string{"async", "semi-sync"}}
	dc := deployer.DesignConfig{
		Roles: map[string][]deployer.InstanceConfig{
			"db": {{
				ID:       "db1",
				Settings: map[string]any{"replicationMode": "async"},
			}},
		},
	}
	if !p.Holds(dc) {
		t.Fatal("expected async replication to satisfy precondition")
	}
}

func TestMechanismsFor_ExcludesWrongReplicationMode(t *testing.T) {
	t.Parallel()

	dc := deployer.DesignConfig{
		Roles: map[string][]deployer.InstanceConfig{
			"db": {{
				ID:       "db1",
				Settings: map[string]any{"replicationMode": "semi-sync"},
			}},
		},
	}
	got := MechanismsFor(dc, []string{IDFailoverWriteLoss})
	for _, m := range got {
		if m.ID == "async_promote_write_loss" {
			t.Fatalf("async mechanism should not match semi-sync design: %+v", got)
		}
	}
	if len(got) != 1 || got[0].ID != "semi_sync_promote_gap" {
		t.Fatalf("want semi_sync_promote_gap only, got %+v", got)
	}
}

func TestMechanismsFor_ReturnsEmptyWhenNoRulesFired(t *testing.T) {
	t.Parallel()

	dc := deployer.DesignConfig{
		Roles: map[string][]deployer.InstanceConfig{
			"db": {{
				ID:       "db1",
				Settings: map[string]any{"replicationMode": "async"},
			}},
		},
	}
	if got := MechanismsFor(dc, nil); len(got) != 0 {
		t.Fatalf("want empty, got %+v", got)
	}
	if got := MechanismsFor(dc, []string{}); len(got) != 0 {
		t.Fatalf("want empty for unfired rules, got %+v", got)
	}
}

func TestMechanismsFor_IncludesWave3ConsistencyMechanisms(t *testing.T) {
	t.Parallel()

	dc := deployer.DesignConfig{
		Roles: map[string][]deployer.InstanceConfig{
			"db": {{
				ID: "db1",
				Settings: map[string]any{
					"readConsistency": "ryw",
					"replicationMode": "async",
					"readReplicas":    2,
				},
			}},
			"cache": {{
				ID:       "cache1",
				Settings: map[string]any{"cacheStrategy": "write-through"},
			}},
		},
	}
	fired := []string{
		IDRYWViolation,
		IDMonotonicViolation,
		IDCacheDBStale,
		IDPartitionStaleReads,
		IDPartitionWriteStall,
		IDReplicaCannotCatchUp,
	}
	got := MechanismsFor(dc, fired)
	ids := mechanismIDs(got)
	for _, want := range []string{
		"ryw_violation_replica_route",
		"cache_db_stale_proxy",
		"partition_stale_reads_async",
		"replica_cannot_catch_up",
	} {
		if !containsString(ids, want) {
			t.Fatalf("missing %q in %+v", want, ids)
		}
	}
	if containsString(ids, "monotonic_violation_replica_route") {
		t.Fatalf("monotonic mechanism should not match ryw-only readConsistency: %+v", ids)
	}
	if containsString(ids, "partition_write_stall_semi_sync") {
		t.Fatalf("semi-sync stall mechanism should not match async design: %+v", ids)
	}
}

func mechanismIDs(ms []Mechanism) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = m.ID
	}
	return out
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
