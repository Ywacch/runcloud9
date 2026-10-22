package rules

import (
	"testing"
)

func TestCatalog_IDsUniqueAndNonEmpty(t *testing.T) {
	t.Parallel()
	seen := make(map[string]struct{}, len(staticCatalog))
	for _, r := range All() {
		if r.ID == "" {
			t.Fatal("rule id is empty")
		}
		if r.Summary == "" {
			t.Fatalf("rule %q has empty summary", r.ID)
		}
		if r.Visibility == "" {
			t.Fatalf("rule %q has empty visibility", r.ID)
		}
		if _, dup := seen[r.ID]; dup {
			t.Fatalf("duplicate rule id %q", r.ID)
		}
		seen[r.ID] = struct{}{}
	}
	if len(seen) != 21 {
		t.Fatalf("expected 21 static rules, got %d", len(seen))
	}
}

func TestCatalog_ReplyBeforeParentFidelity(t *testing.T) {
	t.Parallel()
	r, ok := Lookup(IDReplyBeforeParent)
	if !ok {
		t.Fatal("missing reply_before_parent rule")
	}
	if r.Fidelity != FidelitySimulated {
		t.Fatalf("fidelity: got %q want %q", r.Fidelity, FidelitySimulated)
	}
}

func TestCatalog_LookupKnownRule(t *testing.T) {
	t.Parallel()
	cases := []string{
		IDCacheHitLow,
		IDErrorRate,
		IDDBLatency,
		IDQueueDepth,
		IDDBConnPressure,
		IDMQBackpressure,
		IDCascadeCacheDB,
		IDCascadeQueueWorker,
		IDCascadeDBPoolErrors,
		IDCascadeLatencyMiss,
		IDCascadeMQBackpressure,
		IDRYWViolation,
		IDMonotonicViolation,
		IDCacheDBStale,
		IDPartitionStaleReads,
		IDPartitionWriteStall,
		IDReplicaCannotCatchUp,
		IDReplyBeforeParent,
		IDFailoverWriteLoss,
		IDPodRestart,
		IDPodOOM,
	}
	for _, id := range cases {
		r, ok := Lookup(id)
		if !ok {
			t.Fatalf("Lookup(%q) = false", id)
		}
		if r.ID != id {
			t.Fatalf("Lookup(%q).ID = %q", id, r.ID)
		}
	}
}

func TestCatalog_ExcludesDoubleSpend(t *testing.T) {
	t.Parallel()
	if _, ok := Lookup("double_spend"); ok {
		t.Fatal("double_spend must not be in the static catalog")
	}
	if _, ok := Lookup("oversell"); ok {
		t.Fatal("oversell is template-only and must not be in the static catalog")
	}
}
