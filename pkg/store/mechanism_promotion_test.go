package store

import (
	"testing"
	"time"
)

func TestAggregatePromotionSummaries_groupsBySignature(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	rows := []InferredMechanismPromotionRow{
		{
			Signature:         "abc",
			TriggerRule:       "failover_write_loss",
			CreatedAt:         t0,
			ClaimText:         "first",
			ProposedMechanism: map[string]any{"trigger": "failover_write_loss"},
		},
		{
			Signature:   "abc",
			TriggerRule: "failover_write_loss",
			CreatedAt:   t0.Add(time.Hour),
			ClaimText:   "second",
		},
		{
			Signature:   "def",
			TriggerRule: "pod_restart",
			CreatedAt:   t0,
		},
	}
	sums := aggregatePromotionSummaries(rows)
	if len(sums) != 2 {
		t.Fatalf("len %d", len(sums))
	}
	if sums[0].Signature != "abc" || sums[0].OccurrenceCount != 2 {
		t.Fatalf("abc group: %+v", sums[0])
	}
}
