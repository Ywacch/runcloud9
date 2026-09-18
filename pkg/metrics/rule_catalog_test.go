package metrics

import (
	"testing"

	"github.com/ywacch/cloud9/pkg/rules"
)

func TestDetectorRuleIDs_ResolveInCatalog(t *testing.T) {
	ids := []string{
		ruleCacheHitLow,
		ruleErrorRate,
		ruleDBLatency,
		ruleQueueDepth,
		ruleDBConnPressure,
		ruleMQBackpressure,
		cascadeRuleCacheDB,
		cascadeRuleQueueWorker,
		cascadeRuleDBPoolErrors,
		cascadeRuleLatencyMiss,
		cascadeRuleMQBackpressure,
		ruleRYWViolation,
		ruleMonotonicViolation,
		ruleCacheDbStale,
		rulePartitionStaleReads,
		rulePartitionWriteStall,
		ruleReplicaCannotCatchUp,
		RuleFailoverWriteLoss,
		rulePodRestart,
		rulePodOOM,
	}
	for _, id := range ids {
		t.Run(id, func(t *testing.T) {
			if id == "" {
				t.Fatal("detector rule id is empty")
			}
			if _, ok := rules.Lookup(id); !ok {
				t.Fatalf("rules.Lookup(%q) not found in catalog", id)
			}
		})
	}
}
