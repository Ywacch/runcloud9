package metrics

import (
	"fmt"
	"time"

	"github.com/ywacch/cloud9/pkg/rules"
	"github.com/ywacch/cloud9/pkg/templates"
)

const (
	rulePartitionStaleReads = rules.IDPartitionStaleReads
	rulePartitionWriteStall = rules.IDPartitionWriteStall

	partitionLagThresholdSec         = 0.05
	partitionDBErrorRateThreshold    = 0.01
	partitionDBQueryLatencyThreshold = 0.5

	partitionScenarioStallReplica        = "stall_replica"
	partitionScenarioSplitPrimaryReplica = "split_primary_replica"
)

// PartitionContext describes an active chaos partition policy in the namespace.
type PartitionContext struct {
	Scenario        string
	TargetComponent string
}

// NoteActivePartition records the active partition scenario for CAP surfacing.
// Pass nil to clear when no partition policy is present.
func (cd *ConsistencyDetector) NoteActivePartition(namespace string, part *PartitionContext) {
	if cd == nil || namespace == "" {
		return
	}
	cd.mu.Lock()
	defer cd.mu.Unlock()
	if part == nil || part.Scenario == "" {
		delete(cd.activePartition, namespace)
		delete(cd.partitionRYWSeen, namespace)
		delete(cd.partitionStaleSeen, namespace)
		return
	}
	existing, had := cd.activePartition[namespace]
	cd.activePartition[namespace] = *part
	if !had || existing.Scenario != part.Scenario {
		traffic := cd.appTraffic[namespace]
		cd.partitionRYWSeen[namespace] = traffic.RYWTotal
		cd.partitionStaleSeen[namespace] = traffic.ReplicaStaleTotal
	}
}

func (cd *ConsistencyDetector) evalPartitionCAP(namespace string, now time.Time, dm *DeploymentMetrics, topo TopologyFlags, _ *templates.TemplateSpec) []Event {
	part, ok := cd.activePartitionFor(namespace)
	if !ok {
		cd.clear(namespace, rulePartitionStaleReads)
		cd.clear(namespace, rulePartitionWriteStall)
		return nil
	}

	mode := topo.ReplicationMode
	if mode == "" {
		mode = "async"
	}

	if partitionUsesWriteStallCAP(mode, topo.DBReadReplicas, part.Scenario) {
		cd.clear(namespace, rulePartitionStaleReads)
		return cd.evalPartitionSyncCAP(namespace, now, dm, part, mode)
	}
	cd.clear(namespace, rulePartitionWriteStall)
	return cd.evalPartitionAsyncCAP(namespace, now, dm, part, mode)
}

// partitionUsesWriteStallCAP reports whether the active partition leaves no replica
// ack path, so the CAP lesson is stalled writes (CP) rather than stale reads (AP).
func partitionUsesWriteStallCAP(mode string, replicas int, scenario string) bool {
	switch mode {
	case "sync":
		return true
	case "semi-sync":
		return !semiSyncAckPathRemains(replicas, scenario)
	default:
		return false
	}
}

// semiSyncAckPathRemains reports whether ANY 1 (*) can still ack after the cut.
// stall_replica with two or more replicas leaves one standby; split_primary_replica
// (or a single replica) does not. split_app_store is not a replica-ack cut.
func semiSyncAckPathRemains(replicas int, scenario string) bool {
	switch scenario {
	case partitionScenarioSplitPrimaryReplica:
		return false
	case partitionScenarioStallReplica:
		return replicas >= 2
	default:
		return true
	}
}

func (cd *ConsistencyDetector) activePartitionFor(namespace string) (PartitionContext, bool) {
	cd.mu.Lock()
	defer cd.mu.Unlock()
	part, ok := cd.activePartition[namespace]
	return part, ok
}

func (cd *ConsistencyDetector) evalPartitionAsyncCAP(namespace string, now time.Time, dm *DeploymentMetrics, part PartitionContext, mode string) []Event {
	if !partitionAsyncBad(cd, namespace, dm) {
		cd.clear(namespace, rulePartitionStaleReads)
		return nil
	}
	if !cd.fire(namespace, rulePartitionStaleReads) {
		return nil
	}
	cd.incrementViolation(namespace, rulePartitionStaleReads)
	return []Event{cd.partitionStaleReadsEvent(now, part, mode)}
}

func (cd *ConsistencyDetector) evalPartitionSyncCAP(namespace string, now time.Time, dm *DeploymentMetrics, part PartitionContext, mode string) []Event {
	if !partitionSyncBad(dm) {
		cd.clear(namespace, rulePartitionWriteStall)
		return nil
	}
	if !cd.fire(namespace, rulePartitionWriteStall) {
		return nil
	}
	cd.incrementViolation(namespace, rulePartitionWriteStall)
	return []Event{cd.partitionWriteStallEvent(now, part, mode, dm)}
}

func partitionAsyncBad(cd *ConsistencyDetector, namespace string, dm *DeploymentMetrics) bool {
	cd.mu.Lock()
	traffic := cd.appTraffic[namespace]
	rywTotal := traffic.RYWTotal
	staleTotal := traffic.ReplicaStaleTotal
	cd.mu.Unlock()

	if rywTotal > cd.partitionRYWBaseline(namespace) {
		return true
	}
	if staleTotal > cd.partitionStaleBaseline(namespace) {
		return true
	}
	return maxReplicaLagSeconds(dm) > partitionLagThresholdSec
}

func partitionSyncBad(dm *DeploymentMetrics) bool {
	if dm == nil {
		return false
	}
	db := dm.Components["db"]
	if db.StorageIO != nil && db.StorageIO.ErrorRate > partitionDBErrorRateThreshold {
		return true
	}
	return db.DbQueryLatency > partitionDBQueryLatencyThreshold
}

func (cd *ConsistencyDetector) partitionRYWBaseline(namespace string) float64 {
	cd.mu.Lock()
	defer cd.mu.Unlock()
	return cd.partitionRYWSeen[namespace]
}

func (cd *ConsistencyDetector) partitionStaleBaseline(namespace string) float64 {
	cd.mu.Lock()
	defer cd.mu.Unlock()
	return cd.partitionStaleSeen[namespace]
}

func maxReplicaLagSeconds(dm *DeploymentMetrics) float64 {
	if dm == nil {
		return 0
	}
	db, ok := dm.Components["db"]
	if !ok {
		return 0
	}
	var maxLag float64
	for _, layer := range db.Layers {
		if layer.ReplicationLagSeconds == nil {
			continue
		}
		if *layer.ReplicationLagSeconds > maxLag {
			maxLag = *layer.ReplicationLagSeconds
		}
	}
	return maxLag
}

func (cd *ConsistencyDetector) partitionStaleReadsEvent(now time.Time, part PartitionContext, mode string) Event {
	desc := "Under partition, replicas can fall behind while writes keep succeeding"
	if part.Scenario != "" {
		desc = fmt.Sprintf("Partition %s on %s: reads can go stale while %s replication keeps accepting writes", part.Scenario, part.TargetComponent, mode)
	}
	return Event{
		Timestamp:   now,
		Type:        "invariant",
		Source:      "detector",
		Component:   "db",
		RuleID:      rulePartitionStaleReads,
		Audience:    "student",
		Title:       "Consistency degraded under partition",
		Description: desc,
		Detail: map[string]string{
			"scenario":        part.Scenario,
			"targetComponent": part.TargetComponent,
			"replicationMode": mode,
			"capTradeoff":     "availability_over_consistency",
		},
	}
}

func (cd *ConsistencyDetector) partitionWriteStallEvent(now time.Time, part PartitionContext, mode string, dm *DeploymentMetrics) Event {
	desc := "Under partition, synchronous replication can block commits"
	if part.Scenario != "" {
		desc = fmt.Sprintf("Partition %s on %s: %s replication is blocking writes to preserve consistency", part.Scenario, part.TargetComponent, mode)
	}
	detail := map[string]string{
		"scenario":        part.Scenario,
		"targetComponent": part.TargetComponent,
		"replicationMode": mode,
		"capTradeoff":     "consistency_over_availability",
	}
	if db, ok := dm.Components["db"]; ok {
		if db.StorageIO != nil && db.StorageIO.ErrorRate > 0 {
			detail["dbErrorRate"] = fmt.Sprintf("%.3f", db.StorageIO.ErrorRate)
		}
		if db.DbQueryLatency > 0 {
			detail["dbQueryLatency"] = fmt.Sprintf("%.3f", db.DbQueryLatency)
		}
	}
	return Event{
		Timestamp:   now,
		Type:        "invariant",
		Source:      "detector",
		Component:   "db",
		RuleID:      rulePartitionWriteStall,
		Audience:    "student",
		Title:       "Writes stalling under partition",
		Description: desc,
		Detail:      detail,
	}
}
