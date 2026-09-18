package metrics

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ywacch/cloud9/pkg/rules"
)

const ruleReplicaCannotCatchUp = rules.IDReplicaCannotCatchUp

// NoteDBReplicaPrimaryConnectivity records replica pod readiness and whether each ready
// standby appears in pg_stat_replication on the primary. Call once per Collect tick before Eval.
func (cd *ConsistencyDetector) NoteDBReplicaPrimaryConnectivity(namespace string, ready, connectedOnPrimary map[string]bool) {
	if cd == nil || namespace == "" {
		return
	}
	cd.mu.Lock()
	defer cd.mu.Unlock()
	cd.replicaReady[namespace] = cloneBoolMap(ready)
	cd.replicaConnectedOnPrimary[namespace] = cloneBoolMap(connectedOnPrimary)
}

func (cd *ConsistencyDetector) evalReplicaCannotCatchUp(namespace string, now time.Time, topo TopologyFlags) []Event {
	if topo.DBReadReplicas <= 0 {
		cd.clear(namespace, ruleReplicaCannotCatchUp)
		return nil
	}
	if _, ok := cd.activePartitionFor(namespace); ok {
		cd.clear(namespace, ruleReplicaCannotCatchUp)
		return nil
	}

	cd.mu.Lock()
	ready := cd.replicaReady[namespace]
	connected := cd.replicaConnectedOnPrimary[namespace]
	cd.mu.Unlock()

	var disconnected []string
	for pod, isReady := range ready {
		if !isReady {
			continue
		}
		if connected == nil || !connected[pod] {
			disconnected = append(disconnected, pod)
		}
	}
	sort.Strings(disconnected)

	if len(disconnected) == 0 {
		cd.clear(namespace, ruleReplicaCannotCatchUp)
		return nil
	}
	if !cd.fire(namespace, ruleReplicaCannotCatchUp) {
		return nil
	}
	cd.incrementViolation(namespace, ruleReplicaCannotCatchUp)
	return []Event{cd.replicaCannotCatchUpEvent(now, disconnected)}
}

func (cd *ConsistencyDetector) replicaCannotCatchUpEvent(now time.Time, pods []string) Event {
	desc := "A ready replica is not streaming from the primary; required WAL may have been removed"
	if len(pods) == 1 {
		desc = fmt.Sprintf("%s is ready but not connected on the primary; required WAL may have been removed", pods[0])
	} else if len(pods) > 1 {
		desc = fmt.Sprintf("%d ready replicas are not connected on the primary; required WAL may have been removed", len(pods))
	}
	return Event{
		Timestamp:   now,
		Type:        "invariant",
		Source:      "detector",
		Component:   "db",
		RuleID:      ruleReplicaCannotCatchUp,
		Audience:    "student",
		Title:       "Replica cannot catch up",
		Description: desc,
		Detail: map[string]string{
			"kind": "replica_cannot_catch_up",
			"pods": strings.Join(pods, ","),
		},
	}
}

func cloneBoolMap(in map[string]bool) map[string]bool {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]bool, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
