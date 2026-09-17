package metrics

import (
	"testing"

	"github.com/ywacch/cloud9/pkg/deployer"
)

func TestApplyReplicaPromotionPreview_mostCurrent(t *testing.T) {
	t.Parallel()

	layers := []ComponentLayerMetrics{
		{ID: "postgres-replica-0", Role: dbLayerRoleReplica},
		{ID: "postgres-replica-1", Role: dbLayerRoleReplica},
	}
	connected := map[string]bool{
		"postgres-replica-0": true,
		"postgres-replica-1": true,
	}
	syncByPod := map[string]string{
		"postgres-replica-0": "async",
		"postgres-replica-1": "sync",
	}
	lagBytesByPod := map[string]int64{
		"postgres-replica-0": 8192,
		"postgres-replica-1": 0,
	}

	applyReplicaPromotionPreview(layers, connected, syncByPod, lagBytesByPod)

	if !layers[1].PromotionCandidate {
		t.Fatalf("expected replica-1 marked candidate, got %+v", layers)
	}
	if layers[1].SyncState != "sync" {
		t.Fatalf("sync state: %+v", layers[1])
	}
	if layers[1].LagBytes == nil || *layers[1].LagBytes != 0 {
		t.Fatalf("lag bytes: %+v", layers[1].LagBytes)
	}
	if layers[0].PromotionCandidate {
		t.Fatal("replica-0 should not be candidate")
	}
}

func TestApplyReplicaPromotionPreview_disconnectedAsync(t *testing.T) {
	t.Parallel()

	layers := []ComponentLayerMetrics{
		{ID: "postgres-replica-0", Role: dbLayerRoleReplica},
	}
	applyReplicaPromotionPreview(layers, map[string]bool{"postgres-replica-0": false}, nil, nil)
	if layers[0].SyncState != "" {
		t.Fatalf("expected empty sync state, got %q", layers[0].SyncState)
	}
	if layers[0].LagBytes != nil {
		t.Fatal("disconnected replica should not have lag bytes")
	}
	if layers[0].PromotionCandidate {
		t.Fatal("disconnected replica should not be candidate")
	}
}

func TestApplyReplicaPromotionPreview_matchesPickCandidate(t *testing.T) {
	t.Parallel()

	candidates := []deployer.PromotionCandidate{
		{PodName: "postgres-replica-0", SyncState: "streaming", LagBytes: 4096},
		{PodName: "postgres-replica-1", SyncState: "async", LagBytes: 0},
	}
	picked, err := deployer.PickCandidate(candidates, deployer.PreferMostCurrent)
	if err != nil {
		t.Fatalf("PickCandidate: %v", err)
	}

	layers := []ComponentLayerMetrics{
		{ID: "postgres-replica-0", Role: dbLayerRoleReplica},
		{ID: "postgres-replica-1", Role: dbLayerRoleReplica},
	}
	connected := map[string]bool{
		"postgres-replica-0": true,
		"postgres-replica-1": true,
	}
	syncByPod := map[string]string{
		"postgres-replica-0": "streaming",
		"postgres-replica-1": "async",
	}
	lagBytesByPod := map[string]int64{
		"postgres-replica-0": 4096,
		"postgres-replica-1": 0,
	}
	applyReplicaPromotionPreview(layers, connected, syncByPod, lagBytesByPod)

	for _, layer := range layers {
		if layer.ID == picked.PodName {
			if !layer.PromotionCandidate {
				t.Fatalf("expected %s marked", picked.PodName)
			}
			return
		}
	}
	t.Fatalf("picked pod %q not found in layers", picked.PodName)
}
