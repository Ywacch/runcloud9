package chaos

import (
	"context"
	"fmt"

	"github.com/ywacch/cloud9/pkg/deployer"
	"k8s.io/client-go/kubernetes"
)

const ScenarioKillReplica = "kill_replica"

// KillReplicaScenario deletes one running postgres standby pod so StatefulSet
// recreates it from pg_basebackup. Contrast with stall_replica + Heal catch-up.
type KillReplicaScenario struct {
	Client kubernetes.Interface
}

// Apply deletes the first running postgres-replica pod in the namespace.
func (s *KillReplicaScenario) Apply(ctx context.Context, namespace string, design deployer.DeploymentDesign, targetComponent string) error {
	if s == nil || s.Client == nil {
		return fmt.Errorf("chaos: kubernetes client is required")
	}
	if targetComponent != "db" {
		return fmt.Errorf("chaos: kill_replica only supports targetComponent \"db\"")
	}
	if err := requireDBStreamingReplicas(design); err != nil {
		return err
	}

	podName, err := findRunningPod(ctx, s.Client, namespace, postgresReplicaAppLabel)
	if err != nil {
		return fmt.Errorf("no running postgres replica pod: %w", err)
	}
	return deletePod(ctx, s.Client, namespace, podName)
}

// Revert is a no-op for kill_replica.
func (s *KillReplicaScenario) Revert(context.Context, string, deployer.DeploymentDesign, string) error {
	return nil
}

// Stateful reports that kill_replica does not leave reversible cluster state.
func (s *KillReplicaScenario) Stateful() bool { return false }

// Description returns UI copy for the scenario.
func (s *KillReplicaScenario) Description(targetComponent string) string {
	return fmt.Sprintf("Kill %s read replica — triggers full pg_basebackup re-clone", targetComponent)
}
