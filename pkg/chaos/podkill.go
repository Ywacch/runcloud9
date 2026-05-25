package chaos

import (
	"context"
	"fmt"

	"github.com/ywacch/cloud9/pkg/deployer"
	"k8s.io/client-go/kubernetes"
)

// PodKillScenario deletes one running pod so the Deployment controller restarts it.
type PodKillScenario struct {
	Client kubernetes.Interface
}

// Apply resolves the target and deletes the first running pod.
func (s *PodKillScenario) Apply(ctx context.Context, namespace string, design deployer.DeploymentDesign, targetComponent string) error {
	resolved, err := resolveTarget(targetComponent, design)
	if err != nil {
		return err
	}

	podName, err := findRunningPod(ctx, s.Client, namespace, resolved.AppLabel)
	if err != nil {
		return fmt.Errorf("no running pod for component %q: %w", targetComponent, err)
	}

	return deletePod(ctx, s.Client, namespace, podName)
}

// Description returns UI copy for the scenario.
func (s *PodKillScenario) Description(targetComponent string) string {
	return fmt.Sprintf("Kill %s pod — triggers K8s restart", targetComponent)
}
