// Package chaos implements one-shot chaos scenarios against deployment namespaces.
package chaos

import (
	"context"
	"fmt"

	"github.com/ywacch/cloud9/pkg/deployer"
	"k8s.io/client-go/kubernetes"
)

// ChaosScenario applies a single fire-and-forget disruption in a deployment namespace.
type ChaosScenario interface {
	Apply(ctx context.Context, namespace string, design deployer.DeploymentDesign, targetComponent string) error
	Description(targetComponent string) string
}

// NewScenario constructs a scenario by name.
func NewScenario(name string, client kubernetes.Interface) (ChaosScenario, error) {
	switch name {
	case "pod_kill":
		if client == nil {
			return nil, fmt.Errorf("chaos: kubernetes client is required")
		}
		return &PodKillScenario{Client: client}, nil
	case "cache_flush":
		if client == nil {
			return nil, fmt.Errorf("chaos: kubernetes client is required")
		}
		return &CacheFlushScenario{Client: client}, nil
	default:
		return nil, fmt.Errorf("chaos: unknown scenario %q", name)
	}
}
