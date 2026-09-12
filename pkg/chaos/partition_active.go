package chaos

import (
	"context"
	"errors"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// ErrPartitionAlreadyActive is returned when a namespace already has a chaos partition policy.
var ErrPartitionAlreadyActive = errors.New("chaos: partition already active")

// DefaultPartitionTTL is how long a partition policy stays before C4 auto-revert removes it.
const DefaultPartitionTTL = 15 * time.Minute

// ActivePartitionScenarios lists scenario ids with an active partition policy in the namespace.
func ActivePartitionScenarios(ctx context.Context, client kubernetes.Interface, namespace string) ([]string, error) {
	if client == nil {
		return nil, fmt.Errorf("chaos: kubernetes client is required")
	}
	if namespace == "" {
		return nil, fmt.Errorf("chaos: namespace is required")
	}

	policies, err := client.NetworkingV1().NetworkPolicies(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("chaos: list partition policies: %w", err)
	}

	var active []string
	for _, p := range policies.Items {
		scenario := p.Labels[ChaosLabelScenario]
		if scenario == "" {
			continue
		}
		active = append(active, scenario)
	}
	return active, nil
}

func ensurePartitionSlot(ctx context.Context, client kubernetes.Interface, namespace, scenario string) error {
	active, err := ActivePartitionScenarios(ctx, client, namespace)
	if err != nil {
		return err
	}
	for _, existing := range active {
		if existing == scenario {
			return nil
		}
	}
	if len(active) > 0 {
		return ErrPartitionAlreadyActive
	}
	return nil
}

func partitionPolicyName(scenario string) string {
	return chaosPolicyNamePrefix + scenario
}
