package chaos

import (
	"context"
	"errors"
	"time"

	"k8s.io/client-go/kubernetes"
)

// ErrPartitionAlreadyActive is returned when a namespace already has a chaos partition policy.
var ErrPartitionAlreadyActive = errors.New("chaos: partition already active")

// DefaultPartitionTTL is how long a partition policy stays before C4 auto-revert removes it.
const DefaultPartitionTTL = 15 * time.Minute

// ActivePartitionScenarios lists scenario ids with an active partition policy in the namespace.
func ActivePartitionScenarios(ctx context.Context, client kubernetes.Interface, namespace string) ([]string, error) {
	partitions, err := ListActivePartitions(ctx, client, namespace)
	if err != nil {
		return nil, err
	}
	active := make([]string, 0, len(partitions))
	for _, part := range partitions {
		active = append(active, part.Scenario)
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
