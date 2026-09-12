package chaos

import (
	"context"
	"fmt"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// ActivePartition describes a chaos partition NetworkPolicy in a deployment namespace.
type ActivePartition struct {
	Scenario        string
	TargetComponent string
	ExpiresAt       time.Time
}

// ListActivePartitions returns labeled chaos partition policies in the namespace.
func ListActivePartitions(ctx context.Context, client kubernetes.Interface, namespace string) ([]ActivePartition, error) {
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

	var active []ActivePartition
	for _, p := range policies.Items {
		scenario := p.Labels[ChaosLabelScenario]
		if scenario == "" {
			continue
		}
		expiresAt, _ := time.Parse(time.RFC3339, p.Annotations[ChaosAnnotationExpiresAt])
		active = append(active, ActivePartition{
			Scenario:        scenario,
			TargetComponent: p.Labels[ChaosLabelTarget],
			ExpiresAt:       expiresAt,
		})
	}
	return active, nil
}

// HealPartitions deletes all active chaos partition policies in the namespace.
func HealPartitions(ctx context.Context, client kubernetes.Interface, namespace string) ([]ActivePartition, error) {
	active, err := ListActivePartitions(ctx, client, namespace)
	if err != nil {
		return nil, err
	}
	if len(active) == 0 {
		return nil, nil
	}

	healed := make([]ActivePartition, 0, len(active))
	for _, part := range active {
		scenario := &PartitionScenario{Client: client, Scenario: part.Scenario}
		if err := scenario.Revert(ctx, namespace, deployer.DeploymentDesign{}, ""); err != nil {
			return healed, err
		}
		healed = append(healed, part)
	}
	return healed, nil
}

// ExpireDuePartitions heals partition policies whose expires-at is in the past.
func ExpireDuePartitions(ctx context.Context, client kubernetes.Interface, namespace string, now time.Time) ([]ActivePartition, error) {
	active, err := ListActivePartitions(ctx, client, namespace)
	if err != nil {
		return nil, err
	}
	if len(active) == 0 {
		return nil, nil
	}

	var due []ActivePartition
	for _, part := range active {
		if !part.ExpiresAt.IsZero() && !part.ExpiresAt.After(now) {
			due = append(due, part)
		}
	}
	if len(due) == 0 {
		return nil, nil
	}

	healed := make([]ActivePartition, 0, len(due))
	for _, part := range due {
		scenario := &PartitionScenario{Client: client, Scenario: part.Scenario}
		if err := scenario.Revert(ctx, namespace, deployer.DeploymentDesign{}, ""); err != nil {
			return healed, err
		}
		healed = append(healed, part)
	}
	return healed, nil
}
