package chaos

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// ActivePartition describes a chaos partition in a deployment namespace.
type ActivePartition struct {
	Scenario        string
	TargetComponent string
	ExpiresAt       time.Time
	ReplicaPod      string
}

// ListActivePartitions returns labeled chaos partition policies and stall state ConfigMaps.
func ListActivePartitions(ctx context.Context, client kubernetes.Interface, namespace string) ([]ActivePartition, error) {
	if client == nil {
		return nil, fmt.Errorf("chaos: kubernetes client is required")
	}
	if namespace == "" {
		return nil, fmt.Errorf("chaos: namespace is required")
	}

	byScenario := make(map[string]ActivePartition)

	policies, err := client.NetworkingV1().NetworkPolicies(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("chaos: list partition policies: %w", err)
	}
	for _, p := range policies.Items {
		part, ok := activePartitionFromMeta(p.Labels, p.Annotations)
		if !ok {
			continue
		}
		byScenario[part.Scenario] = part
	}

	cms, err := client.CoreV1().ConfigMaps(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("chaos: list partition state: %w", err)
	}
	for _, cm := range cms.Items {
		part, ok := activePartitionFromMeta(cm.Labels, cm.Annotations)
		if !ok {
			continue
		}
		if part.ReplicaPod == "" {
			part.ReplicaPod = cm.Data["replicaPod"]
		}
		byScenario[part.Scenario] = part
	}

	if len(byScenario) == 0 {
		return nil, nil
	}
	names := make([]string, 0, len(byScenario))
	for name := range byScenario {
		names = append(names, name)
	}
	sort.Strings(names)
	active := make([]ActivePartition, 0, len(names))
	for _, name := range names {
		active = append(active, byScenario[name])
	}
	return active, nil
}

func activePartitionFromMeta(labels, annotations map[string]string) (ActivePartition, bool) {
	if labels == nil {
		return ActivePartition{}, false
	}
	scenario := labels[ChaosLabelScenario]
	if scenario == "" {
		return ActivePartition{}, false
	}
	var expiresAt time.Time
	if annotations != nil {
		expiresAt, _ = time.Parse(time.RFC3339, annotations[ChaosAnnotationExpiresAt])
	}
	replicaPod := ""
	if annotations != nil {
		replicaPod = annotations[ChaosAnnotationReplicaPod]
	}
	return ActivePartition{
		Scenario:        scenario,
		TargetComponent: labels[ChaosLabelTarget],
		ExpiresAt:       expiresAt,
		ReplicaPod:      replicaPod,
	}, true
}

// HealPartitions reverts all active chaos partitions in the namespace.
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
		if err := revertActiveChaos(ctx, client, namespace, part); err != nil {
			return healed, err
		}
		healed = append(healed, part)
	}
	return healed, nil
}

func revertActiveChaos(ctx context.Context, client kubernetes.Interface, namespace string, part ActivePartition) error {
	switch part.Scenario {
	case scenarioPauseConsumers:
		s := &PauseConsumersScenario{Client: client}
		return s.Revert(ctx, namespace, deployer.DeploymentDesign{}, part.TargetComponent)
	default:
		scenario := &PartitionScenario{Client: client, Scenario: part.Scenario}
		return scenario.Revert(ctx, namespace, deployer.DeploymentDesign{}, "")
	}
}

// ExpireDuePartitions heals partitions whose expires-at is in the past.
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
		if err := revertActiveChaos(ctx, client, namespace, part); err != nil {
			return healed, err
		}
		healed = append(healed, part)
	}
	return healed, nil
}
