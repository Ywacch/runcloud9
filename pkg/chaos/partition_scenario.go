package chaos

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	scenarioStallReplica        = "stall_replica"
	scenarioSplitPrimaryReplica = "split_primary_replica"
	scenarioSplitAppStore       = "split_app_store"
	postgresReplicaAppLabel     = "postgres-replica"
	appServerAppLabel           = "app-server"
	postgresPrimaryAppLabel     = "postgres"
)

// PartitionScenario applies a reversible ingress partition via NetworkPolicy.
type PartitionScenario struct {
	Client   kubernetes.Interface
	Scenario string
}

// Apply creates the partition NetworkPolicy when the namespace slot is free.
func (s *PartitionScenario) Apply(ctx context.Context, namespace string, design deployer.DeploymentDesign, targetComponent string) error {
	if s == nil || s.Client == nil {
		return fmt.Errorf("chaos: kubernetes client is required")
	}
	if s.Scenario == "" {
		return fmt.Errorf("chaos: partition scenario is required")
	}
	if err := ensurePartitionSlot(ctx, s.Client, namespace, s.Scenario); err != nil {
		return err
	}

	in, err := s.buildInput(ctx, namespace, targetComponent, design, time.Now().Add(DefaultPartitionTTL))
	if err != nil {
		return err
	}
	policy, err := BuildPartitionNetworkPolicy(in)
	if err != nil {
		return err
	}

	_, err = s.Client.NetworkingV1().NetworkPolicies(namespace).Create(ctx, policy, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("chaos: create partition policy: %w", err)
	}
	slog.Info("chaos partition applied", "namespace", namespace, "scenario", s.Scenario, "target", targetComponent)
	return nil
}

// Revert deletes the partition NetworkPolicy for this scenario.
func (s *PartitionScenario) Revert(ctx context.Context, namespace string, _ deployer.DeploymentDesign, _ string) error {
	if s == nil || s.Client == nil {
		return fmt.Errorf("chaos: kubernetes client is required")
	}
	if s.Scenario == "" {
		return fmt.Errorf("chaos: partition scenario is required")
	}

	err := s.Client.NetworkingV1().NetworkPolicies(namespace).Delete(ctx, partitionPolicyName(s.Scenario), metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("chaos: delete partition policy: %w", err)
	}
	slog.Info("chaos partition reverted", "namespace", namespace, "scenario", s.Scenario)
	return nil
}

// Stateful reports that partition scenarios leave cluster state until Revert runs.
func (s *PartitionScenario) Stateful() bool { return true }

// Description returns UI copy for the scenario.
func (s *PartitionScenario) Description(targetComponent string) string {
	switch s.Scenario {
	case scenarioStallReplica:
		return fmt.Sprintf("Stall one %s replica — replication lag climbs", targetComponent)
	case scenarioSplitPrimaryReplica:
		return fmt.Sprintf("Split %s primary from replicas", targetComponent)
	case scenarioSplitAppStore:
		return fmt.Sprintf("Split app from %s", targetComponent)
	default:
		return fmt.Sprintf("Partition %s", targetComponent)
	}
}

func (s *PartitionScenario) buildInput(ctx context.Context, namespace, targetComponent string, design deployer.DeploymentDesign, expiresAt time.Time) (PartitionPolicyInput, error) {
	switch s.Scenario {
	case scenarioStallReplica:
		return buildStallReplicaInput(ctx, s.Client, namespace, targetComponent, design, expiresAt)
	case scenarioSplitPrimaryReplica:
		return buildSplitPrimaryReplicaInput(namespace, targetComponent, design, expiresAt)
	case scenarioSplitAppStore:
		return buildSplitAppStoreInput(namespace, targetComponent, design, expiresAt)
	default:
		return PartitionPolicyInput{}, fmt.Errorf("chaos: unknown partition scenario %q", s.Scenario)
	}
}

func buildStallReplicaInput(ctx context.Context, client kubernetes.Interface, namespace, targetComponent string, design deployer.DeploymentDesign, expiresAt time.Time) (PartitionPolicyInput, error) {
	if targetComponent != "db" {
		return PartitionPolicyInput{}, fmt.Errorf("chaos: %s only supports targetComponent \"db\"", scenarioStallReplica)
	}
	if err := requireDBStreamingReplicas(design); err != nil {
		return PartitionPolicyInput{}, err
	}
	podName, err := firstRunningReplicaPod(ctx, client, namespace)
	if err != nil {
		return PartitionPolicyInput{}, err
	}
	return PartitionPolicyInput{
		Namespace:       namespace,
		Scenario:        scenarioStallReplica,
		TargetComponent: targetComponent,
		ExpiresAt:       expiresAt,
		DestAppLabel:    postgresPrimaryAppLabel,
		Port:            postgresPartitionPort,
		Exclude:         PartitionPeerExclusion{PodName: podName},
	}, nil
}

func buildSplitPrimaryReplicaInput(namespace, targetComponent string, design deployer.DeploymentDesign, expiresAt time.Time) (PartitionPolicyInput, error) {
	if targetComponent != "db" {
		return PartitionPolicyInput{}, fmt.Errorf("chaos: %s only supports targetComponent \"db\"", scenarioSplitPrimaryReplica)
	}
	if err := requireDBStreamingReplicas(design); err != nil {
		return PartitionPolicyInput{}, err
	}
	return PartitionPolicyInput{
		Namespace:       namespace,
		Scenario:        scenarioSplitPrimaryReplica,
		TargetComponent: targetComponent,
		ExpiresAt:       expiresAt,
		DestAppLabel:    postgresPrimaryAppLabel,
		Port:            postgresPartitionPort,
		Exclude:         PartitionPeerExclusion{AppLabel: postgresReplicaAppLabel},
	}, nil
}

func buildSplitAppStoreInput(namespace, targetComponent string, design deployer.DeploymentDesign, expiresAt time.Time) (PartitionPolicyInput, error) {
	switch targetComponent {
	case "db":
		if err := requireDBEngineSupported(design); err != nil {
			return PartitionPolicyInput{}, err
		}
	case "cache":
		topo := deployer.StorytellingTopologyFromDesign(&design, design.Template)
		if !topo.HasCache {
			return PartitionPolicyInput{}, fmt.Errorf("chaos: target component %q not found in deployment design", targetComponent)
		}
	default:
		return PartitionPolicyInput{}, fmt.Errorf("chaos: %s only supports targetComponent \"db\" or \"cache\"", scenarioSplitAppStore)
	}

	resolved, err := resolveTarget(targetComponent, design)
	if err != nil {
		return PartitionPolicyInput{}, err
	}
	port := postgresPartitionPort
	if targetComponent == "cache" {
		port = redisPartitionPort
	}
	return PartitionPolicyInput{
		Namespace:       namespace,
		Scenario:        scenarioSplitAppStore,
		TargetComponent: targetComponent,
		ExpiresAt:       expiresAt,
		DestAppLabel:    resolved.AppLabel,
		Port:            port,
		Exclude:         PartitionPeerExclusion{AppLabel: appServerAppLabel},
	}, nil
}

func requireDBStreamingReplicas(design deployer.DeploymentDesign) error {
	if err := requireDBEngineSupported(design); err != nil {
		return err
	}
	cfg, ok := deployer.DatabaseNodeConfig(design)
	if !ok {
		return fmt.Errorf("chaos: deployment design has no db node")
	}
	if deployer.ParseReadReplicaCount(cfg) < 1 {
		return fmt.Errorf("chaos: readReplicas must be at least 1")
	}
	return nil
}

func requireDBEngineSupported(design deployer.DeploymentDesign) error {
	topo := deployer.StorytellingTopologyFromDesign(&design, design.Template)
	engine := normalizeDBEngine(topo.DBEngine)
	if !topo.HasDB {
		return fmt.Errorf("chaos: deployment design has no db node")
	}
	if !deployer.DBEngineSupportsStreamingReplicas(engine) {
		return fmt.Errorf("chaos: db engine %q does not support streaming replicas", engine)
	}
	return nil
}

func normalizeDBEngine(engine string) string {
	engine = strings.ToLower(strings.TrimSpace(engine))
	if engine == "" || engine == "postgres" {
		return "postgresql"
	}
	return engine
}

func firstRunningReplicaPod(ctx context.Context, client kubernetes.Interface, namespace string) (string, error) {
	pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector(postgresReplicaAppLabel),
	})
	if err != nil {
		return "", fmt.Errorf("chaos: list replica pods: %w", err)
	}
	var names []string
	for _, pod := range pods.Items {
		if pod.DeletionTimestamp != nil {
			continue
		}
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}
		names = append(names, pod.Name)
	}
	if len(names) == 0 {
		return "", fmt.Errorf("chaos: no running postgres replica pod")
	}
	sort.Strings(names)
	return names[0], nil
}
