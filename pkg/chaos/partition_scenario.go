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

// PartitionScenario applies a reversible partition. stall_replica blackholes replica
// primary_conninfo; split_* scenarios still use an ingress NetworkPolicy.
type PartitionScenario struct {
	Client   kubernetes.Interface
	Scenario string
	// SetReplicaPrimaryHost rewrites overlay primary_conninfo and reloads. Nil uses live pod exec.
	SetReplicaPrimaryHost setReplicaPrimaryHostFunc
	// DropReplication terminates the primary walsender for stall_replica. Nil uses live primary admin.
	DropReplication dropReplicationFunc
	// TargetPod is the replica to stall. Empty picks the first running standby.
	TargetPod string
}

// Apply records partition state when the namespace slot is free.
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
	if s.Scenario == scenarioStallReplica {
		if err := s.applyStallReplica(ctx, namespace, design, targetComponent); err != nil {
			return err
		}
		slog.Info("chaos partition applied", "namespace", namespace, "scenario", s.Scenario, "target", targetComponent)
		return nil
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
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("chaos: create partition policy: %w", err)
	}
	created := err == nil
	if created {
		slog.Info("chaos partition applied", "namespace", namespace, "scenario", s.Scenario, "target", targetComponent)
	}
	return nil
}

// Revert restores stall conninfo or deletes the partition NetworkPolicy.
func (s *PartitionScenario) Revert(ctx context.Context, namespace string, _ deployer.DeploymentDesign, _ string) error {
	if s == nil || s.Client == nil {
		return fmt.Errorf("chaos: kubernetes client is required")
	}
	if s.Scenario == "" {
		return fmt.Errorf("chaos: partition scenario is required")
	}
	if s.Scenario == scenarioStallReplica {
		if err := s.revertStallReplica(ctx, namespace); err != nil {
			return err
		}
		slog.Info("chaos partition reverted", "namespace", namespace, "scenario", s.Scenario)
		return nil
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
		return splitAppStoreDescription(targetComponent)
	default:
		return fmt.Sprintf("Partition %s", targetComponent)
	}
}

func (s *PartitionScenario) buildInput(ctx context.Context, namespace, targetComponent string, design deployer.DeploymentDesign, expiresAt time.Time) (PartitionPolicyInput, error) {
	switch s.Scenario {
	case scenarioStallReplica:
		return buildStallReplicaInput(ctx, s.Client, namespace, targetComponent, design, expiresAt, s.TargetPod)
	case scenarioSplitPrimaryReplica:
		return buildSplitPrimaryReplicaInput(namespace, targetComponent, design, expiresAt)
	case scenarioSplitAppStore:
		return buildSplitAppStoreInput(namespace, targetComponent, design, expiresAt)
	default:
		return PartitionPolicyInput{}, fmt.Errorf("chaos: unknown partition scenario %q", s.Scenario)
	}
}

func buildStallReplicaInput(ctx context.Context, client kubernetes.Interface, namespace, targetComponent string, design deployer.DeploymentDesign, expiresAt time.Time, requestedPod string) (PartitionPolicyInput, error) {
	if targetComponent != "db" {
		return PartitionPolicyInput{}, fmt.Errorf("chaos: %s only supports targetComponent \"db\"", scenarioStallReplica)
	}
	if err := requireDBStreamingReplicas(design); err != nil {
		return PartitionPolicyInput{}, err
	}
	podName, err := resolveStallReplicaPod(ctx, client, namespace, requestedPod)
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

func splitAppStoreDescription(targetComponent string) string {
	switch targetComponent {
	case "cache":
		return "Block app-server from cache. Workers still reach cache."
	case "db":
		return "Block app-server from the db primary. Workers and read replicas still reach it."
	default:
		return fmt.Sprintf("Block app-server from %s", targetComponent)
	}
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
	port, err := splitAppStorePort(targetComponent, design)
	if err != nil {
		return PartitionPolicyInput{}, err
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

func splitAppStorePort(targetComponent string, design deployer.DeploymentDesign) (int32, error) {
	switch targetComponent {
	case "db":
		return postgresPartitionPort, nil
	case "cache":
		topo := deployer.StorytellingTopologyFromDesign(&design, design.Template)
		engine := strings.ToLower(strings.TrimSpace(topo.CacheEngine))
		if engine == "" {
			engine = "redis"
		}
		switch engine {
		case "memcached":
			return memcachedPartitionPort(), nil
		case "redis":
			return redisPartitionPort, nil
		default:
			port, ok := deployer.ListenPort(engine)
			if !ok {
				return 0, fmt.Errorf("chaos: split_app_store has no port for cache engine %q", engine)
			}
			return port, nil
		}
	default:
		return 0, fmt.Errorf("chaos: %s only supports targetComponent \"db\" or \"cache\"", scenarioSplitAppStore)
	}
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

func runningStandbyReplicaPods(ctx context.Context, client kubernetes.Interface, namespace string) ([]string, error) {
	writerPod, _ := deployer.PostgresWritePod(ctx, client, namespace)
	pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector(postgresReplicaAppLabel),
	})
	if err != nil {
		return nil, fmt.Errorf("chaos: list replica pods: %w", err)
	}
	var names []string
	for _, pod := range pods.Items {
		if pod.DeletionTimestamp != nil {
			continue
		}
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}
		if writerPod != "" && pod.Name == writerPod {
			continue
		}
		names = append(names, pod.Name)
	}
	sort.Strings(names)
	return names, nil
}

func resolveStallReplicaPod(ctx context.Context, client kubernetes.Interface, namespace, requestedPod string) (string, error) {
	requestedPod = strings.TrimSpace(requestedPod)
	names, err := runningStandbyReplicaPods(ctx, client, namespace)
	if err != nil {
		return "", err
	}
	if requestedPod == "" {
		if len(names) == 0 {
			return "", fmt.Errorf("chaos: no running postgres replica pod")
		}
		return names[0], nil
	}
	if err := validateReplicaPodName(requestedPod); err != nil {
		return "", fmt.Errorf("%w: invalid replica pod name %q", ErrInvalidStallTarget, requestedPod)
	}
	for _, name := range names {
		if name == requestedPod {
			return name, nil
		}
	}
	return "", fmt.Errorf("%w: replica pod %q is not a running standby", ErrInvalidStallTarget, requestedPod)
}
