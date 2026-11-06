package chaos

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	// stallBlackholeHost is TEST-NET-1 (RFC 5737). It is not cluster DNS, so walreceiver
	// cannot reconnect after reload the way host=db does on this CNI.
	stallBlackholeHost  = "192.0.2.1"
	stallPrimaryHost    = "db"
	replicaAutoConfPath = "/var/lib/postgresql/data/postgresql.auto.conf"
)

// ErrInvalidStallTarget is returned when stall_replica cannot use the requested pod.
var ErrInvalidStallTarget = errors.New("chaos: invalid stall target")

var replicaPodNamePattern = regexp.MustCompile(`^postgres-replica(?:-[a-z]+)?-[0-9]+$`)

// setReplicaPrimaryHostFunc rewrites overlay primary_conninfo host and reloads the replica.
type setReplicaPrimaryHostFunc func(ctx context.Context, client kubernetes.Interface, namespace, replicaPod, host string) error

// dropReplicationFunc terminates the primary walsender for one replica application_name.
type dropReplicationFunc func(ctx context.Context, client kubernetes.Interface, namespace, applicationName string) error

func defaultDropReplication(ctx context.Context, client kubernetes.Interface, namespace, applicationName string) error {
	if err := validateReplicaPodName(applicationName); err != nil {
		return err
	}
	admin := deployer.NewPostgresPrimaryAdmin(client, namespace, nil)
	query := fmt.Sprintf(
		"SELECT pg_terminate_backend(pid) FROM pg_stat_replication WHERE application_name = '%s'",
		applicationName,
	)
	if _, err := admin.ExecSQL(ctx, query); err != nil {
		return fmt.Errorf("chaos: terminate replication for %s: %w", applicationName, err)
	}
	return nil
}

func defaultSetReplicaPrimaryHost(ctx context.Context, client kubernetes.Interface, namespace, replicaPod, host string) error {
	if err := validateReplicaPodName(replicaPod); err != nil {
		return err
	}
	if host != stallPrimaryHost && host != stallBlackholeHost {
		return fmt.Errorf("chaos: invalid stall primary host %q", host)
	}

	stdout, _, err := execInPod(ctx, client, namespace, replicaPod, postgresReplicaAppLabel, []string{"cat", replicaAutoConfPath})
	if err != nil {
		return fmt.Errorf("chaos: read replica auto.conf: %w", err)
	}
	rewritten, err := rewriteOverlayConninfo(stdout, replicaPod, host)
	if err != nil {
		return err
	}
	encoded := base64.StdEncoding.EncodeToString([]byte(rewritten))
	writeCmd := fmt.Sprintf("printf '%%s' '%s' | base64 -d > %s", encoded, replicaAutoConfPath)
	if _, _, err := execInPod(ctx, client, namespace, replicaPod, postgresReplicaAppLabel, []string{"sh", "-c", writeCmd}); err != nil {
		return fmt.Errorf("chaos: write replica auto.conf: %w", err)
	}

	admin := deployer.NewPostgresPodAdmin(client, namespace, replicaPod, postgresReplicaAppLabel)
	if _, err := admin.ExecSQL(ctx, "SELECT pg_reload_conf()"); err != nil {
		return fmt.Errorf("chaos: reload replica conf: %w", err)
	}
	return nil
}

func validateReplicaPodName(name string) error {
	if !replicaPodNamePattern.MatchString(name) {
		return fmt.Errorf("chaos: invalid replica pod name %q", name)
	}
	return nil
}

func isFakeKubernetesClient(client kubernetes.Interface) bool {
	if client == nil {
		return false
	}
	return strings.Contains(fmt.Sprintf("%T", client), "fake.Clientset")
}

func noopSetReplicaPrimaryHost(context.Context, kubernetes.Interface, string, string, string) error {
	return nil
}

func noopDropReplication(context.Context, kubernetes.Interface, string, string) error {
	return nil
}

func overlayPrimaryConninfo(host, replicaPod string) string {
	return fmt.Sprintf(
		"host=%s port=5432 user=syscrack application_name=%s sslmode=disable",
		host, replicaPod,
	)
}

// rewriteOverlayConninfo replaces the last primary_conninfo line for replicaPod.
// Leaves primary_slot_name and the pg_basebackup -R conninfo line unchanged.
func rewriteOverlayConninfo(autoConf, replicaPod, host string) (string, error) {
	if err := validateReplicaPodName(replicaPod); err != nil {
		return "", err
	}
	if host != stallPrimaryHost && host != stallBlackholeHost {
		return "", fmt.Errorf("chaos: invalid stall primary host %q", host)
	}

	overlayNeedle := "application_name=" + replicaPod
	lines := strings.Split(autoConf, "\n")
	last := -1
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "primary_conninfo") {
			continue
		}
		if !strings.Contains(trimmed, overlayNeedle) {
			continue
		}
		last = i
	}
	if last < 0 {
		return "", fmt.Errorf("chaos: overlay primary_conninfo for %s not found", replicaPod)
	}
	lines[last] = fmt.Sprintf("primary_conninfo = '%s'", overlayPrimaryConninfo(host, replicaPod))
	return strings.Join(lines, "\n"), nil
}

func (s *PartitionScenario) setReplicaHost(ctx context.Context, namespace, replicaPod, host string) error {
	setHost := s.SetReplicaPrimaryHost
	if setHost == nil {
		if isFakeKubernetesClient(s.Client) {
			setHost = noopSetReplicaPrimaryHost
		} else {
			setHost = defaultSetReplicaPrimaryHost
		}
	}
	return setHost(ctx, s.Client, namespace, replicaPod, host)
}

func (s *PartitionScenario) dropStalledReplica(ctx context.Context, namespace, replicaPod string) error {
	if err := validateReplicaPodName(replicaPod); err != nil {
		return err
	}
	drop := s.DropReplication
	if drop == nil {
		if isFakeKubernetesClient(s.Client) {
			drop = noopDropReplication
		} else {
			drop = defaultDropReplication
		}
	}
	return drop(ctx, s.Client, namespace, replicaPod)
}

func (s *PartitionScenario) applyStallReplica(ctx context.Context, namespace string, design deployer.DeploymentDesign, targetComponent string) error {
	in, err := buildStallReplicaInput(ctx, s.Client, namespace, targetComponent, design, time.Now().Add(DefaultPartitionTTL), s.TargetPod)
	if err != nil {
		return err
	}
	replicaPod := in.Exclude.PodName
	s.TargetPod = replicaPod
	if err := upsertStallState(ctx, s.Client, namespace, replicaPod, targetComponent, in.ExpiresAt); err != nil {
		return err
	}
	if err := s.setReplicaHost(ctx, namespace, replicaPod, stallBlackholeHost); err != nil {
		return err
	}
	if err := s.dropStalledReplica(ctx, namespace, replicaPod); err != nil {
		return err
	}
	return nil
}

func (s *PartitionScenario) revertStallReplica(ctx context.Context, namespace string) error {
	cm, err := s.Client.CoreV1().ConfigMaps(namespace).Get(ctx, partitionPolicyName(scenarioStallReplica), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return s.deleteStallNetworkPolicy(ctx, namespace)
	}
	if err != nil {
		return fmt.Errorf("chaos: get stall state: %w", err)
	}

	replicaPod := cm.Annotations[ChaosAnnotationReplicaPod]
	if replicaPod == "" {
		replicaPod = cm.Data["replicaPod"]
	}
	if replicaPod != "" {
		if err := s.setReplicaHost(ctx, namespace, replicaPod, stallPrimaryHost); err != nil {
			return err
		}
	}

	err = s.Client.CoreV1().ConfigMaps(namespace).Delete(ctx, cm.Name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("chaos: delete stall state: %w", err)
	}
	return s.deleteStallNetworkPolicy(ctx, namespace)
}

func (s *PartitionScenario) deleteStallNetworkPolicy(ctx context.Context, namespace string) error {
	err := s.Client.NetworkingV1().NetworkPolicies(namespace).Delete(ctx, partitionPolicyName(scenarioStallReplica), metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("chaos: delete leftover stall policy: %w", err)
	}
	return nil
}

func upsertStallState(ctx context.Context, client kubernetes.Interface, namespace, replicaPod, targetComponent string, expiresAt time.Time) error {
	cm := buildStallStateConfigMap(namespace, replicaPod, targetComponent, expiresAt)
	_, err := client.CoreV1().ConfigMaps(namespace).Create(ctx, cm, metav1.CreateOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("chaos: create stall state: %w", err)
	}
	existing, getErr := client.CoreV1().ConfigMaps(namespace).Get(ctx, cm.Name, metav1.GetOptions{})
	if getErr != nil {
		return fmt.Errorf("chaos: get stall state: %w", getErr)
	}
	cm.ResourceVersion = existing.ResourceVersion
	if _, err := client.CoreV1().ConfigMaps(namespace).Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("chaos: update stall state: %w", err)
	}
	return nil
}

func buildStallStateConfigMap(namespace, replicaPod, targetComponent string, expiresAt time.Time) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      partitionPolicyName(scenarioStallReplica),
			Namespace: namespace,
			Labels: map[string]string{
				ChaosLabelScenario: scenarioStallReplica,
				ChaosLabelTarget:   targetComponent,
			},
			Annotations: map[string]string{
				ChaosAnnotationExpiresAt:  expiresAt.UTC().Format(time.RFC3339),
				ChaosAnnotationReplicaPod: replicaPod,
			},
		},
		Data: map[string]string{
			"replicaPod":  replicaPod,
			"primaryHost": stallBlackholeHost,
		},
	}
}
