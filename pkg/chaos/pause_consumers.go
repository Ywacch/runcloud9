package chaos

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	scenarioPauseConsumers      = "pause_consumers"
	pauseConsumersConfigMapName = "c9-chaos-pause-consumers"
	pauseConsumersReplicasKey   = "workerReplicas"
	workerDeploymentName        = "worker"
)

// PauseConsumersScenario scales worker deployments to zero and restores on heal.
type PauseConsumersScenario struct {
	Client kubernetes.Interface
}

// Apply records prior replica count and scales workers to zero.
func (s *PauseConsumersScenario) Apply(ctx context.Context, namespace string, design deployer.DeploymentDesign, targetComponent string) error {
	if s == nil || s.Client == nil {
		return fmt.Errorf("chaos: kubernetes client is required")
	}
	if targetComponent != "mq" {
		return fmt.Errorf("pause_consumers only supports targetComponent %q", "mq")
	}
	if err := ensurePartitionSlot(ctx, s.Client, namespace, scenarioPauseConsumers); err != nil {
		return err
	}

	dep, err := s.Client.AppsV1().Deployments(namespace).Get(ctx, workerDeploymentName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("chaos: get worker deployment: %w", err)
	}
	replicas := int32(1)
	if dep.Spec.Replicas != nil {
		replicas = *dep.Spec.Replicas
	}

	zero := int32(0)
	dep.Spec.Replicas = &zero
	if _, err := s.Client.AppsV1().Deployments(namespace).Update(ctx, dep, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("chaos: scale worker to zero: %w", err)
	}

	expires := time.Now().Add(DefaultPartitionTTL)
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: pauseConsumersConfigMapName,
			Labels: map[string]string{
				ChaosLabelScenario: scenarioPauseConsumers,
				ChaosLabelTarget:   targetComponent,
			},
			Annotations: map[string]string{
				ChaosAnnotationExpiresAt: expires.UTC().Format(time.RFC3339),
			},
		},
		Data: map[string]string{
			pauseConsumersReplicasKey: strconv.FormatInt(int64(replicas), 10),
		},
	}
	if _, err := s.Client.CoreV1().ConfigMaps(namespace).Create(ctx, cm, metav1.CreateOptions{}); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("chaos: record pause_consumers state: %w", err)
		}
	}
	slog.Info("chaos pause_consumers applied", "namespace", namespace, "priorReplicas", replicas)
	return nil
}

// Revert restores the worker replica count and deletes state.
func (s *PauseConsumersScenario) Revert(ctx context.Context, namespace string, _ deployer.DeploymentDesign, _ string) error {
	if s == nil || s.Client == nil {
		return fmt.Errorf("chaos: kubernetes client is required")
	}
	cm, err := s.Client.CoreV1().ConfigMaps(namespace).Get(ctx, pauseConsumersConfigMapName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("chaos: read pause_consumers state: %w", err)
	}
	replicas := int32(1)
	if raw := cm.Data[pauseConsumersReplicasKey]; raw != "" {
		n, parseErr := strconv.ParseInt(raw, 10, 32)
		if parseErr == nil && n >= 0 {
			replicas = int32(n)
		}
	}
	if replicas < 1 {
		replicas = 1
	}

	if err := scaleWorkerDeployment(ctx, s.Client, namespace, replicas); err != nil {
		return err
	}
	if err := s.Client.CoreV1().ConfigMaps(namespace).Delete(ctx, pauseConsumersConfigMapName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("chaos: delete pause_consumers state: %w", err)
	}
	slog.Info("chaos pause_consumers reverted", "namespace", namespace, "replicas", replicas)
	return nil
}

func scaleWorkerDeployment(ctx context.Context, client kubernetes.Interface, namespace string, replicas int32) error {
	dep, err := client.AppsV1().Deployments(namespace).Get(ctx, workerDeploymentName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("chaos: get worker deployment: %w", err)
	}
	dep.Spec.Replicas = &replicas
	_, err = client.AppsV1().Deployments(namespace).Update(ctx, dep, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("chaos: restore worker replicas: %w", err)
	}
	return nil
}

// Stateful reports that pause_consumers leaves cluster state until heal.
func (s *PauseConsumersScenario) Stateful() bool { return true }

// Description returns UI copy for the scenario.
func (s *PauseConsumersScenario) Description(targetComponent string) string {
	_ = targetComponent
	return "Pause consumers — scale workers to zero so queue depth grows"
}
