package chaos

import (
	"context"
	"fmt"
	"log/slog"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func labelSelector(appLabel string) string {
	return fmt.Sprintf("app=%s", appLabel)
}

func findRunningPod(ctx context.Context, client kubernetes.Interface, namespace, appLabel string) (string, error) {
	pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector(appLabel),
	})
	if err != nil {
		return "", fmt.Errorf("list pods app=%s: %w", appLabel, err)
	}

	for _, pod := range pods.Items {
		if pod.DeletionTimestamp != nil {
			continue
		}
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}
		return pod.Name, nil
	}

	return "", fmt.Errorf("no running pod for component with app=%s", appLabel)
}

func deletePod(ctx context.Context, client kubernetes.Interface, namespace, podName string) error {
	propagation := metav1.DeletePropagationBackground
	if err := client.CoreV1().Pods(namespace).Delete(ctx, podName, metav1.DeleteOptions{
		PropagationPolicy: &propagation,
	}); err != nil {
		return fmt.Errorf("delete pod %s: %w", podName, err)
	}
	slog.Info("chaos pod kill", "namespace", namespace, "pod", podName)
	return nil
}
