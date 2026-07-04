package deployer

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const namespaceResourceQuotaName = "cloud9-deployment-quota"

// EnsureNamespaceResourceQuota creates the deployment namespace (if needed) and applies
// a ResourceQuota sized to the design's requested workloads plus sidecar/proxy overhead.
func EnsureNamespaceResourceQuota(
	ctx context.Context,
	clientset kubernetes.Interface,
	namespace string,
	design DeploymentDesign,
	userTier string,
) error {
	if clientset == nil {
		return fmt.Errorf("ensure namespace resource quota: kubernetes client is nil")
	}
	if namespace == "" {
		return fmt.Errorf("ensure namespace resource quota: namespace is required")
	}

	_, err := clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: namespace},
	}, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("create namespace: %w", err)
	}

	limits := QuotaResourceLimits(design, userTier)
	hard := corev1.ResourceList{
		corev1.ResourceLimitsCPU:      limits.CPU,
		corev1.ResourceLimitsMemory:    limits.Memory,
		corev1.ResourceRequestsCPU:    limits.CPU,
		corev1.ResourceRequestsMemory: limits.Memory,
	}

	rq := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{
			Name:      namespaceResourceQuotaName,
			Namespace: namespace,
		},
		Spec: corev1.ResourceQuotaSpec{Hard: hard},
	}
	if _, err := clientset.CoreV1().ResourceQuotas(namespace).Create(ctx, rq, metav1.CreateOptions{}); err != nil {
		if errors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("create resource quota: %w", err)
	}
	return nil
}
