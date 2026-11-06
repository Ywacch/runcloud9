package metrics

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// inventory is the clusterstate read path for Collect inventory helpers.
// All methods are namespace-scoped and must not call the API server.
type inventory interface {
	Pods(namespace string) ([]*corev1.Pod, error)
	Pod(namespace, name string) (*corev1.Pod, error)
	Deployments(namespace string) ([]*appsv1.Deployment, error)
	StatefulSets(namespace string) ([]*appsv1.StatefulSet, error)
	HorizontalPodAutoscalers(namespace string) ([]*autoscalingv2.HorizontalPodAutoscaler, error)
}

// SetInventory installs the cache-backed inventory source. Nil restores clientset calls.
func (c *MetricsCollector) SetInventory(inv inventory) {
	if c == nil {
		return
	}
	c.inv = inv
}

func copyPodsWithDeploymentLabel(ptrs []*corev1.Pod, namespace string) []corev1.Pod {
	out := make([]corev1.Pod, 0, len(ptrs))
	for _, p := range ptrs {
		if p == nil {
			continue
		}
		if p.Labels["deployment"] != namespace {
			continue
		}
		out = append(out, *p.DeepCopy())
	}
	return out
}

func copyPodsWithAppAndDeployment(ptrs []*corev1.Pod, app, namespace string) []corev1.Pod {
	out := make([]corev1.Pod, 0, len(ptrs))
	for _, p := range ptrs {
		if p == nil {
			continue
		}
		if p.Labels["app"] != app || p.Labels["deployment"] != namespace {
			continue
		}
		out = append(out, *p.DeepCopy())
	}
	return out
}

func deploymentItems(deps []*appsv1.Deployment) []appsv1.Deployment {
	out := make([]appsv1.Deployment, 0, len(deps))
	for _, d := range deps {
		if d == nil {
			continue
		}
		out = append(out, *d.DeepCopy())
	}
	return out
}

func (c *MetricsCollector) listDeploymentPods(ctx context.Context, namespace string) ([]corev1.Pod, error) {
	if c == nil || namespace == "" {
		return nil, nil
	}
	if c.inv != nil {
		ptrs, err := c.inv.Pods(namespace)
		if err != nil {
			return nil, err
		}
		return copyPodsWithDeploymentLabel(ptrs, namespace), nil
	}
	if c.clientset == nil {
		return nil, nil
	}
	list, err := c.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "deployment=" + namespace,
	})
	if err != nil {
		return nil, err
	}
	items := make([]corev1.Pod, len(list.Items))
	copy(items, list.Items)
	return items, nil
}

func (c *MetricsCollector) loadSessionHPAs(ctx context.Context, ns string) ([]autoscalingv2.HorizontalPodAutoscaler, bool) {
	if c == nil {
		return nil, false
	}
	if c.inv != nil {
		ptrs, err := c.inv.HorizontalPodAutoscalers(ns)
		if err != nil {
			return nil, false
		}
		out := make([]autoscalingv2.HorizontalPodAutoscaler, 0, len(ptrs))
		for _, h := range ptrs {
			if h == nil {
				continue
			}
			out = append(out, *h.DeepCopy())
		}
		return out, true
	}
	if c.clientset == nil {
		return nil, false
	}
	hpaList, err := c.clientset.AutoscalingV2().HorizontalPodAutoscalers(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, false
	}
	return hpaList.Items, true
}
