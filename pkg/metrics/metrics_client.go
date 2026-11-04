package metrics

import (
	"context"
	"errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"
)

// PodMetricsProvider lists pod resource usage from metrics.k8s.io (same source as kubectl top).
type PodMetricsProvider interface {
	ListPodMetrics(ctx context.Context, namespace string) (*metricsv1beta1.PodMetricsList, error)
}

// MetricsClientAdapter wraps the generated metrics clientset.
type MetricsClientAdapter struct {
	Client metricsclient.Interface
}

// ListPodMetrics implements PodMetricsProvider.
func (a *MetricsClientAdapter) ListPodMetrics(ctx context.Context, namespace string) (*metricsv1beta1.PodMetricsList, error) {
	if a == nil || a.Client == nil {
		return nil, errors.New("metrics client not configured")
	}
	return a.Client.MetricsV1beta1().PodMetricses(namespace).List(ctx, metav1.ListOptions{})
}

// ListAllPodMetrics lists pod metrics across all namespaces (one list per cluster refresh).
func (a *MetricsClientAdapter) ListAllPodMetrics(ctx context.Context) (*metricsv1beta1.PodMetricsList, error) {
	if a == nil || a.Client == nil {
		return nil, errors.New("metrics client not configured")
	}
	return a.Client.MetricsV1beta1().PodMetricses(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
}
