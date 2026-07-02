package metrics

import (
	"context"
	"errors"

	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
