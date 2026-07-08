package metrics

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
)

func metav1ObjectMeta(name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: name}
}

func podMetricsFixture(cpuMilli int64, memBytes int64) *metricsv1beta1.PodMetricsList {
	return &metricsv1beta1.PodMetricsList{
		Items: []metricsv1beta1.PodMetrics{{
			ObjectMeta: metav1ObjectMeta("postgres-abc123"),
			Containers: []metricsv1beta1.ContainerMetrics{
				{
					Name: "postgres-exporter",
					Usage: corev1.ResourceList{
						corev1.ResourceCPU:    *resource.NewMilliQuantity(6, resource.DecimalSI),
						corev1.ResourceMemory: *resource.NewQuantity(7<<20, resource.BinarySI),
					},
				},
				{
					Name: "postgres",
					Usage: corev1.ResourceList{
						corev1.ResourceCPU:    *resource.NewMilliQuantity(cpuMilli, resource.DecimalSI),
						corev1.ResourceMemory: *resource.NewQuantity(memBytes, resource.BinarySI),
					},
				},
			},
		}},
	}
}

func appServerPodMetricsFixture() *metricsv1beta1.PodMetricsList {
	return &metricsv1beta1.PodMetricsList{
		Items: []metricsv1beta1.PodMetrics{
			{
				ObjectMeta: metav1ObjectMeta("app-server-abc"),
				Containers: []metricsv1beta1.ContainerMetrics{{
					Name: "app-server",
					Usage: corev1.ResourceList{
						corev1.ResourceMemory: *resource.NewQuantity(128<<20, resource.BinarySI),
					},
				}},
			},
			{
				ObjectMeta: metav1ObjectMeta("app-server-def"),
				Containers: []metricsv1beta1.ContainerMetrics{{
					Name: "app-server",
					Usage: corev1.ResourceList{
						corev1.ResourceMemory: *resource.NewQuantity(256<<20, resource.BinarySI),
					},
				}},
			},
		},
	}
}
