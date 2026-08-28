package metrics

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestMergePodMetricMaps(t *testing.T) {
	t.Parallel()
	names := []string{"app-server-abc", "app-server-def"}
	ready := map[string]bool{"app-server-abc": true, "app-server-def": false}
	rps := map[string]float64{"app-server-abc": 10, "app-server-def": 5}
	p50 := map[string]float64{"app-server-abc": 0.001}
	p95 := map[string]float64{"app-server-abc": 0.005}
	p99 := map[string]float64{"app-server-abc": 0.02}
	p50Session := map[string]float64{"app-server-def": 0.002}
	p95Session := map[string]float64{"app-server-def": 0.006}
	p99Session := map[string]float64{"app-server-def": 0.021}
	cpu := map[string]float64{"app-server-def": 0.1}
	errRate := map[string]float64{"app-server-abc": 0.1}
	totalErrs5xx := map[string]float64{"app-server-abc": 7, "app-server-def": 5}

	got := mergePodMetricMaps(
		names, ready,
		rps, p50, p95, p99,
		p50Session, p95Session, p99Session,
		cpu, errRate,
		map[string]float64{
			"app-server-abc": 16240,
			"app-server-def": 16105,
		},
		totalErrs5xx,
		map[string]int64{
			"app-server-abc": 134_217_728,
			"app-server-def": 268_435_456,
		},
	)
	if len(got) != 2 {
		t.Fatalf("len = %d want 2", len(got))
	}
	byName := make(map[string]ReplicaMetrics)
	for _, r := range got {
		byName[r.Name] = r
	}
	abc := byName["app-server-abc"]
	if abc.RequestRate != 10 || !abc.Ready {
		t.Fatalf("abc: %+v", abc)
	}
	if abc.TotalRequests != 16240 {
		t.Fatalf("abc totalRequests: want 16240, got %v", abc.TotalRequests)
	}
	if abc.P50Latency != 0.001 || abc.P95Latency != 0.005 || abc.P99Latency != 0.02 {
		t.Fatalf("abc latencies: p50=%v p95=%v p99=%v", abc.P50Latency, abc.P95Latency, abc.P99Latency)
	}
	if abc.TotalErrors9m != 7 {
		t.Fatalf("abc totalErrors9m: want 7, got %v", abc.TotalErrors9m)
	}
	if abc.MemoryBytes != 134_217_728 {
		t.Fatalf("abc memoryBytes: want 134217728, got %d", abc.MemoryBytes)
	}

	def := byName["app-server-def"]
	if def.CPUUsage != 0.1 || def.Ready {
		t.Fatalf("def: %+v", def)
	}
	if def.TotalRequests != 16105 {
		t.Fatalf("def totalRequests: want 16105, got %v", def.TotalRequests)
	}
	if def.P50Latency9m != 0.002 || def.P95Latency9m != 0.006 || def.P99Latency9m != 0.021 {
		t.Fatalf("def session latencies: p50=%v p95=%v p99=%v", def.P50Latency9m, def.P95Latency9m, def.P99Latency9m)
	}
	if def.TotalErrors9m != 5 {
		t.Fatalf("def totalErrors9m: want 5, got %v", def.TotalErrors9m)
	}
	if def.MemoryBytes != 268_435_456 {
		t.Fatalf("def memoryBytes: want 268435456, got %d", def.MemoryBytes)
	}
}

func TestListWorkerPods(t *testing.T) {
	t.Parallel()
	const ns = "ns-worker-list"
	desired := int32(2)
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "worker",
			Namespace: ns,
			Labels:    map[string]string{"app": "worker"},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &desired,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "worker", "deployment": ns}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "worker", "deployment": ns}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "worker", Image: "worker:v1"}}},
			},
		},
	}
	podA := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "worker-abc",
			Namespace: ns,
			Labels:    map[string]string{"app": "worker", "deployment": ns},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	podB := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "worker-def",
			Namespace: ns,
			Labels:    map[string]string{"app": "worker", "deployment": ns},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}},
		},
	}
	clientset := fake.NewSimpleClientset(deployment, podA, podB)
	c := NewMetricsCollector("", nil, clientset, nil, nil)

	names, ready, counts := c.listWorkerPods(context.Background(), ns)
	if len(names) != 2 {
		t.Fatalf("names len = %d want 2", len(names))
	}
	if !ready["worker-abc"] || ready["worker-def"] {
		t.Fatalf("ready map: %+v", ready)
	}
	if counts.Desired != 2 || counts.Ready != 1 {
		t.Fatalf("counts: %+v", counts)
	}
}
