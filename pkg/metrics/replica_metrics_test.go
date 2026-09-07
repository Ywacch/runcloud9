package metrics

import (
	"context"
	"slices"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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

func TestIsLiveInventoryPod(t *testing.T) {
	t.Parallel()
	deleted := metav1.Now()
	tests := []struct {
		name string
		pod  corev1.Pod
		want bool
	}{
		{
			name: "running ready",
			pod: corev1.Pod{
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
			},
			want: true,
		},
		{
			name: "pending",
			pod: corev1.Pod{
				Status: corev1.PodStatus{Phase: corev1.PodPending},
			},
			want: true,
		},
		{
			name: "empty phase treated as live",
			pod:  corev1.Pod{},
			want: true,
		},
		{
			name: "succeeded completed",
			pod: corev1.Pod{
				Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
			},
			want: false,
		},
		{
			name: "failed",
			pod: corev1.Pod{
				Status: corev1.PodStatus{Phase: corev1.PodFailed},
			},
			want: false,
		},
		{
			name: "running but deleting",
			pod: corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &deleted},
				Status:     corev1.PodStatus{Phase: corev1.PodRunning},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := isLiveInventoryPod(tt.pod); got != tt.want {
				t.Fatalf("isLiveInventoryPod = %v want %v", got, tt.want)
			}
		})
	}
}

func TestDeploymentNameMatchesApp(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, dep, app string
		want           bool
	}{
		{name: "exact app-server", dep: "app-server", app: "app-server", want: true},
		{name: "mixed size", dep: "app-server-small", app: "app-server", want: true},
		{name: "worker exact", dep: "worker", app: "worker", want: true},
		{name: "worker sized", dep: "worker-medium", app: "worker", want: true},
		{name: "cache skipped", dep: "cache", app: "app-server", want: false},
		{name: "prefix without hyphen", dep: "app-serverfoo", app: "app-server", want: false},
		{name: "worker not app-server", dep: "worker", app: "app-server", want: false},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := deploymentNameMatchesApp(tt.dep, tt.app); got != tt.want {
				t.Fatalf("deploymentNameMatchesApp(%q, %q) = %v want %v", tt.dep, tt.app, got, tt.want)
			}
		})
	}
}

func TestMergePodMetricMapsSkipsPromOnlyWhenInventoryPresent(t *testing.T) {
	t.Parallel()
	got := mergePodMetricMaps(
		[]string{"app-server-a", "app-server-b", "app-server-c"},
		map[string]bool{"app-server-a": true, "app-server-b": true, "app-server-c": true},
		map[string]float64{
			"app-server-a":      10,
			"app-server-b":      10,
			"app-server-c":      10,
			"app-server-zombie": 0,
		},
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
	)
	if len(got) != 3 {
		t.Fatalf("len = %d want 3 (zombie leftover Prom series)", len(got))
	}
	for _, row := range got {
		if row.Name == "app-server-zombie" {
			t.Fatalf("zombie row present: %+v", row)
		}
	}
}

func TestMergePodMetricMapsFallsBackToPromWhenInventoryEmpty(t *testing.T) {
	t.Parallel()
	got := mergePodMetricMaps(
		nil,
		map[string]bool{},
		map[string]float64{"app-server-a": 4},
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
	)
	if len(got) != 1 || got[0].Name != "app-server-a" || got[0].RequestRate != 4 {
		t.Fatalf("fallback rows: %+v", got)
	}
}

func TestMergePodMetricMapsEmptyInventoryOmitsProm(t *testing.T) {
	t.Parallel()
	got := mergePodMetricMaps(
		[]string{},
		map[string]bool{},
		map[string]float64{"app-server-zombie": 0},
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
	)
	if len(got) != 0 {
		t.Fatalf("empty inventory should omit Prom leftovers: %+v", got)
	}
}

func TestListAppServerPods(t *testing.T) {
	t.Parallel()
	const ns = "cloud9-url-shortener-cd9fbd72-0cf5d0"
	tests := []struct {
		name      string
		objects   []runtime.Object
		wantNames []string
		wantReady map[string]bool
		desired   int32
		ready     int32
	}{
		{
			name: "completed corpse plus three running unlabeled deployment",
			objects: []runtime.Object{
				unlabeledDeployment("app-server", ns, 3),
				unlabeledDeployment("cache", ns, 1),
				appServerPod(ns, "app-server-55dbdcc9cb-k6pfv", corev1.PodRunning, true),
				appServerPod(ns, "app-server-55dbdcc9cb-t56jn", corev1.PodRunning, true),
				appServerPod(ns, "app-server-55dbdcc9cb-r7vl6", corev1.PodRunning, true),
				appServerPod(ns, "app-server-55dbdcc9cb-zs5sb", corev1.PodSucceeded, false),
			},
			wantNames: []string{
				"app-server-55dbdcc9cb-k6pfv",
				"app-server-55dbdcc9cb-r7vl6",
				"app-server-55dbdcc9cb-t56jn",
			},
			wantReady: map[string]bool{
				"app-server-55dbdcc9cb-k6pfv": true,
				"app-server-55dbdcc9cb-r7vl6": true,
				"app-server-55dbdcc9cb-t56jn": true,
			},
			desired: 3,
			ready:   3,
		},
		{
			name: "all succeeded still reports desired from unlabeled deployment",
			objects: []runtime.Object{
				unlabeledDeployment("app-server", ns, 3),
				appServerPod(ns, "app-server-55dbdcc9cb-zs5sb", corev1.PodSucceeded, false),
			},
			wantNames: []string{},
			wantReady: map[string]bool{},
			desired:   3,
			ready:     0,
		},
		{
			name: "mixed size deployments without metadata labels",
			objects: []runtime.Object{
				unlabeledDeployment("app-server", ns, 2),
				unlabeledDeployment("app-server-small", ns, 1),
				appServerPod(ns, "app-server-aaa", corev1.PodRunning, true),
				appServerPod(ns, "app-server-bbb", corev1.PodRunning, true),
				appServerPod(ns, "app-server-ccc", corev1.PodRunning, false),
			},
			wantNames: []string{"app-server-aaa", "app-server-bbb", "app-server-ccc"},
			wantReady: map[string]bool{
				"app-server-aaa": true,
				"app-server-bbb": true,
				"app-server-ccc": false,
			},
			desired: 3,
			ready:   2,
		},
		{
			name: "terminating and failed pods omitted",
			objects: []runtime.Object{
				unlabeledDeployment("app-server", ns, 1),
				appServerPod(ns, "app-server-live", corev1.PodRunning, true),
				terminatingAppServerPod(ns, "app-server-dying", corev1.PodRunning),
				appServerPod(ns, "app-server-dead", corev1.PodFailed, false),
			},
			wantNames: []string{"app-server-live"},
			wantReady: map[string]bool{"app-server-live": true},
			desired:   1,
			ready:     1,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			clientset := fake.NewSimpleClientset(tt.objects...)
			c := NewMetricsCollector("", nil, clientset, nil, nil)
			names, ready, counts := c.listAppServerPods(context.Background(), ns)
			got := append([]string(nil), names...)
			slices.Sort(got)
			if !slices.Equal(got, tt.wantNames) {
				t.Fatalf("names = %v want %v", got, tt.wantNames)
			}
			if counts.Desired != tt.desired || counts.Ready != tt.ready {
				t.Fatalf("counts = %+v want desired=%d ready=%d", counts, tt.desired, tt.ready)
			}
			for name := range ready {
				if !slices.Contains(tt.wantNames, name) {
					t.Fatalf("unexpected ready entry %s: %+v", name, ready)
				}
			}
			for name, wantReady := range tt.wantReady {
				if ready[name] != wantReady {
					t.Fatalf("ready[%s] = %v want %v (map=%+v)", name, ready[name], wantReady, ready)
				}
			}
		})
	}
}

func TestListWorkerPodsIgnoresSucceededAndUnlabeledDesired(t *testing.T) {
	t.Parallel()
	const ns = "ns-worker-zombie"
	clientset := fake.NewSimpleClientset(
		unlabeledDeployment("worker", ns, 2),
		workerPod(ns, "worker-live-a", corev1.PodRunning, true),
		workerPod(ns, "worker-live-b", corev1.PodRunning, true),
		workerPod(ns, "worker-done", corev1.PodSucceeded, false),
	)
	c := NewMetricsCollector("", nil, clientset, nil, nil)
	names, ready, counts := c.listWorkerPods(context.Background(), ns)
	got := append([]string(nil), names...)
	slices.Sort(got)
	want := []string{"worker-live-a", "worker-live-b"}
	if !slices.Equal(got, want) {
		t.Fatalf("names = %v want %v", got, want)
	}
	if !ready["worker-live-a"] || !ready["worker-live-b"] {
		t.Fatalf("ready map: %+v", ready)
	}
	if _, ok := ready["worker-done"]; ok {
		t.Fatalf("succeeded worker present: %+v", ready)
	}
	if counts.Desired != 2 || counts.Ready != 2 {
		t.Fatalf("counts: %+v", counts)
	}
}

func unlabeledDeployment(name, ns string, replicas int32) *appsv1.Deployment {
	n := replicas
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       appsv1.DeploymentSpec{Replicas: &n},
	}
}

func appServerPod(ns, name string, phase corev1.PodPhase, ready bool) *corev1.Pod {
	return labeledPod(ns, name, "app-server", phase, ready)
}

func workerPod(ns, name string, phase corev1.PodPhase, ready bool) *corev1.Pod {
	return labeledPod(ns, name, "worker", phase, ready)
}

func labeledPod(ns, name, app string, phase corev1.PodPhase, ready bool) *corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    map[string]string{"app": app, "deployment": ns},
		},
		Status: corev1.PodStatus{
			Phase:      phase,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: status}},
		},
	}
}

func terminatingAppServerPod(ns, name string, phase corev1.PodPhase) *corev1.Pod {
	p := appServerPod(ns, name, phase, false)
	ts := metav1.Now()
	p.DeletionTimestamp = &ts
	p.Finalizers = []string{"foregroundDeletion"}
	return p
}
