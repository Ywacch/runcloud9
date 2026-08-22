package metrics

import (
	"context"
	"sync"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type recordingConsistencyObserver struct {
	mu      sync.Mutex
	started map[string]int
	stopped map[string]int
}

func newRecordingConsistencyObserver() *recordingConsistencyObserver {
	return &recordingConsistencyObserver{
		started: make(map[string]int),
		stopped: make(map[string]int),
	}
}

func (r *recordingConsistencyObserver) Start(namespace string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started[namespace] > 0 {
		return
	}
	r.started[namespace] = 1
}

func (r *recordingConsistencyObserver) Stop(namespace string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started[namespace] == 0 {
		return
	}
	delete(r.started, namespace)
	r.stopped[namespace]++
}

func (r *recordingConsistencyObserver) countStarted(namespace string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.started[namespace]
}

func (r *recordingConsistencyObserver) countStopped(namespace string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stopped[namespace]
}

func TestCollect_startsConsistencyObserverWhenReplicasPresent(t *testing.T) {
	t.Parallel()
	ns := "ns-observer-start"
	desired := int32(1)
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dbReplicaDeploymentName,
			Namespace: ns,
			Labels:    map[string]string{"app": dbReplicaAppLabel},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &desired,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": dbReplicaAppLabel}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": dbReplicaAppLabel, "deployment": ns}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "postgres", Image: "postgres:16"}}},
			},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "postgres-replica-obs",
			Namespace: ns,
			Labels:    map[string]string{"app": dbReplicaAppLabel, "deployment": ns},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	clientset := fake.NewSimpleClientset(deployment, pod)
	rec := newRecordingConsistencyObserver()
	c := NewMetricsCollector("", nil, clientset, nil, nil)
	c.observer = rec

	topo := TopologyFlags{HasDB: true, DBEngine: "postgresql", DBRole: "primary"}
	if _, err := c.Collect(context.Background(), ns, "url-shortener", topo); err != nil {
		t.Fatal(err)
	}
	if rec.countStarted(ns) != 1 {
		t.Fatalf("Start calls: got %d want 1", rec.countStarted(ns))
	}

	c.Collect(context.Background(), ns, "url-shortener", topo)
	if rec.countStarted(ns) != 1 {
		t.Fatalf("Start should be idempotent: got %d", rec.countStarted(ns))
	}
}

func TestCollect_doesNotStartObserverWithoutReplicas(t *testing.T) {
	t.Parallel()
	ns := "ns-no-replica"
	rec := newRecordingConsistencyObserver()
	c := NewMetricsCollector("", nil, fake.NewSimpleClientset(), nil, nil)
	c.observer = rec

	topo := TopologyFlags{HasDB: true, DBEngine: "postgresql", DBRole: "primary"}
	if _, err := c.Collect(context.Background(), ns, "url-shortener", topo); err != nil {
		t.Fatal(err)
	}
	if rec.countStarted(ns) != 0 {
		t.Fatalf("Start calls: got %d want 0", rec.countStarted(ns))
	}
}

func TestCleanup_stopsConsistencyObserver(t *testing.T) {
	t.Parallel()
	ns := "ns-observer-stop"
	rec := newRecordingConsistencyObserver()
	c := NewMetricsCollector("", nil, fake.NewSimpleClientset(), nil, nil)
	c.observer = rec
	rec.Start(ns)

	c.Cleanup(ns)
	if rec.countStopped(ns) != 1 {
		t.Fatalf("Stop calls: got %d want 1", rec.countStopped(ns))
	}
}
