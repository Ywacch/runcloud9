package metrics

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

type fakeInventory struct {
	pods  []*corev1.Pod
	deps  []*appsv1.Deployment
	sts   []*appsv1.StatefulSet
	hpas  []*autoscalingv2.HorizontalPodAutoscaler
	podBy map[string]*corev1.Pod
}

func (f *fakeInventory) Pods(namespace string) ([]*corev1.Pod, error) {
	_ = namespace
	return f.pods, nil
}

func (f *fakeInventory) Pod(namespace, name string) (*corev1.Pod, error) {
	_ = namespace
	if f.podBy != nil {
		if p, ok := f.podBy[name]; ok {
			return p, nil
		}
	}
	for _, p := range f.pods {
		if p != nil && p.Name == name {
			return p, nil
		}
	}
	return nil, context.Canceled
}

func (f *fakeInventory) Deployments(namespace string) ([]*appsv1.Deployment, error) {
	_ = namespace
	return f.deps, nil
}

func (f *fakeInventory) StatefulSets(namespace string) ([]*appsv1.StatefulSet, error) {
	_ = namespace
	return f.sts, nil
}

func (f *fakeInventory) HorizontalPodAutoscalers(namespace string) ([]*autoscalingv2.HorizontalPodAutoscaler, error) {
	_ = namespace
	return f.hpas, nil
}

func TestListLabeledLivePods_InventorySkipsClient(t *testing.T) {
	ns := "cloud9-inv-live"
	replicas := int32(2)
	inv := &fakeInventory{
		deps: []*appsv1.Deployment{{
			ObjectMeta: metav1.ObjectMeta{Name: "app-server", Namespace: ns},
			Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
		}},
		pods: []*corev1.Pod{{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "app-server-abc",
				Namespace: ns,
				Labels:    map[string]string{"app": "app-server", "deployment": ns},
			},
			Status: corev1.PodStatus{
				Phase:      corev1.PodRunning,
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		}},
	}
	client := fake.NewSimpleClientset()
	client.PrependReactor("list", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		t.Fatal("clientset pod list should not run when inventory is set")
		return true, nil, nil
	})
	client.PrependReactor("list", "deployments", func(action ktesting.Action) (bool, runtime.Object, error) {
		t.Fatal("clientset deployment list should not run when inventory is set")
		return true, nil, nil
	})

	c := NewMetricsCollector("", nil, client, nil, nil)
	c.SetInventory(inv)

	names, ready, counts := c.listLabeledLivePods(context.Background(), ns, "app-server")
	if counts.Desired != 2 {
		t.Fatalf("desired = %d, want 2", counts.Desired)
	}
	if len(names) != 1 || names[0] != "app-server-abc" {
		t.Fatalf("names = %v", names)
	}
	if !ready["app-server-abc"] || counts.Ready != 1 {
		t.Fatalf("ready=%v counts=%+v", ready, counts)
	}
}

func TestListLabeledLivePods_NilInventoryLists(t *testing.T) {
	ns := "cloud9-api-list"
	replicas := int32(1)
	client := fake.NewSimpleClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "app-server", Namespace: ns},
			Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "app-server-x",
				Namespace: ns,
				Labels:    map[string]string{"app": "app-server", "deployment": ns},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		},
	)
	var listed bool
	client.PrependReactor("list", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		listed = true
		return false, nil, nil
	})

	c := NewMetricsCollector("", nil, client, nil, nil)
	names, _, counts := c.listLabeledLivePods(context.Background(), ns, "app-server")
	if !listed {
		t.Fatal("expected clientset pod list")
	}
	if len(names) != 1 || counts.Desired != 1 {
		t.Fatalf("names=%v desired=%d", names, counts.Desired)
	}
}

func TestCollect_HPAFromInventory(t *testing.T) {
	ns := "cloud9-hpa-inv"
	desired := int32(3)
	inv := &fakeInventory{
		hpas: []*autoscalingv2.HorizontalPodAutoscaler{{
			ObjectMeta: metav1.ObjectMeta{Name: "app-server", Namespace: ns},
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
					Kind: "Deployment",
					Name: "app-server",
				},
			},
			Status: autoscalingv2.HorizontalPodAutoscalerStatus{DesiredReplicas: desired},
		}},
	}
	client := fake.NewSimpleClientset()
	client.PrependReactor("list", "horizontalpodautoscalers", func(action ktesting.Action) (bool, runtime.Object, error) {
		t.Fatal("clientset HPA list should not run when inventory is set")
		return true, nil, nil
	})

	c := NewMetricsCollector("", nil, client, nil, nil)
	c.SetInventory(inv)

	hpas, ok := c.loadSessionHPAs(context.Background(), ns)
	if !ok || len(hpas) != 1 {
		t.Fatalf("loadSessionHPAs: ok=%v len=%d", ok, len(hpas))
	}
	st := scaleStatusForApp(hpas, "app-server", 1)
	if st == nil || st.Desired != desired {
		t.Fatalf("scaleStatus: %+v", st)
	}
}

func TestPostgresWriterContainer_InventoryGet(t *testing.T) {
	ns := "cloud9-writer-inv"
	writer := "postgres-0"
	inv := &fakeInventory{
		podBy: map[string]*corev1.Pod{
			writer: {
				ObjectMeta: metav1.ObjectMeta{
					Name:      writer,
					Namespace: ns,
					Labels:    map[string]string{"app": dbReplicaAppLabel, "deployment": ns},
				},
			},
		},
	}
	client := fake.NewSimpleClientset()
	client.PrependReactor("get", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		t.Fatal("clientset pod get should not run when inventory is set")
		return true, nil, nil
	})

	c := NewMetricsCollector("", nil, client, nil, nil)
	c.SetInventory(inv)

	got := c.postgresWriterContainer(context.Background(), ns, writer)
	if got != dbReplicaContainerName {
		t.Fatalf("container = %q, want %q", got, dbReplicaContainerName)
	}
}
