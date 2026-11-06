package deployer

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func lbWatchTestDesign() DeploymentDesign {
	return DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "lb1", Type: "lb", Name: "LB"},
			{ID: "a1", Type: "app-server", Name: "App", Config: map[string]interface{}{"replicas": 1}},
		},
		Connections: []DeploymentConn{{SourceID: "lb1", TargetID: "a1"}},
	}
}

func lbWatchTestClientset(t *testing.T, ns string, ep *corev1.Endpoints, nginxConf string) *fake.Clientset {
	t.Helper()
	spec := DefaultLBSpec()
	if nginxConf == "" {
		nginxConf = spec.nginxConfig([]string{"app-server"}, "round-robin")
	}
	objs := []runtime.Object{
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: lbConfigMapName, Namespace: ns},
			Data:       map[string]string{"nginx.conf": nginxConf},
		},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "lb", Namespace: ns},
			Spec: appsv1.DeploymentSpec{
				Replicas: ptr(int32(1)),
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "lb", "deployment": ns}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "lb", "deployment": ns}},
				},
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "lb-ready",
				Namespace: ns,
				Labels:    map[string]string{"app": "lb", "deployment": ns},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				}},
			},
		},
	}
	if ep != nil {
		objs = append(objs, ep)
	}
	return fake.NewSimpleClientset(objs...)
}

func TestReadyAddressSet(t *testing.T) {
	t.Parallel()
	ep := &corev1.Endpoints{
		Subsets: []corev1.EndpointSubset{{
			Addresses: []corev1.EndpointAddress{{IP: "10.0.0.2"}, {IP: "10.0.0.1"}},
		}},
	}
	got := readyAddressSet(ep)
	want := []string{"10.0.0.1", "10.0.0.2"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}

func TestLBEndpointsWatch_debouncedReconcileUpdatesConfig(t *testing.T) {
	ns := "ns-lb-watch-direct"
	spec := DefaultLBSpec()
	design := lbWatchTestDesign()
	ep := &corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{Name: "app-server", Namespace: ns},
		Subsets: []corev1.EndpointSubset{{
			Addresses: []corev1.EndpointAddress{{IP: "10.0.0.5"}},
		}},
	}
	client := lbWatchTestClientset(t, ns, ep, spec.nginxConfig([]string{"app-server"}, "round-robin"))

	state := &lbEndpointsWatch{
		clientset: client,
		namespace: ns,
		design:    design,
		debounce:  10 * time.Millisecond,
	}
	state.onEndpoints(ep)

	wantConf := spec.nginxConfig([]string{"10.0.0.5"}, "round-robin")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		cm, err := client.CoreV1().ConfigMaps(ns).Get(context.Background(), lbConfigMapName, metav1.GetOptions{})
		if err == nil && cm.Data["nginx.conf"] == wantConf {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	cm, _ := client.CoreV1().ConfigMaps(ns).Get(context.Background(), lbConfigMapName, metav1.GetOptions{})
	t.Fatalf("nginx.conf not updated; got %q", cm.Data["nginx.conf"])
}

func TestLBEndpointsWatcher_firstReadySetUpdatesConfig(t *testing.T) {
	ns := "ns-lb-watch-first"
	spec := DefaultLBSpec()
	design := lbWatchTestDesign()
	epEmpty := &corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{Name: "app-server", Namespace: ns, ResourceVersion: "1"},
	}
	client := lbWatchTestClientset(t, ns, epEmpty, "")

	state := &lbEndpointsWatch{
		clientset: client,
		namespace: ns,
		design:    design,
		debounce:  20 * time.Millisecond,
	}
	ep := epEmpty.DeepCopy()
	ep.Subsets = []corev1.EndpointSubset{{
		Addresses: []corev1.EndpointAddress{
			{IP: "10.0.0.1", TargetRef: &corev1.ObjectReference{Name: "app-server-0"}},
		},
	}}
	if _, err := client.CoreV1().Endpoints(ns).Update(context.Background(), ep, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update endpoints: %v", err)
	}
	state.onEndpoints(ep)

	wantConf := spec.nginxConfig([]string{"10.0.0.1"}, "round-robin")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		cm, err := client.CoreV1().ConfigMaps(ns).Get(context.Background(), lbConfigMapName, metav1.GetOptions{})
		if err == nil && cm.Data["nginx.conf"] == wantConf {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	cm, _ := client.CoreV1().ConfigMaps(ns).Get(context.Background(), lbConfigMapName, metav1.GetOptions{})
	t.Fatalf("nginx.conf not updated; got %q", cm.Data["nginx.conf"])
}

func TestLBEndpointsWatcher_replacementPodUpdatesConfig(t *testing.T) {
	ns := "ns-lb-watch-replace"
	spec := DefaultLBSpec()
	design := lbWatchTestDesign()
	ep := &corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{Name: "app-server", Namespace: ns},
		Subsets: []corev1.EndpointSubset{{
			Addresses: []corev1.EndpointAddress{
				{IP: "10.0.0.1", TargetRef: &corev1.ObjectReference{Name: "app-server-0"}},
			},
		}},
	}
	client := lbWatchTestClientset(t, ns, ep, spec.nginxConfig([]string{"10.0.0.1"}, "round-robin"))

	state := &lbEndpointsWatch{
		clientset: client,
		namespace: ns,
		design:    design,
		debounce:  20 * time.Millisecond,
		lastSet:   []string{"10.0.0.1"},
	}

	ep2 := ep.DeepCopy()
	ep2.ResourceVersion = "2"
	ep2.Subsets[0].Addresses = []corev1.EndpointAddress{
		{IP: "10.0.0.9", TargetRef: &corev1.ObjectReference{Name: "app-server-1"}},
	}
	if _, err := client.CoreV1().Endpoints(ns).Update(context.Background(), ep2, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update endpoints: %v", err)
	}
	state.onEndpoints(ep2)

	wantConf := spec.nginxConfig([]string{"10.0.0.9"}, "round-robin")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		cm, err := client.CoreV1().ConfigMaps(ns).Get(context.Background(), lbConfigMapName, metav1.GetOptions{})
		if err == nil && cm.Data["nginx.conf"] == wantConf {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	cm, _ := client.CoreV1().ConfigMaps(ns).Get(context.Background(), lbConfigMapName, metav1.GetOptions{})
	t.Fatalf("nginx.conf not updated after pod replace; got %q", cm.Data["nginx.conf"])
}

func TestLBEndpointsWatcher_unchangedReadySetNoConfigWrite(t *testing.T) {
	ns := "ns-lb-watch-noop"
	spec := DefaultLBSpec()
	design := lbWatchTestDesign()
	wantConf := spec.nginxConfig([]string{"10.0.0.1"}, "round-robin")
	ep := &corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{Name: "app-server", Namespace: ns},
		Subsets: []corev1.EndpointSubset{{
			Addresses: []corev1.EndpointAddress{{IP: "10.0.0.1"}},
		}},
	}
	client := lbWatchTestClientset(t, ns, ep, wantConf)

	var updates atomic.Int32
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		updates.Add(1)
		return false, nil, nil
	})

	w := NewLBEndpointsWatcher()
	w.SetDebounce(20 * time.Millisecond)
	w.Start(client, ns, design, nil)
	t.Cleanup(func() { w.Stop(ns) })

	state, ok := w.watches[ns]
	if !ok {
		t.Fatal("expected active watch state")
	}
	ep2 := ep.DeepCopy()
	ep2.ResourceVersion = "2"
	ep2.Annotations = map[string]string{"touch": "1"}
	state.onEndpoints(ep2)
	time.Sleep(150 * time.Millisecond)

	if n := updates.Load(); n != 0 {
		t.Fatalf("expected no configmap updates, got %d", n)
	}
	cm, err := client.CoreV1().ConfigMaps(ns).Get(context.Background(), lbConfigMapName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get cm: %v", err)
	}
	if cm.Data["nginx.conf"] != wantConf {
		t.Fatalf("nginx.conf changed unexpectedly")
	}
}

func TestLBEndpointsWatcher_stopPreventsFurtherReconcile(t *testing.T) {
	ns := "ns-lb-watch-stop"
	spec := DefaultLBSpec()
	design := lbWatchTestDesign()
	ep := &corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{Name: "app-server", Namespace: ns},
		Subsets: []corev1.EndpointSubset{{
			Addresses: []corev1.EndpointAddress{{IP: "10.0.0.1"}},
		}},
	}
	staleConf := spec.nginxConfig([]string{"10.0.0.1"}, "round-robin")
	client := lbWatchTestClientset(t, ns, ep, staleConf)

	w := NewLBEndpointsWatcher()
	w.SetDebounce(20 * time.Millisecond)
	w.Start(client, ns, design, nil)
	w.Stop(ns)
	if _, ok := w.watches[ns]; ok {
		t.Fatal("watch still registered after stop")
	}

	time.Sleep(150 * time.Millisecond)

	cm, err := client.CoreV1().ConfigMaps(ns).Get(context.Background(), lbConfigMapName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get cm: %v", err)
	}
	if strings.Contains(cm.Data["nginx.conf"], "10.0.0.8") {
		t.Fatal("watch reconciled after stop")
	}
}

func TestLBEndpointsWatcher_noLBNodeIsNoOp(t *testing.T) {
	ns := "ns-lb-watch-nolb"
	design := DeploymentDesign{
		Nodes: []DeploymentNode{{ID: "a1", Type: "app-server", Name: "App"}},
	}
	client := fake.NewSimpleClientset()
	w := NewLBEndpointsWatcher()
	w.Start(client, ns, design, nil)
	w.Stop(ns)
}
