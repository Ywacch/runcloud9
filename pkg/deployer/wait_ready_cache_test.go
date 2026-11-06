package deployer

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestMatchEqualSelector(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		labels   map[string]string
		selector string
		want     bool
		wantErr  bool
	}{
		{
			name:     "exact match",
			labels:   map[string]string{"deployment": "ns-a", "app": "postgres"},
			selector: "deployment=ns-a,app=postgres",
			want:     true,
		},
		{
			name:     "missing key",
			labels:   map[string]string{"app": "postgres"},
			selector: "deployment=ns-a,app=postgres",
			want:     false,
		},
		{
			name:     "wrong value",
			labels:   map[string]string{"deployment": "ns-b", "app": "postgres"},
			selector: "deployment=ns-a,app=postgres",
			want:     false,
		},
		{
			name:     "empty selector matches nothing",
			labels:   map[string]string{"deployment": "ns-a"},
			selector: "",
			want:     false,
		},
		{
			name:     "invalid selector",
			labels:   map[string]string{"deployment": "ns-a"},
			selector: "badterm",
			wantErr:  true,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := matchEqualSelector(tc.labels, tc.selector)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("matchEqualSelector: %v", err)
			}
			if got != tc.want {
				t.Fatalf("matchEqualSelector = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestWaitForReady_ListerDoesNotList(t *testing.T) {
	ns := "cloud9-lister-no-list"
	labels := map[string]string{"app": "app-server", "deployment": ns}
	readyPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "app-server-0",
			Namespace: ns,
			Labels:    labels,
			UID:       types.UID("ready-uid"),
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	fakeClient := fake.NewSimpleClientset()
	var listed atomic.Bool
	fakeClient.PrependReactor("list", "pods", func(action ktesting.Action) (handled bool, ret runtime.Object, err error) {
		listed.Store(true)
		return false, nil, nil
	})

	d := NewK8sDeployer(fakeClient, "db")
	d.SetPodLister(func(namespace string) ([]*corev1.Pod, error) {
		if namespace != ns {
			return nil, nil
		}
		return []*corev1.Pod{readyPod}, nil
	})

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cfg := DeployConfig{Namespace: ns, Node: DeploymentNode{ID: "app-1"}}
	if err := d.waitForReady(waitCtx, cfg, "app-server", 1, waitProgressOpts{}); err != nil {
		t.Fatalf("waitForReady: %v", err)
	}
	if listed.Load() {
		t.Fatal("expected no API pod list when PodLister is set")
	}
}

func TestWaitForReady_NilListerLists(t *testing.T) {
	ns := "cloud9-lister-api-list"
	labels := map[string]string{"app": "app-server", "deployment": ns}
	fakeClient := fake.NewSimpleClientset()
	ctx := context.Background()
	pod, err := fakeClient.CoreV1().Pods(ns).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "app-server-0", Namespace: ns, Labels: labels},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create pod: %v", err)
	}
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if _, err := fakeClient.CoreV1().Pods(ns).UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update status: %v", err)
	}

	var listed atomic.Bool
	fakeClient.PrependReactor("list", "pods", func(action ktesting.Action) (handled bool, ret runtime.Object, err error) {
		listed.Store(true)
		return false, nil, nil
	})

	d := NewK8sDeployer(fakeClient, "db")
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cfg := DeployConfig{Namespace: ns, Node: DeploymentNode{ID: "app-1"}}
	if err := d.waitForReady(waitCtx, cfg, "app-server", 1, waitProgressOpts{}); err != nil {
		t.Fatalf("waitForReady: %v", err)
	}
	if !listed.Load() {
		t.Fatal("expected API pod list when PodLister is nil")
	}
}

func TestWaitForReady_CrashLoopStillFails(t *testing.T) {
	ns := "cloud9-lister-crash"
	labels := map[string]string{"app": "app-server", "deployment": ns}
	crashPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "app-server-0",
			Namespace: ns,
			Labels:    labels,
			UID:       types.UID("crash-uid"),
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:         "app",
				RestartCount: waitReadyRestartFailThreshold,
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff", Message: "back-off"},
				},
			}},
		},
	}
	fakeClient := fake.NewSimpleClientset()
	d := NewK8sDeployer(fakeClient, "db")
	d.SetPodLister(func(namespace string) ([]*corev1.Pod, error) {
		return []*corev1.Pod{crashPod}, nil
	})

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cfg := DeployConfig{Namespace: ns, Node: DeploymentNode{ID: "app-1"}}
	err := d.waitForReady(waitCtx, cfg, "app-server", 1, waitProgressOpts{})
	if err == nil {
		t.Fatal("expected error from CrashLoopBackOff")
	}
	if !strings.Contains(err.Error(), "CrashLoopBackOff") {
		t.Fatalf("expected CrashLoopBackOff failure, got %v", err)
	}
}
