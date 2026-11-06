package clusterstate

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func startTestCache(t *testing.T, objects ...runtime.Object) *Cache {
	t.Helper()
	client := fake.NewSimpleClientset(objects...)
	c := newCacheFromClient(client, nil)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c.Start(ctx)
	syncCtx, syncCancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(syncCancel)
	if err := c.WaitForSync(syncCtx); err != nil {
		t.Fatalf("WaitForSync: %v", err)
	}
	return c
}

func TestCache_PodsByNamespace(t *testing.T) {
	objs := []runtime.Object{
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "labeled", Namespace: "ns-a", Labels: map[string]string{"deployment": "ns-a"}},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "unlabeled", Namespace: "ns-a"},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "other-ns", Namespace: "ns-b", Labels: map[string]string{"deployment": "ns-b"}},
		},
	}
	c := startTestCache(t, objs...)

	pods, err := c.Pods("ns-a")
	if err != nil {
		t.Fatalf("Pods: %v", err)
	}
	if len(pods) != 1 || pods[0].Name != "labeled" {
		t.Fatalf("Pods(ns-a) = %#v, want only labeled", pods)
	}
	if _, err := c.Pods(""); err == nil {
		t.Fatal("Pods(\"\") expected error")
	}
}

func TestCache_PodNotFound(t *testing.T) {
	c := startTestCache(t)
	_, err := c.Pod("ns-a", "missing")
	if err == nil || !apierrors.IsNotFound(err) {
		t.Fatalf("Pod missing: err = %v, want NotFound", err)
	}
}

func TestCache_EndpointSlicesByService(t *testing.T) {
	svcA := "app-server"
	svcB := "other"
	objs := []runtime.Object{
		&discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "slice-a",
				Namespace: "ns-a",
				Labels:    map[string]string{discoveryv1.LabelServiceName: svcA},
			},
		},
		&discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "slice-b",
				Namespace: "ns-a",
				Labels:    map[string]string{discoveryv1.LabelServiceName: svcB},
			},
		},
	}
	c := startTestCache(t, objs...)
	slices, err := c.EndpointSlices("ns-a", svcA)
	if err != nil {
		t.Fatalf("EndpointSlices: %v", err)
	}
	if len(slices) != 1 || slices[0].Name != "slice-a" {
		t.Fatalf("slices = %#v, want slice-a only", slices)
	}
}

func TestEnabled(t *testing.T) {
	tests := []struct {
		raw  string
		want bool
	}{
		{"", false},
		{"0", false},
		{"false", false},
		{"1", true},
		{"TRUE", true},
	}
	for _, tc := range tests {
		if got := Enabled(tc.raw); got != tc.want {
			t.Errorf("Enabled(%q) = %v, want %v", tc.raw, got, tc.want)
		}
	}
}
