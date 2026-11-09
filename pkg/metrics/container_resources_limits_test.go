package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func podWithCPULimit(name, ns, container, cpu string, deleting bool) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    map[string]string{"deployment": ns},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: container,
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse(cpu),
					},
				},
			}},
		},
	}
	if deleting {
		now := metav1.NewTime(time.Now())
		p.DeletionTimestamp = &now
	}
	return p
}

func TestLimits_FromCache(t *testing.T) {
	t.Parallel()

	ns := "session-a"
	inv := &fakeInventory{
		pods: []*corev1.Pod{
			podWithCPULimit("app-server-1", ns, "app-server", "500m", false),
			podWithCPULimit("app-server-2", ns, "app-server", "300m", false),
			podWithCPULimit("app-server-old", ns, "app-server", "2", true),
		},
	}
	c := NewMetricsCollector("", nil, nil, nil, nil)
	c.SetInventory(inv)

	got := c.kubeLimitCPU(context.Background(), ns, "app-server.*", "app-server")
	want := 0.8
	if got != want {
		t.Fatalf("cpu limit sum: got %v want %v", got, want)
	}
}

func TestLimits_NilCacheUsesPromQL(t *testing.T) {
	t.Parallel()

	ns := "session-b"
	var queries int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries++
		q := r.URL.Query().Get("query")
		if q != kubeLimitCPUPromQL(ns, "app-server.*", "", "app-server") {
			t.Errorf("unexpected query: %s", q)
		}
		_ = json.NewEncoder(w).Encode(promVectorJSON("1.5"))
	}))
	t.Cleanup(srv.Close)

	c := NewMetricsCollector(srv.URL, nil, nil, nil, nil)
	got := c.kubeLimitCPU(context.Background(), ns, "app-server.*", "app-server")
	if got != 1.5 {
		t.Fatalf("got %v want 1.5", got)
	}
	if queries != 1 {
		t.Fatalf("prometheus calls: got %d want 1", queries)
	}
}
