package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/store"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestHandleChaos_RequiresKubernetes(t *testing.T) {
	t.Parallel()
	st := &fakeDeploymentStore{
		byNS: map[string]store.DeploymentRow{
			"ns1": {
				Namespace: "ns1",
				Status:    "running",
				Design: map[string]any{
					"nodes": []any{
						map[string]any{"type": "app-server"},
					},
				},
			},
		},
	}
	h := NewHandler(nil, st, nil, nil, nil, nil, nil, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body := `{"scenario":"pod_kill","targetComponent":"app-server"}`
	req := httptest.NewRequest(http.MethodPost, "/deployments/ns1/chaos", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status: want 503, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleChaos_RequestBodyTooLarge(t *testing.T) {
	t.Parallel()
	st := &fakeDeploymentStore{
		byNS: map[string]store.DeploymentRow{
			"ns1": {
				Namespace: "ns1",
				Status:    "running",
				Design: map[string]any{
					"nodes": []any{
						map[string]any{"type": "app-server"},
					},
				},
			},
		},
	}
	h := NewHandler(nil, st, nil, nil, nil, nil, fake.NewClientset(), nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	huge := strings.Repeat("a", int(maxJSONBodyBytes))
	tooLarge := `{"scenario":"` + huge + `","targetComponent":"app-server"}`
	req := httptest.NewRequest(http.MethodPost, "/deployments/ns1/chaos", strings.NewReader(tooLarge))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: want 400, got %d body=%s", rec.Code, rec.Body.String())
	}
	var er struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &er)
	if !strings.Contains(er.Error, "request body too large") {
		t.Fatalf("error message: %q", er.Error)
	}
}

func TestHandleChaos_PodKillOK(t *testing.T) {
	t.Parallel()
	ns := "ns-chaos"
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "app-server-xyz",
			Namespace: ns,
			Labels:    map[string]string{"app": "app-server"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	client := fake.NewClientset(pod)

	st := &fakeDeploymentStore{
		byNS: map[string]store.DeploymentRow{
			ns: {
				Namespace: ns,
				Status:    "running",
				Design: map[string]any{
					"nodes": []any{
						map[string]any{"type": "app-server", "name": "App"},
					},
				},
			},
		},
	}
	bus := metrics.NewEventBus()
	h := NewHandler(nil, st, nil, nil, nil, nil, client, nil, bus)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body := `{"scenario":"pod_kill","targetComponent":"app-server"}`
	req := httptest.NewRequest(http.MethodPost, "/deployments/"+ns+"/chaos", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}

	events := bus.Drain(ns)
	if len(events) != 1 || events[0].Source != "chaos" {
		t.Fatalf("events: %+v", events)
	}

	_, err := client.CoreV1().Pods(ns).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err == nil {
		t.Fatal("expected pod deleted")
	}
}

func TestHandleChaos_NotRunning(t *testing.T) {
	t.Parallel()
	st := &fakeDeploymentStore{
		byNS: map[string]store.DeploymentRow{
			"ns1": {
				Namespace: "ns1",
				Status:    "provisioning",
				Design: map[string]any{
					"nodes": []any{map[string]any{"type": "app-server"}},
				},
			},
		},
	}
	h := NewHandler(nil, st, nil, nil, nil, nil, fake.NewClientset(), nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body := `{"scenario":"pod_kill","targetComponent":"app-server"}`
	req := httptest.NewRequest(http.MethodPost, "/deployments/ns1/chaos", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: want 400, got %d", rec.Code)
	}
}

func TestHandleChaos_CacheFlushRequiresCache(t *testing.T) {
	t.Parallel()
	ns := "ns-flush"
	st := &fakeDeploymentStore{
		byNS: map[string]store.DeploymentRow{
			ns: {
				Namespace: ns,
				Status:    "running",
				Design: map[string]any{
					"nodes": []any{
						map[string]any{"type": "app-server"},
						map[string]any{"type": "cache", "config": map[string]any{"strategy": "cache-aside"}},
					},
				},
				CreatedAt: time.Now(),
			},
		},
	}
	h := NewHandler(nil, st, nil, nil, nil, nil, fake.NewClientset(), nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body := `{"scenario":"cache_flush","targetComponent":"app-server"}`
	req := httptest.NewRequest(http.MethodPost, "/deployments/"+ns+"/chaos", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status: want 500, got %d body=%s", rec.Code, rec.Body.String())
	}
	var er struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &er)
	if !strings.Contains(er.Error, "cache_flush only supports") {
		t.Fatalf("error: %q", er.Error)
	}
}
