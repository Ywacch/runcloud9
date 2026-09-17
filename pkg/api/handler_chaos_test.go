package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
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
	if events[0].Kind != "chaos_inject" {
		t.Fatalf("kind: want chaos_inject, got %q", events[0].Kind)
	}
	if events[0].Detail["scenario"] != "pod_kill" || events[0].Detail["targetComponent"] != "app-server" {
		t.Fatalf("detail: %+v", events[0].Detail)
	}
	if events[0].Audience != "both" {
		t.Fatalf("audience: %q", events[0].Audience)
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

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: want 400, got %d body=%s", rec.Code, rec.Body.String())
	}
	var er struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &er)
	if !strings.Contains(er.Error, "supported:") {
		t.Fatalf("error: %q", er.Error)
	}
}

func chaosDBDesign() map[string]any {
	return map[string]any{
		"nodes": []any{
			map[string]any{"type": "app-server", "name": "App"},
			map[string]any{
				"type": "db",
				"name": "DB",
				"config": map[string]any{
					"engine":       "postgresql",
					"readReplicas": 1,
				},
			},
		},
	}
}

func TestHandleChaosHeal_OK(t *testing.T) {
	t.Parallel()
	ns := "ns-heal"
	replica := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "postgres-replica-0",
			Namespace: ns,
			Labels:    map[string]string{"app": "postgres-replica"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	client := fake.NewClientset(replica)
	st := &fakeDeploymentStore{
		byNS: map[string]store.DeploymentRow{
			ns: {
				Namespace: ns,
				Status:    "running",
				Design:    chaosDBDesign(),
			},
		},
	}
	bus := metrics.NewEventBus()
	h := NewHandler(nil, st, nil, nil, nil, nil, client, nil, bus)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	applyBody := `{"scenario":"split_primary_replica","targetComponent":"db"}`
	applyReq := httptest.NewRequest(http.MethodPost, "/deployments/"+ns+"/chaos", strings.NewReader(applyBody))
	applyReq.Header.Set("Content-Type", "application/json")
	applyRec := httptest.NewRecorder()
	mux.ServeHTTP(applyRec, applyReq)
	if applyRec.Code != http.StatusOK {
		t.Fatalf("apply status: want 200, got %d body=%s", applyRec.Code, applyRec.Body.String())
	}

	delReq := httptest.NewRequest(http.MethodDelete, "/deployments/"+ns+"/chaos", nil)
	delRec := httptest.NewRecorder()
	mux.ServeHTTP(delRec, delReq)
	if delRec.Code != http.StatusOK {
		t.Fatalf("delete status: want 200, got %d body=%s", delRec.Code, delRec.Body.String())
	}

	events := bus.Drain(ns)
	var heal *metrics.Event
	for i := range events {
		if events[i].Kind == "chaos_heal" {
			heal = &events[i]
			break
		}
	}
	if heal == nil {
		t.Fatalf("expected chaos_heal event, got %+v", events)
	}
	if heal.Detail["scenario"] != "split_primary_replica" || heal.Detail["targetComponent"] != "db" || heal.Detail["reason"] != "manual" {
		t.Fatalf("heal detail: %+v", heal.Detail)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/deployments/"+ns+"/chaos", nil)
	listRec := httptest.NewRecorder()
	mux.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list status: want 200, got %d", listRec.Code)
	}
	var listResp struct {
		Partitions []map[string]string `json:"partitions"`
	}
	_ = json.Unmarshal(listRec.Body.Bytes(), &listResp)
	if len(listResp.Partitions) != 0 {
		t.Fatalf("expected empty partitions, got %+v", listResp.Partitions)
	}
}

func TestHandleChaosHeal_noActive(t *testing.T) {
	t.Parallel()
	ns := "ns-heal-empty"
	st := &fakeDeploymentStore{
		byNS: map[string]store.DeploymentRow{
			ns: {
				Namespace: ns,
				Status:    "running",
				Design:    chaosDBDesign(),
			},
		},
	}
	h := NewHandler(nil, st, nil, nil, nil, nil, fake.NewClientset(), nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodDelete, "/deployments/"+ns+"/chaos", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestFailoverWriteLossEvent_zeroLoss(t *testing.T) {
	t.Parallel()

	ev := failoverWriteLossEvent(time.Now(), "promote_replica", deployer.PromotionResult{
		PromotedPod:  "postgres-replica-0",
		SyncState:    "sync",
		WalBytesLost: 0,
	}, map[string]any{
		"replicationMode": "semi-sync",
		"syncCommitLevel": "remote_apply",
	})
	if ev != nil {
		t.Fatalf("expected nil event for zero loss, got %+v", ev)
	}
}

func TestFailoverWriteLossEvent_lossy(t *testing.T) {
	t.Parallel()

	ev := failoverWriteLossEvent(time.Now(), "promote_lagging_replica", deployer.PromotionResult{
		PromotedPod:  "postgres-replica-1",
		SyncState:    "async",
		WalBytesLost: 4096,
	}, map[string]any{
		"replicationMode": "async",
	})
	if ev == nil {
		t.Fatal("expected invariant event")
	}
	if ev.RuleID != metrics.RuleFailoverWriteLoss {
		t.Fatalf("ruleId: %q", ev.RuleID)
	}
	if ev.Detail["walBytesLost"] != "4096" {
		t.Fatalf("detail: %+v", ev.Detail)
	}
}

func TestMergePromotionChaosDetail(t *testing.T) {
	t.Parallel()

	detail := map[string]string{
		"scenario":        "promote_replica",
		"targetComponent": "db",
	}
	mergePromotionChaosDetail(detail, deployer.PromotionResult{
		PromotedPod:  "postgres-replica-1",
		SyncState:    "streaming",
		LagBytes:     4096,
		WalBytesLost: 0,
	}, map[string]any{
		"replicationMode": "semi-sync",
		"syncCommitLevel": "remote_apply",
		"readReplicas":    2,
	})

	if detail["promotedPod"] != "postgres-replica-1" {
		t.Fatalf("promotedPod: %q", detail["promotedPod"])
	}
	if detail["candidateSyncState"] != "streaming" {
		t.Fatalf("candidateSyncState: %q", detail["candidateSyncState"])
	}
	if detail["candidateLagBytes"] != "4096" {
		t.Fatalf("candidateLagBytes: %q", detail["candidateLagBytes"])
	}
	if detail["walBytesLost"] != "0" {
		t.Fatalf("walBytesLost: %q", detail["walBytesLost"])
	}
	if detail["replicationMode"] != "semi-sync" {
		t.Fatalf("replicationMode: %q", detail["replicationMode"])
	}
	if detail["syncCommitLevel"] != "remote_apply" {
		t.Fatalf("syncCommitLevel: %q", detail["syncCommitLevel"])
	}
}

func TestHandleChaos_promoteSecondPartitionConflict(t *testing.T) {
	t.Parallel()
	ns := "ns-promote-conflict"
	replica := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "postgres-replica-0",
			Namespace: ns,
			Labels:    map[string]string{"app": "postgres-replica"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	client := fake.NewClientset(replica)
	st := &fakeDeploymentStore{
		byNS: map[string]store.DeploymentRow{
			ns: {
				Namespace: ns,
				Status:    "running",
				Design:    chaosDBDesign(),
			},
		},
	}
	h := NewHandler(nil, st, nil, nil, nil, nil, client, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	first := `{"scenario":"stall_replica","targetComponent":"db"}`
	req1 := httptest.NewRequest(http.MethodPost, "/deployments/"+ns+"/chaos", strings.NewReader(first))
	req1.Header.Set("Content-Type", "application/json")
	rec1 := httptest.NewRecorder()
	mux.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("first apply: want 200, got %d body=%s", rec1.Code, rec1.Body.String())
	}

	second := `{"scenario":"promote_replica","targetComponent":"db"}`
	req2 := httptest.NewRequest(http.MethodPost, "/deployments/"+ns+"/chaos", strings.NewReader(second))
	req2.Header.Set("Content-Type", "application/json")
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusConflict {
		t.Fatalf("promote during partition: want 409, got %d body=%s", rec2.Code, rec2.Body.String())
	}
}

func TestHandleChaos_secondPartitionConflict(t *testing.T) {
	t.Parallel()
	ns := "ns-conflict"
	replica := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "postgres-replica-0",
			Namespace: ns,
			Labels:    map[string]string{"app": "postgres-replica"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	client := fake.NewClientset(replica)
	st := &fakeDeploymentStore{
		byNS: map[string]store.DeploymentRow{
			ns: {
				Namespace: ns,
				Status:    "running",
				Design:    chaosDBDesign(),
			},
		},
	}
	h := NewHandler(nil, st, nil, nil, nil, nil, client, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	first := `{"scenario":"stall_replica","targetComponent":"db"}`
	req1 := httptest.NewRequest(http.MethodPost, "/deployments/"+ns+"/chaos", strings.NewReader(first))
	req1.Header.Set("Content-Type", "application/json")
	rec1 := httptest.NewRecorder()
	mux.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("first apply: want 200, got %d body=%s", rec1.Code, rec1.Body.String())
	}

	second := `{"scenario":"split_primary_replica","targetComponent":"db"}`
	req2 := httptest.NewRequest(http.MethodPost, "/deployments/"+ns+"/chaos", strings.NewReader(second))
	req2.Header.Set("Content-Type", "application/json")
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusConflict {
		t.Fatalf("second apply: want 409, got %d body=%s", rec2.Code, rec2.Body.String())
	}
}

func TestHandleChaosList_activePartition(t *testing.T) {
	t.Parallel()
	ns := "ns-list"
	replica := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "postgres-replica-0",
			Namespace: ns,
			Labels:    map[string]string{"app": "postgres-replica"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	client := fake.NewClientset(replica)
	st := &fakeDeploymentStore{
		byNS: map[string]store.DeploymentRow{
			ns: {
				Namespace: ns,
				Status:    "running",
				Design:    chaosDBDesign(),
			},
		},
	}
	h := NewHandler(nil, st, nil, nil, nil, nil, client, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	applyBody := `{"scenario":"stall_replica","targetComponent":"db"}`
	applyReq := httptest.NewRequest(http.MethodPost, "/deployments/"+ns+"/chaos", strings.NewReader(applyBody))
	applyReq.Header.Set("Content-Type", "application/json")
	applyRec := httptest.NewRecorder()
	mux.ServeHTTP(applyRec, applyReq)
	if applyRec.Code != http.StatusOK {
		t.Fatalf("apply status: want 200, got %d", applyRec.Code)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/deployments/"+ns+"/chaos", nil)
	listRec := httptest.NewRecorder()
	mux.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list status: want 200, got %d body=%s", listRec.Code, listRec.Body.String())
	}
	var listResp struct {
		Partitions []struct {
			Scenario        string `json:"scenario"`
			TargetComponent string `json:"targetComponent"`
			ExpiresAt       string `json:"expiresAt"`
			TargetPod       string `json:"targetPod"`
		} `json:"partitions"`
	}
	if err := json.Unmarshal(listRec.Body.Bytes(), &listResp); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(listResp.Partitions) != 1 {
		t.Fatalf("partitions: %+v", listResp.Partitions)
	}
	if listResp.Partitions[0].Scenario != "stall_replica" || listResp.Partitions[0].TargetComponent != "db" {
		t.Fatalf("partition: %+v", listResp.Partitions[0])
	}
	if listResp.Partitions[0].ExpiresAt == "" {
		t.Fatal("expected expiresAt on active partition")
	}
	if listResp.Partitions[0].TargetPod != "postgres-replica-0" {
		t.Fatalf("targetPod: %+v", listResp.Partitions[0])
	}
}

func TestHandleChaos_stallTargetPod(t *testing.T) {
	t.Parallel()
	ns := "ns-stall-pod"
	client := fake.NewClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "postgres-replica-0",
				Namespace: ns,
				Labels:    map[string]string{"app": "postgres-replica"},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "postgres-replica-1",
				Namespace: ns,
				Labels:    map[string]string{"app": "postgres-replica"},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		},
	)
	st := &fakeDeploymentStore{
		byNS: map[string]store.DeploymentRow{
			ns: {
				Namespace: ns,
				Status:    "running",
				Design:    chaosDBDesign(),
			},
		},
	}
	h := NewHandler(nil, st, nil, nil, nil, nil, client, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body := `{"scenario":"stall_replica","targetComponent":"db","targetPod":"postgres-replica-1"}`
	req := httptest.NewRequest(http.MethodPost, "/deployments/"+ns+"/chaos", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("apply: want 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var applied struct {
		TargetPod string `json:"targetPod"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &applied); err != nil {
		t.Fatalf("decode apply: %v", err)
	}
	if applied.TargetPod != "postgres-replica-1" {
		t.Fatalf("applied targetPod: %+v", applied)
	}
}

func TestHandleChaos_stallUnknownTargetPod(t *testing.T) {
	t.Parallel()
	ns := "ns-stall-unknown"
	client := fake.NewClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "postgres-replica-0",
				Namespace: ns,
				Labels:    map[string]string{"app": "postgres-replica"},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		},
	)
	st := &fakeDeploymentStore{
		byNS: map[string]store.DeploymentRow{
			ns: {
				Namespace: ns,
				Status:    "running",
				Design:    chaosDBDesign(),
			},
		},
	}
	h := NewHandler(nil, st, nil, nil, nil, nil, client, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body := `{"scenario":"stall_replica","targetComponent":"db","targetPod":"postgres-replica-9"}`
	req := httptest.NewRequest(http.MethodPost, "/deployments/"+ns+"/chaos", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: want 400, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleChaos_targetPodRejectedForSplit(t *testing.T) {
	t.Parallel()
	ns := "ns-split-pod"
	client := fake.NewClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "postgres-replica-0",
				Namespace: ns,
				Labels:    map[string]string{"app": "postgres-replica"},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		},
	)
	st := &fakeDeploymentStore{
		byNS: map[string]store.DeploymentRow{
			ns: {
				Namespace: ns,
				Status:    "running",
				Design:    chaosDBDesign(),
			},
		},
	}
	h := NewHandler(nil, st, nil, nil, nil, nil, client, nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body := `{"scenario":"split_primary_replica","targetComponent":"db","targetPod":"postgres-replica-0"}`
	req := httptest.NewRequest(http.MethodPost, "/deployments/"+ns+"/chaos", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: want 400, got %d body=%s", rec.Code, rec.Body.String())
	}
}
