package api

import (
	"context"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/admission"
	"github.com/ywacch/cloud9/pkg/store"
)

func TestProvisionETA_FallbackThenEWMA(t *testing.T) {
	t.Parallel()
	eta := newProvisionETA(240)
	if got := eta.Seconds(); got != 240 {
		t.Fatalf("fallback = %v, want 240", got)
	}
	eta.Observe(100 * time.Second)
	if got := eta.Seconds(); got != 100 {
		t.Fatalf("first sample = %v, want 100", got)
	}
	eta.Observe(200 * time.Second)
	got := eta.Seconds()
	// 0.3*200 + 0.7*100 = 130
	if got < 129 || got > 131 {
		t.Fatalf("ewma = %v, want ~130", got)
	}
}

func TestDrainDeployQueue_ExpiresTTL(t *testing.T) {
	t.Setenv("CLUSTER_DEPLOY_BUDGET_CPU", "100000m")
	t.Setenv("CLUSTER_DEPLOY_BUDGET_MEMORY", "200000Mi")

	s := newFakeDeploymentStore()
	ns := "ns-expired"
	s.byNS[ns] = store.DeploymentRow{
		ID: "1", UserID: "u1", Namespace: ns, Template: "url-shortener",
		Status: "queued", CreatedAt: time.Now().Add(-time.Hour),
		ExpiresAt: time.Now().Add(-time.Minute),
		Design: map[string]any{
			"template": "url-shortener",
			"nodes": []any{
				map[string]any{"id": "app", "type": "app-server"},
				map[string]any{"id": "db", "type": "db"},
			},
			"connections": []any{map[string]any{"sourceId": "app", "targetId": "db"}},
		},
	}
	s.active = append(s.active, store.ActiveDeploymentRow{
		ID: "1", Namespace: ns, Template: "url-shortener", Status: "queued",
		CreatedAt: time.Now().Add(-time.Hour), ExpiresAt: time.Now().Add(-time.Minute),
	})
	cpu, mem := int64(100), int64(100)
	s.admittedByNS[ns] = store.AdmissionOccupancy{CPUMillicores: cpu, MemoryMi: mem}

	h := NewHandler(nil, s, nil, nil, nil, nil, nil, nil, nil)
	h.drainDeployQueue(context.Background())

	row, err := s.GetDeployment(context.Background(), ns)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != "terminated" {
		t.Fatalf("status = %q, want terminated", row.Status)
	}
}

func TestDrainDeployQueue_PromotesWhenFits(t *testing.T) {
	t.Setenv("CLUSTER_DEPLOY_BUDGET_CPU", "100000m")
	t.Setenv("CLUSTER_DEPLOY_BUDGET_MEMORY", "200000Mi")

	s := newFakeDeploymentStore()
	ns := "ns-queued"
	s.users = map[string]store.UserRecord{"u1": {IsInternal: true}}
	s.byNS[ns] = store.DeploymentRow{
		ID: "1", UserID: "u1", Namespace: ns, Template: "url-shortener",
		Status: "queued", CreatedAt: time.Now().Add(-time.Minute),
		ExpiresAt: time.Now().Add(time.Hour),
		Design: map[string]any{
			"template":  "url-shortener",
			"archetype": "standard",
			"nodes": []any{
				map[string]any{"id": "app", "type": "app-server", "name": "app", "config": map[string]any{}},
				map[string]any{"id": "db", "type": "db", "name": "db", "config": map[string]any{}},
			},
			"connections": []any{map[string]any{"sourceId": "app", "targetId": "db"}},
		},
	}
	s.active = append(s.active, store.ActiveDeploymentRow{
		ID: "1", Namespace: ns, Template: "url-shortener", Status: "queued",
		CreatedAt: time.Now().Add(-time.Minute), ExpiresAt: time.Now().Add(time.Hour),
	})
	s.admittedByNS[ns] = store.AdmissionOccupancy{CPUMillicores: 900, MemoryMi: 2000}

	h := NewHandler(nil, s, nil, nil, nil, nil, nil, nil, nil)
	if h.roomBudget == nil {
		t.Fatal("expected room budget")
	}
	h.drainDeployQueue(context.Background())

	row, err := s.GetDeployment(context.Background(), ns)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != "provisioning" {
		t.Fatalf("status = %q, want provisioning", row.Status)
	}
}

func TestDrainDeployQueue_PaidPromotedBeforeFittingExplorer(t *testing.T) {
	t.Setenv("CLUSTER_DEPLOY_BUDGET_CPU", "1000m")
	t.Setenv("CLUSTER_DEPLOY_BUDGET_MEMORY", "3000Mi")

	s := newFakeDeploymentStore()
	future := time.Now().Add(24 * time.Hour)
	s.users = map[string]store.UserRecord{
		"explorer": {},
		"engineer": {IsPremium: true, PremiumExpiresAt: &future},
	}
	s.forcedOccupancy = &store.AdmissionOccupancy{CPUMillicores: 200, MemoryMi: 500}

	mk := func(ns, userID string, cpu, mem int64, created time.Time) {
		s.byNS[ns] = store.DeploymentRow{
			ID: ns, UserID: userID, Namespace: ns, Template: "url-shortener",
			Status: "queued", CreatedAt: created, ExpiresAt: time.Now().Add(time.Hour),
			Design: map[string]any{
				"template":  "url-shortener",
				"archetype": "standard",
				"nodes": []any{
					map[string]any{"id": "app", "type": "app-server", "name": "app", "config": map[string]any{}},
					map[string]any{"id": "db", "type": "db", "name": "db", "config": map[string]any{}},
				},
				"connections": []any{map[string]any{"sourceId": "app", "targetId": "db"}},
			},
		}
		s.active = append(s.active, store.ActiveDeploymentRow{
			ID: ns, Namespace: ns, Template: "url-shortener", Status: "queued",
			CreatedAt: created, ExpiresAt: time.Now().Add(time.Hour),
		})
		s.admittedByNS[ns] = store.AdmissionOccupancy{CPUMillicores: cpu, MemoryMi: mem}
	}
	// Explorer head blocks room; paid tail fits; light explorer also fits but should wait for paid pass.
	mk("heavy-explorer", "explorer", 900, 2000, time.Now().Add(-3*time.Minute))
	mk("paid-fits", "engineer", 100, 100, time.Now().Add(-2*time.Minute))
	mk("light-explorer", "explorer", 100, 100, time.Now().Add(-time.Minute))

	h := NewHandler(nil, s, nil, nil, nil, nil, nil, nil, nil)
	h.roomBudget = &admission.Config{CPUMillicores: 1000, MemoryMi: 3000}
	h.drainDeployQueue(context.Background())

	heavy, _ := s.GetDeployment(context.Background(), "heavy-explorer")
	paid, _ := s.GetDeployment(context.Background(), "paid-fits")
	light, _ := s.GetDeployment(context.Background(), "light-explorer")
	if heavy.Status != "queued" {
		t.Fatalf("heavy status = %q, want queued", heavy.Status)
	}
	if paid.Status != "provisioning" {
		t.Fatalf("paid status = %q, want provisioning", paid.Status)
	}
	if light.Status != "provisioning" {
		t.Fatalf("light status = %q, want provisioning", light.Status)
	}
}

func TestDrainDeployQueue_FirstFitPromotesLightBehindHeavy(t *testing.T) {
	t.Setenv("CLUSTER_DEPLOY_BUDGET_CPU", "1000m")
	t.Setenv("CLUSTER_DEPLOY_BUDGET_MEMORY", "3000Mi")

	s := newFakeDeploymentStore()
	s.users = map[string]store.UserRecord{"u1": {IsInternal: true}}
	// Occupancy nearly full: head needs 900m and will not fit; lighter tail can.
	s.forcedOccupancy = &store.AdmissionOccupancy{CPUMillicores: 200, MemoryMi: 500}

	mk := func(ns string, cpu, mem int64, created time.Time) {
		s.byNS[ns] = store.DeploymentRow{
			ID: ns, UserID: "u1", Namespace: ns, Template: "url-shortener",
			Status: "queued", CreatedAt: created, ExpiresAt: time.Now().Add(time.Hour),
			Design: map[string]any{
				"template":  "url-shortener",
				"archetype": "standard",
				"nodes": []any{
					map[string]any{"id": "app", "type": "app-server", "name": "app", "config": map[string]any{}},
					map[string]any{"id": "db", "type": "db", "name": "db", "config": map[string]any{}},
				},
				"connections": []any{map[string]any{"sourceId": "app", "targetId": "db"}},
			},
		}
		s.active = append(s.active, store.ActiveDeploymentRow{
			ID: ns, Namespace: ns, Template: "url-shortener", Status: "queued",
			CreatedAt: created, ExpiresAt: time.Now().Add(time.Hour),
		})
		s.admittedByNS[ns] = store.AdmissionOccupancy{CPUMillicores: cpu, MemoryMi: mem}
	}
	mk("heavy", 900, 2000, time.Now().Add(-2*time.Minute))
	mk("light", 100, 100, time.Now().Add(-time.Minute))

	h := NewHandler(nil, s, nil, nil, nil, nil, nil, nil, nil)
	h.roomBudget = &admission.Config{CPUMillicores: 1000, MemoryMi: 3000}
	h.drainDeployQueue(context.Background())

	heavy, _ := s.GetDeployment(context.Background(), "heavy")
	light, _ := s.GetDeployment(context.Background(), "light")
	if heavy.Status != "queued" {
		t.Fatalf("heavy status = %q, want queued (blocked)", heavy.Status)
	}
	if light.Status != "provisioning" {
		t.Fatalf("light status = %q, want provisioning (first-fit)", light.Status)
	}
}

func TestDrainDeployQueue_ClearsPoisonHeadThenPromotes(t *testing.T) {
	t.Setenv("CLUSTER_DEPLOY_BUDGET_CPU", "1000m")
	t.Setenv("CLUSTER_DEPLOY_BUDGET_MEMORY", "3000Mi")

	s := newFakeDeploymentStore()
	s.users = map[string]store.UserRecord{"u1": {IsInternal: true}}
	s.forcedOccupancy = &store.AdmissionOccupancy{CPUMillicores: 0, MemoryMi: 0}

	mk := func(ns string, cpu, mem int64, created time.Time) {
		s.byNS[ns] = store.DeploymentRow{
			ID: ns, UserID: "u1", Namespace: ns, Template: "url-shortener",
			Status: "queued", CreatedAt: created, ExpiresAt: time.Now().Add(time.Hour),
			Design: map[string]any{
				"template":  "url-shortener",
				"archetype": "standard",
				"nodes": []any{
					map[string]any{"id": "app", "type": "app-server", "name": "app", "config": map[string]any{}},
					map[string]any{"id": "db", "type": "db", "name": "db", "config": map[string]any{}},
				},
				"connections": []any{map[string]any{"sourceId": "app", "targetId": "db"}},
			},
		}
		s.active = append(s.active, store.ActiveDeploymentRow{
			ID: ns, Namespace: ns, Template: "url-shortener", Status: "queued",
			CreatedAt: created, ExpiresAt: time.Now().Add(time.Hour),
		})
		s.admittedByNS[ns] = store.AdmissionOccupancy{CPUMillicores: cpu, MemoryMi: mem}
	}
	// Poison: larger than empty room. Fit: fits empty and current occ.
	mk("poison", 2000, 100, time.Now().Add(-2*time.Minute))
	mk("ok", 100, 100, time.Now().Add(-time.Minute))

	h := NewHandler(nil, s, nil, nil, nil, nil, nil, nil, nil)
	h.roomBudget = &admission.Config{CPUMillicores: 1000, MemoryMi: 3000}
	h.drainDeployQueue(context.Background())

	poison, _ := s.GetDeployment(context.Background(), "poison")
	ok, _ := s.GetDeployment(context.Background(), "ok")
	if poison.Status != "terminated" {
		t.Fatalf("poison status = %q, want terminated", poison.Status)
	}
	if reason, _ := poison.MetricsSummary["reason"].(string); reason != "design_exceeds_cluster_capacity" {
		t.Fatalf("poison reason = %v, want design_exceeds_cluster_capacity summary=%v", reason, poison.MetricsSummary)
	}
	if ok.Status != "provisioning" {
		t.Fatalf("ok status = %q, want provisioning", ok.Status)
	}
}
