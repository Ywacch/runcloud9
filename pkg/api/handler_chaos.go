package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/ywacch/cloud9/pkg/chaos"
	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/store"
)

type chaosRequest struct {
	Scenario        string `json:"scenario"`
	TargetComponent string `json:"targetComponent"`
}

type chaosPartitionResponse struct {
	Scenario        string `json:"scenario"`
	TargetComponent string `json:"targetComponent"`
	ExpiresAt       string `json:"expiresAt,omitempty"`
}

func (h *Handler) requireChaosDeployment(w http.ResponseWriter, r *http.Request, namespace string) *store.DeploymentRow {
	if h.store == nil {
		writeError(w, http.StatusServiceUnavailable, "deployment store unavailable")
		return nil
	}
	row := h.requireOwnership(w, r, namespace)
	if row == nil {
		return nil
	}
	if row.Status != "running" {
		writeError(w, http.StatusBadRequest, "chaos requires deployment status running")
		return nil
	}
	if h.clientset == nil {
		writeError(w, http.StatusServiceUnavailable, "chaos requires kubernetes (set DEPLOYER=k8s)")
		return nil
	}
	return row
}

// handleChaosApply applies a chaos scenario to a running deployment.
func (h *Handler) handleChaosApply(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("id")
	if namespace == "" {
		writeError(w, http.StatusBadRequest, "missing deployment id")
		return
	}

	row := h.requireChaosDeployment(w, r, namespace)
	if row == nil {
		return
	}
	limitJSONBody(w, r)

	var req chaosRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if isBodyTooLarge(err) {
			writeError(w, http.StatusBadRequest, "request body too large")
			return
		}
		if errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, "request body required")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if req.Scenario == "" {
		writeError(w, http.StatusBadRequest, "scenario is required")
		return
	}
	if req.TargetComponent == "" {
		writeError(w, http.StatusBadRequest, "targetComponent is required")
		return
	}
	if req.TargetComponent == "loadgen" {
		writeError(w, http.StatusBadRequest, "cannot target loadgen")
		return
	}

	design, err := deployer.DesignFromMap(row.Design)
	if err != nil || design == nil {
		writeError(w, http.StatusBadRequest, "deployment design is invalid")
		return
	}

	if _, err := chaos.ResolveTarget(req.TargetComponent, *design); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	scenario, err := chaos.Resolve(req.Scenario, req.TargetComponent, *design, h.clientset)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := scenario.Apply(r.Context(), namespace, *design, req.TargetComponent); err != nil {
		if errors.Is(err, chaos.ErrPartitionAlreadyActive) {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if req.Scenario == "pod_kill" && req.TargetComponent == "db" {
		if dbCfg, ok := deployer.DatabaseNodeConfig(*design); ok && deployer.RequiresPostgresSynchronousReplication(dbCfg) {
			h.reconcilePostgresSyncAfterChaos(namespace, dbCfg)
		}
	}

	if scenario.Stateful() {
		h.refreshChaosPartitionTTL(r.Context(), namespace)
	}

	if h.eventBus != nil {
		h.eventBus.Emit(namespace, metrics.Event{
			Timestamp:   time.Now(),
			Type:        "action",
			Source:      "chaos",
			Component:   req.TargetComponent,
			Kind:        "chaos_inject",
			Audience:    "both",
			Title:       scenario.Description(req.TargetComponent),
			Description: req.Scenario,
			Detail: map[string]string{
				"scenario":        req.Scenario,
				"targetComponent": req.TargetComponent,
			},
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":          "applied",
		"scenario":        req.Scenario,
		"targetComponent": req.TargetComponent,
	})
}

// handleChaosHeal removes active partition policies from a running deployment.
func (h *Handler) handleChaosHeal(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("id")
	if namespace == "" {
		writeError(w, http.StatusBadRequest, "missing deployment id")
		return
	}
	if h.requireChaosDeployment(w, r, namespace) == nil {
		return
	}

	healed, err := chaos.HealPartitions(r.Context(), h.clientset, namespace)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.cancelChaosPartitionTTL(namespace)
	for _, part := range healed {
		h.emitChaosHeal(namespace, part, "manual")
	}

	resp := make([]chaosPartitionResponse, 0, len(healed))
	for _, part := range healed {
		item := chaosPartitionResponse{
			Scenario:        part.Scenario,
			TargetComponent: part.TargetComponent,
		}
		if !part.ExpiresAt.IsZero() {
			item.ExpiresAt = part.ExpiresAt.UTC().Format(time.RFC3339)
		}
		resp = append(resp, item)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":     "healed",
		"partitions": resp,
	})
}

// handleChaosList returns active partition policies for page reload.
func (h *Handler) handleChaosList(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("id")
	if namespace == "" {
		writeError(w, http.StatusBadRequest, "missing deployment id")
		return
	}
	if h.requireChaosDeployment(w, r, namespace) == nil {
		return
	}

	active, err := chaos.ListActivePartitions(r.Context(), h.clientset, namespace)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	resp := make([]chaosPartitionResponse, 0, len(active))
	for _, part := range active {
		item := chaosPartitionResponse{
			Scenario:        part.Scenario,
			TargetComponent: part.TargetComponent,
		}
		if !part.ExpiresAt.IsZero() {
			item.ExpiresAt = part.ExpiresAt.UTC().Format(time.RFC3339)
		}
		resp = append(resp, item)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"partitions": resp,
	})
}

// reconcilePostgresSyncAfterChaos re-applies synchronous replication GUCs when a primary
// pod recreate wipes postgresql.auto.conf from emptyDir PGDATA.
func (h *Handler) reconcilePostgresSyncAfterChaos(namespace string, dbConfig map[string]any) {
	if h == nil || h.clientset == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		defer cancel()

		err := deployer.ReconcilePostgresSynchronousReplication(ctx, h.clientset, namespace, dbConfig)
		if h.eventBus == nil {
			if err != nil {
				slog.Warn("postgres sync reconcile failed", "namespace", namespace, "error", err)
			}
			return
		}
		if err != nil {
			h.eventBus.Emit(namespace, metrics.Event{
				Timestamp:   time.Now(),
				Type:        "action",
				Source:      "chaos",
				Component:   "db",
				Kind:        "postgres_sync_reconcile_failed",
				Audience:    "both",
				Title:       "Postgres sync settings not restored",
				Description: err.Error(),
			})
			slog.Warn("postgres sync reconcile failed", "namespace", namespace, "error", err)
			return
		}
		h.eventBus.Emit(namespace, metrics.Event{
			Timestamp:   time.Now(),
			Type:        "action",
			Source:      "chaos",
			Component:   "db",
			Kind:        "postgres_sync_reconcile_ok",
			Audience:    "both",
			Title:       "Postgres synchronous replication restored",
			Description: "Re-applied synchronous_standby_names after primary pod recreate",
		})
	}()
}
