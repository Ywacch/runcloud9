package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/ywacch/cloud9/pkg/chaos"
	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/metrics"
)

type chaosRequest struct {
	Scenario          string `json:"scenario"`
	TargetComponent   string `json:"targetComponent"`
}

// handleChaos applies a one-shot chaos scenario to a running deployment.
func (h *Handler) handleChaos(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("id")
	if namespace == "" {
		writeError(w, http.StatusBadRequest, "missing deployment id")
		return
	}

	if h.store == nil {
		writeError(w, http.StatusServiceUnavailable, "deployment store unavailable")
		return
	}

	row := h.requireOwnership(w, r, namespace)
	if row == nil {
		return
	}

	if row.Status != "running" {
		writeError(w, http.StatusBadRequest, "chaos requires deployment status running")
		return
	}

	if h.clientset == nil {
		writeError(w, http.StatusServiceUnavailable, "chaos requires kubernetes (set DEPLOYER=k8s)")
		return
	}

	var req chaosRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
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

	scenario, err := chaos.NewScenario(req.Scenario, h.clientset)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := scenario.Apply(r.Context(), namespace, *design, req.TargetComponent); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if h.eventBus != nil {
		h.eventBus.Emit(namespace, metrics.Event{
			Timestamp:   time.Now(),
			Type:        "action",
			Source:      "chaos",
			Component:   req.TargetComponent,
			Title:       scenario.Description(req.TargetComponent),
			Description: req.Scenario,
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
