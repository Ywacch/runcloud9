package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ywacch/cloud9/pkg/chaos"
	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/store"
)

type chaosRequest struct {
	Scenario        string `json:"scenario"`
	TargetComponent string `json:"targetComponent"`
	TargetPod       string `json:"targetPod"`
}

type chaosPartitionResponse struct {
	Scenario        string `json:"scenario"`
	TargetComponent string `json:"targetComponent"`
	ExpiresAt       string `json:"expiresAt,omitempty"`
	TargetPod       string `json:"targetPod,omitempty"`
}

func chaosPartitionItem(part chaos.ActivePartition) chaosPartitionResponse {
	item := chaosPartitionResponse{
		Scenario:        part.Scenario,
		TargetComponent: part.TargetComponent,
		TargetPod:       part.ReplicaPod,
	}
	if !part.ExpiresAt.IsZero() {
		item.ExpiresAt = part.ExpiresAt.UTC().Format(time.RFC3339)
	}
	return item
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
	req.TargetPod = strings.TrimSpace(req.TargetPod)
	if req.TargetPod != "" && req.Scenario != "stall_replica" {
		writeError(w, http.StatusBadRequest, "targetPod is only valid for stall_replica")
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
	if part, ok := scenario.(*chaos.PartitionScenario); ok {
		part.TargetPod = req.TargetPod
	}

	if err := scenario.Apply(r.Context(), namespace, *design, req.TargetComponent); err != nil {
		if errors.Is(err, chaos.ErrPartitionAlreadyActive) {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		if errors.Is(err, chaos.ErrInvalidStallTarget) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	dbCfg, _ := deployer.DatabaseNodeConfig(*design)
	if req.Scenario == "pod_kill" && req.TargetComponent == "db" {
		if deployer.RequiresPostgresSynchronousReplication(dbCfg) {
			h.reconcilePostgresSyncAfterChaos(namespace, dbCfg)
		}
	}

	if scenario.Stateful() {
		h.refreshChaosPartitionTTL(r.Context(), namespace)
	}

	var promotedPod string
	if h.eventBus != nil {
		now := time.Now()
		detail := map[string]string{
			"scenario":        req.Scenario,
			"targetComponent": req.TargetComponent,
			"fidelity":        chaos.ScenarioFidelity(req.Scenario),
		}
		if part, ok := scenario.(*chaos.PartitionScenario); ok && part.TargetPod != "" {
			detail["targetPod"] = part.TargetPod
		}
		if ps, ok := scenario.(*chaos.PromoteScenario); ok {
			promotedPod = ps.Result.PromotedPod
			mergePromotionChaosDetail(detail, ps.Result, dbCfg)
			if ev := failoverWriteLossEvent(now, req.Scenario, ps.Result, dbCfg); ev != nil {
				h.eventBus.Emit(namespace, *ev)
			}
		}
		h.eventBus.Emit(namespace, metrics.Event{
			Timestamp:   now,
			Type:        "action",
			Source:      "chaos",
			Component:   req.TargetComponent,
			Kind:        "chaos_inject",
			Visibility:  "all",
			Title:       scenario.Description(req.TargetComponent),
			Description: req.Scenario,
			Detail:      detail,
		})
	} else if ps, ok := scenario.(*chaos.PromoteScenario); ok {
		promotedPod = ps.Result.PromotedPod
	}
	if promotedPod != "" {
		h.followOrRecloneThenMaybeSync(namespace, promotedPod, dbCfg)
	}

	applied := map[string]any{
		"status":          "applied",
		"scenario":        req.Scenario,
		"targetComponent": req.TargetComponent,
	}
	if part, ok := scenario.(*chaos.PartitionScenario); ok && part.TargetPod != "" {
		applied["targetPod"] = part.TargetPod
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(applied)
}

// handleChaosHeal reverts active partitions on a running deployment.
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
		resp = append(resp, chaosPartitionItem(part))
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":     "healed",
		"partitions": resp,
	})
}

// handleChaosList returns active partitions for page reload.
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
		resp = append(resp, chaosPartitionItem(part))
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"partitions": resp,
	})
}

func failoverWriteLossEvent(now time.Time, scenario string, result deployer.PromotionResult, dbCfg map[string]any) *metrics.Event {
	replicationMode := ""
	syncCommitLevel := ""
	if dbCfg != nil {
		replicationMode = deployer.ReplicationModeLabel(dbCfg)
		syncCommitLevel = deployer.SyncCommitLevelLabel(dbCfg)
	}
	return metrics.FailoverWriteLossEvent(
		now,
		scenario,
		result.PromotedPod,
		result.SyncState,
		result.WalBytesLost,
		replicationMode,
		syncCommitLevel,
	)
}

func mergePromotionChaosDetail(detail map[string]string, result deployer.PromotionResult, dbCfg map[string]any) {
	detail["promotedPod"] = result.PromotedPod
	detail["candidateSyncState"] = result.SyncState
	detail["candidateLagBytes"] = strconv.FormatInt(result.LagBytes, 10)
	detail["walBytesLost"] = strconv.FormatInt(result.WalBytesLost, 10)
	if dbCfg != nil {
		detail["replicationMode"] = deployer.ReplicationModeLabel(dbCfg)
		detail["syncCommitLevel"] = deployer.SyncCommitLevelLabel(dbCfg)
	}
}

func (h *Handler) followOrRecloneThenMaybeSync(namespace, promotedPod string, dbConfig map[string]any) {
	if h == nil || h.clientset == nil || promotedPod == "" {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		result, err := deployer.FollowOrRecloneStandbys(ctx, h.clientset, namespace, promotedPod, deployer.StandbyFollowOpts{})
		if err != nil {
			slog.Warn("standby follow-or-reclone failed", "namespace", namespace, "promotedPod", promotedPod, "error", err)
		}

		syncRestored := false
		minReady := int32(result.StreamingLeftovers)
		if deployer.RequiresPostgresSynchronousReplication(dbConfig) && minReady >= 1 {
			syncErr := deployer.ReconcilePostgresSynchronousReplicationOnPodWithMinReady(
				ctx, h.clientset, namespace, promotedPod, dbConfig, minReady,
			)
			syncRestored = syncErr == nil
			if syncErr != nil && h.eventBus != nil {
				h.eventBus.Emit(namespace, metrics.Event{
					Timestamp:   time.Now(),
					Type:        "action",
					Source:      "chaos",
					Component:   "db",
					Kind:        "postgres_sync_reconcile_failed",
					Visibility:  "all",
					Title:       "Postgres sync settings not restored",
					Description: syncErr.Error(),
				})
				slog.Warn("postgres sync reconcile after promotion failed", "namespace", namespace, "error", syncErr)
			} else if syncRestored && h.eventBus != nil {
				h.eventBus.Emit(namespace, metrics.Event{
					Timestamp:   time.Now(),
					Type:        "action",
					Source:      "chaos",
					Component:   "db",
					Kind:        "postgres_sync_reconcile_ok",
					Visibility:  "all",
					Title:       "Postgres synchronous replication restored",
					Description: "Re-applied synchronous_standby_names after replica promotion",
				})
			}
		}

		if h.eventBus != nil {
			detail := map[string]string{
				"status":       result.Status,
				"promotedPod":  promotedPod,
				"syncRestored": strconv.FormatBool(syncRestored),
			}
			if len(result.ReclonedPods) > 0 {
				detail["reclonedPods"] = strings.Join(result.ReclonedPods, ",")
			}
			if len(result.PendingPods) > 0 {
				detail["pendingPods"] = strings.Join(result.PendingPods, ",")
			}
			h.eventBus.Emit(namespace, metrics.Event{
				Timestamp:   time.Now(),
				Type:        "action",
				Source:      "chaos",
				Component:   "db",
				Kind:        "standby_reclone",
				Visibility:  "all",
				Title:       "Standby rebuilt after timeline fork",
				Description: "standby_reclone",
				Detail:      detail,
			})
		}
	}()
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
				Visibility:  "all",
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
			Visibility:  "all",
			Title:       "Postgres synchronous replication restored",
			Description: "Re-applied synchronous_standby_names after primary pod recreate",
		})
	}()
}
