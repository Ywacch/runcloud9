package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/rules"
)

func (h *Handler) handleUpdateReplicaLag(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("id")
	if namespace == "" {
		writeError(w, http.StatusBadRequest, "missing deployment id")
		return
	}
	if h.store == nil {
		writeError(w, http.StatusServiceUnavailable, "store unavailable")
		return
	}
	row := h.requireOwnership(w, r, namespace)
	if row == nil {
		return
	}
	if h.clientset == nil {
		writeError(w, http.StatusServiceUnavailable, "kubernetes unavailable")
		return
	}
	limitJSONBody(w, r)

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var body struct {
		ReplicaApplyDelayMs json.RawMessage `json:"replicaApplyDelayMs"`
	}
	if err := dec.Decode(&body); err != nil {
		if isBodyTooLarge(err) {
			writeError(w, http.StatusBadRequest, "request body too large")
			return
		}
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid json: %v", err))
		return
	}
	if len(body.ReplicaApplyDelayMs) == 0 {
		writeError(w, http.StatusBadRequest, "replicaApplyDelayMs required")
		return
	}

	design, err := deployer.DesignFromMap(row.Design)
	if err != nil || design == nil {
		writeError(w, http.StatusInternalServerError, "invalid stored design")
		return
	}
	dbNode := firstDBNode(design)
	if dbNode == nil {
		writeError(w, http.StatusBadRequest, "deployment has no database node")
		return
	}
	readReplicas := int(deployer.ParseReadReplicaCount(dbNode.Config))
	if readReplicas < 1 {
		writeError(w, http.StatusBadRequest, "deployment has no read replicas")
		return
	}

	cfg := cloneConfigMap(dbNode.Config)
	var parsed any
	if err := json.Unmarshal(body.ReplicaApplyDelayMs, &parsed); err != nil {
		writeError(w, http.StatusBadRequest, "invalid replicaApplyDelayMs")
		return
	}
	cfg["replicaApplyDelayMs"] = parsed
	delays, err := deployer.ParseReplicaApplyDelayMs(cfg, readReplicas)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := deployer.ValidateReplicaApplyDelay(cfg, delays); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	hosts := deployer.DBReplicaHostnamesFromConfig(dbNode.Config)
	if len(hosts) != len(delays) {
		writeError(w, http.StatusInternalServerError, "replica host count mismatch")
		return
	}

	ctx := r.Context()
	for i, host := range hosts {
		admin := deployer.NewPostgresPodAdmin(h.clientset, namespace, host, "postgres-replica")
		ms := delays[i]
		alter := fmt.Sprintf("ALTER SYSTEM SET recovery_min_apply_delay = '%dms'", ms)
		if _, err := admin.ExecSQL(ctx, alter); err != nil {
			writeError(w, http.StatusBadGateway, fmt.Sprintf("apply delay on %s: %v", host, err))
			return
		}
		if _, err := admin.ExecSQL(ctx, "SELECT pg_reload_conf()"); err != nil {
			writeError(w, http.StatusBadGateway, fmt.Sprintf("reload on %s: %v", host, err))
			return
		}
	}

	if h.eventBus != nil {
		detail := map[string]string{
			"fidelity": rules.FidelitySimulated,
		}
		for i, host := range hosts {
			detail[host] = strconv.Itoa(delays[i])
		}
		h.eventBus.Emit(namespace, metrics.Event{
			Timestamp:   time.Now(),
			Type:        "workload",
			Source:      "api",
			Kind:        "lag_dial_change",
			Visibility:  "all",
			Title:       "Replica lag dial updated",
			Description: formatReplicaLagDescription(hosts, delays),
			Detail:      detail,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":              "updated",
		"replicaApplyDelayMs": delays,
	})
}

func firstDBNode(design *deployer.DeploymentDesign) *deployer.DeploymentNode {
	if design == nil {
		return nil
	}
	for i := range design.Nodes {
		if design.Nodes[i].Type == "db" {
			return &design.Nodes[i]
		}
	}
	return nil
}

func cloneConfigMap(in map[string]any) map[string]any {
	if in == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func formatReplicaLagDescription(hosts []string, delays []int) string {
	parts := make([]string, 0, len(hosts))
	for i, host := range hosts {
		ms := 0
		if i < len(delays) {
			ms = delays[i]
		}
		parts = append(parts, fmt.Sprintf("%s=%dms", host, ms))
	}
	return strings.Join(parts, ", ")
}
