package api

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ywacch/cloud9/pkg/store"
)

const (
	defaultDeploymentHistoryLimit = 20
	maxDeploymentHistoryLimit     = 50
)

var errInvalidDeploymentHistoryLimit = errors.New("limit must be a positive integer")

type deploymentHistoryResponse struct {
	ID              string                           `json:"id"`
	Namespace       string                           `json:"namespace"`
	Template        string                           `json:"template"`
	Archetype       string                           `json:"archetype"`
	Status          string                           `json:"status"`
	CreatedAt       string                           `json:"createdAt"`
	ExpiresAt       string                           `json:"expiresAt"`
	TerminatedAt    string                           `json:"terminatedAt,omitempty"`
	DurationSeconds *float64                         `json:"durationSeconds,omitempty"`
	PeakRPS         *float64                         `json:"peakRps,omitempty"`
	PeakP99Ms       *float64                         `json:"peakP99Ms,omitempty"`
	Topology        *store.DeploymentHistoryTopology `json:"topology,omitempty"`
}

func (h *Handler) handleListDeploymentHistory(w http.ResponseWriter, r *http.Request) {
	if h.store == nil {
		writeError(w, http.StatusServiceUnavailable, "deployment store unavailable")
		return
	}

	userID := strings.TrimSpace(userIDFromContext(r.Context()))
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "missing or invalid Authorization header")
		return
	}

	limit, err := deploymentHistoryLimit(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	rows, err := h.store.ListUserDeploymentHistory(r.Context(), userID, limit)
	if err != nil {
		log.Printf("list deployment history: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to list deployment history")
		return
	}

	resp := make([]deploymentHistoryResponse, 0, len(rows))
	for _, row := range rows {
		resp = append(resp, deploymentHistoryItem(row))
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func deploymentHistoryLimit(r *http.Request) (int, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("limit"))
	if raw == "" {
		return defaultDeploymentHistoryLimit, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 1 {
		return 0, errInvalidDeploymentHistoryLimit
	}
	if limit > maxDeploymentHistoryLimit {
		limit = maxDeploymentHistoryLimit
	}
	return limit, nil
}

func deploymentHistoryItem(row store.DeploymentHistoryRow) deploymentHistoryResponse {
	return deploymentHistoryResponse{
		ID:              row.ID,
		Namespace:       row.Namespace,
		Template:        row.Template,
		Archetype:       row.Archetype,
		Status:          row.Status,
		CreatedAt:       row.CreatedAt.UTC().Format(time.RFC3339),
		ExpiresAt:       row.ExpiresAt.UTC().Format(time.RFC3339),
		TerminatedAt:    summaryString(row.MetricsSummary, "terminated_at"),
		DurationSeconds: summaryNumber(row.MetricsSummary, "actual_duration_seconds"),
		PeakRPS:         summaryNumber(row.MetricsSummary, "peak_rps"),
		PeakP99Ms:       summaryNumber(row.MetricsSummary, "peak_p99_ms"),
		Topology:        row.Topology,
	}
}

func summaryString(summary map[string]any, key string) string {
	value, _ := summary[key].(string)
	return value
}

func summaryNumber(summary map[string]any, key string) *float64 {
	if summary == nil {
		return nil
	}
	switch value := summary[key].(type) {
	case float64:
		return &value
	case float32:
		number := float64(value)
		return &number
	case int:
		number := float64(value)
		return &number
	case int64:
		number := float64(value)
		return &number
	case json.Number:
		number, err := value.Float64()
		if err == nil {
			return &number
		}
	}
	return nil
}
