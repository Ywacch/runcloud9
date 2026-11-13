package api

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ywacch/cloud9/pkg/store"
)

// fleetSource is implemented by the session registry. Stores without it
// fall back to one ListActiveDeployments call.
type fleetSource interface {
	Fleet() []store.DeploymentRow
}

type fleetItem struct {
	ID        string `json:"id"`
	Namespace string `json:"namespace"`
	Status    string `json:"status"`
	Template  string `json:"template"`
	ExpiresAt string `json:"expiresAt"`
	CreatedAt string `json:"createdAt"`
}

func (h *Handler) handleFleet(w http.ResponseWriter, r *http.Request) {
	allowed := strings.TrimSpace(os.Getenv("FLEET_LIST_USER_ID"))
	if allowed == "" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	if userIDFromContext(r.Context()) != allowed {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}
	if h.store == nil {
		writeError(w, http.StatusServiceUnavailable, "deployment store unavailable")
		return
	}
	items, err := h.fleetItems(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(items)
}

func (h *Handler) fleetItems(ctx context.Context) ([]fleetItem, error) {
	if src, ok := h.store.(fleetSource); ok {
		rows := src.Fleet()
		out := make([]fleetItem, 0, len(rows))
		for _, row := range rows {
			out = append(out, fleetItemFromTimes(row.ID, row.Namespace, row.Status, row.Template, row.ExpiresAt, row.CreatedAt))
		}
		return out, nil
	}
	rows, err := h.store.ListActiveDeployments(ctx, "")
	if err != nil {
		return nil, err
	}
	out := make([]fleetItem, 0, len(rows))
	for _, row := range rows {
		out = append(out, fleetItemFromTimes(row.ID, row.Namespace, row.Status, row.Template, row.ExpiresAt, row.CreatedAt))
	}
	return out, nil
}

func fleetItemFromTimes(id, namespace, status, template string, expiresAt, createdAt time.Time) fleetItem {
	return fleetItem{
		ID:        id,
		Namespace: namespace,
		Status:    status,
		Template:  template,
		ExpiresAt: expiresAt.UTC().Format(time.RFC3339),
		CreatedAt: createdAt.UTC().Format(time.RFC3339),
	}
}
