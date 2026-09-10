package api

import (
	"fmt"
	"net/http"

	"github.com/ywacch/cloud9/pkg/loadgen"
	"github.com/ywacch/cloud9/pkg/store"
)

// validateLoadgenRPSForRequest enforces global and subscription-tier live RPS caps.
// Returns false after writing an HTTP error response.
func (h *Handler) validateLoadgenRPSForRequest(w http.ResponseWriter, r *http.Request, rps int) bool {
	tierMax, err := h.loadgenRPSCapForRequest(r)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load user")
		return false
	}
	if err := loadgen.ValidateLiveRPSForTier(rps, tierMax); err != nil {
		status := http.StatusBadRequest
		if tierMax > 0 && rps > tierMax {
			status = http.StatusForbidden
		}
		writeError(w, status, err.Error())
		return false
	}
	return true
}

func (h *Handler) loadgenRPSCapForRequest(r *http.Request) (int, error) {
	if h.store == nil {
		return 0, nil
	}
	userID := userIDFromContext(r.Context())
	if userID == "" {
		return 0, nil
	}
	userRecord, err := h.loadUserRecord(r.Context(), userID)
	if err != nil {
		return 0, fmt.Errorf("load user: %w", err)
	}
	return store.GetTierLimits(store.ResolveEffectiveTier(userRecord)).MaxLoadgenRPS, nil
}
