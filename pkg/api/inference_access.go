package api

import (
	"context"
	"log"

	"github.com/ywacch/cloud9/pkg/store"
)

// inferenceAllowed is true only for the internal tier.
func inferenceAllowed(rec store.UserRecord) bool {
	return store.ResolveEffectiveTier(rec) == store.TierInternal
}

// inferenceEnabledForUser loads the owner and fails closed on any lookup error.
func (h *Handler) inferenceEnabledForUser(ctx context.Context, userID string) bool {
	if h == nil || h.store == nil || userID == "" {
		return false
	}
	rec, err := h.store.GetUser(ctx, userID)
	if err != nil {
		log.Printf("inference access %s: %v", userID, err)
		return false
	}
	return inferenceAllowed(rec)
}
