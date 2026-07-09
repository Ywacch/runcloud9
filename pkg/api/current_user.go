package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/ywacch/cloud9/pkg/store"
)

// currentUserLimitsResponse is the deployment policy caps returned by GET /me.
type currentUserLimitsResponse struct {
	MaxConcurrent        int   `json:"maxConcurrent"`
	SessionDefaultMins   int   `json:"sessionDefaultMins"`
	SessionCapMins       int   `json:"sessionCapMins"`
	AllTemplates         bool  `json:"allTemplates"`
	MaxAppServerReplicas int32 `json:"maxAppServerReplicas"`
	AllowLoadBalancer    bool  `json:"allowLoadBalancer"`
	AllowMQWorker        bool  `json:"allowMQWorker"`
	AllowProxyCache      bool  `json:"allowProxyCache"`
}

// currentUserResponse is the authenticated caller profile returned by GET /me.
type currentUserResponse struct {
	UserID           string                    `json:"userId"`
	IsInternal       bool                      `json:"isInternal"`
	IsPremium        bool                      `json:"isPremium"`
	PremiumExpiresAt *string                   `json:"premiumExpiresAt"`
	EffectiveTier    string                    `json:"effectiveTier"`
	QuotaTier        string                    `json:"quotaTier"`
	Limits           currentUserLimitsResponse `json:"limits"`
}

func buildCurrentUserResponse(userID string, user store.UserRecord) currentUserResponse {
	effectiveTier := store.ResolveEffectiveTier(user)
	limits := store.GetTierLimits(effectiveTier)
	return currentUserResponse{
		UserID:           userID,
		IsInternal:       user.IsInternal,
		IsPremium:        user.IsPremium,
		PremiumExpiresAt: formatPremiumExpiresAt(user.PremiumExpiresAt),
		EffectiveTier:    effectiveTier,
		QuotaTier:        store.ResourceQuotaTier(effectiveTier),
		Limits:           currentUserLimitsFromStore(limits),
	}
}

func currentUserLimitsFromStore(limits store.TierLimits) currentUserLimitsResponse {
	return currentUserLimitsResponse{
		MaxConcurrent:        limits.MaxConcurrent,
		SessionDefaultMins:   limits.SessionDefaultMins,
		SessionCapMins:       limits.SessionCapMins,
		AllTemplates:         limits.AllTemplates,
		MaxAppServerReplicas: limits.MaxAppServerReplicas,
		AllowLoadBalancer:    limits.AllowLoadBalancer,
		AllowMQWorker:        limits.AllowMQWorker,
		AllowProxyCache:      limits.AllowProxyCache,
	}
}

func formatPremiumExpiresAt(expires *time.Time) *string {
	if expires == nil {
		return nil
	}
	formatted := expires.UTC().Format(time.RFC3339)
	return &formatted
}

// handleGetCurrentUser returns the authenticated user's subscription tier and deployment limits.
func (h *Handler) handleGetCurrentUser(w http.ResponseWriter, r *http.Request) {
	userID := userIDFromContext(r.Context())
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "missing or invalid Authorization header")
		return
	}

	userRecord, err := h.loadUserRecord(r.Context(), userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load user")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(buildCurrentUserResponse(userID, userRecord))
}
