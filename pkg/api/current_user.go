package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/ywacch/cloud9/pkg/store"
)

// currentUserLimitsResponse is the deployment policy caps returned by GET /me.
type currentUserLimitsResponse struct {
	MaxConcurrent        int    `json:"maxConcurrent"`
	SessionDefaultMins   int    `json:"sessionDefaultMins"`
	SessionCapMins       int    `json:"sessionCapMins"`
	MaxSessionsPerMonth  *int   `json:"maxSessionsPerMonth"`
	AllTemplates         bool   `json:"allTemplates"`
	MaxAppServerReplicas int32  `json:"maxAppServerReplicas"`
	AllowLoadBalancer    bool   `json:"allowLoadBalancer"`
	AllowMQWorker        bool   `json:"allowMQWorker"`
	AllowProxyCache      bool   `json:"allowProxyCache"`
	AllowAutoscale       bool   `json:"allowAutoscale"`
	MaxInstanceSize      string `json:"maxInstanceSize"`
	MaxLoadgenRps        int    `json:"maxLoadgenRps"`
	AllowEngineSwap      bool   `json:"allowEngineSwap"`
	AllowHeavyEngines    bool   `json:"allowHeavyEngines"`
}

// currentUserResponse is the authenticated caller profile returned by GET /me.
type currentUserResponse struct {
	UserID                     string                    `json:"userId"`
	IsInternal                 bool                      `json:"isInternal"`
	IsPremium                  bool                      `json:"isPremium"`
	PremiumExpiresAt           *string                   `json:"premiumExpiresAt"`
	EffectiveTier              string                    `json:"effectiveTier"`
	QuotaTier                  string                    `json:"quotaTier"`
	Limits                     currentUserLimitsResponse `json:"limits"`
	SessionsUsedThisMonth      int                       `json:"sessionsUsedThisMonth"`
	SessionsRemainingThisMonth *int                      `json:"sessionsRemainingThisMonth"`
}

func buildCurrentUserResponse(userID string, user store.UserRecord, sessionsUsed int) currentUserResponse {
	effectiveTier := store.ResolveEffectiveTier(user)
	limits := store.GetTierLimits(effectiveTier)
	resp := currentUserResponse{
		UserID:                userID,
		IsInternal:            user.IsInternal,
		IsPremium:             user.IsPremium,
		PremiumExpiresAt:      formatPremiumExpiresAt(user.PremiumExpiresAt),
		EffectiveTier:         effectiveTier,
		QuotaTier:             store.ResourceQuotaTier(effectiveTier),
		Limits:                currentUserLimitsFromStore(limits),
		SessionsUsedThisMonth: sessionsUsed,
	}
	if limits.MaxSessionsPerMonth > 0 {
		remaining := limits.MaxSessionsPerMonth - sessionsUsed
		if remaining < 0 {
			remaining = 0
		}
		resp.SessionsRemainingThisMonth = &remaining
	}
	return resp
}

func currentUserLimitsFromStore(limits store.TierLimits) currentUserLimitsResponse {
	out := currentUserLimitsResponse{
		MaxConcurrent:        limits.MaxConcurrent,
		SessionDefaultMins:   limits.SessionDefaultMins,
		SessionCapMins:       limits.SessionCapMins,
		AllTemplates:         limits.AllTemplates,
		MaxAppServerReplicas: limits.MaxAppServerReplicas,
		AllowLoadBalancer:    limits.AllowLoadBalancer,
		AllowMQWorker:        limits.AllowMQWorker,
		AllowProxyCache:      limits.AllowProxyCache,
		AllowAutoscale:       limits.AllowAutoscale,
		MaxInstanceSize:      limits.MaxInstanceSize,
		MaxLoadgenRps:        limits.MaxLoadgenRPS,
		AllowEngineSwap:      limits.AllowEngineSwap,
		AllowHeavyEngines:    limits.AllowHeavyEngines,
	}
	if limits.MaxSessionsPerMonth > 0 {
		max := limits.MaxSessionsPerMonth
		out.MaxSessionsPerMonth = &max
	}
	return out
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

	sessionsUsed := 0
	limits := store.GetTierLimits(store.ResolveEffectiveTier(userRecord))
	if limits.MaxSessionsPerMonth > 0 && h.store != nil {
		n, err := h.store.CountUserDeploymentsSince(r.Context(), userID, store.StartOfUTCMonth(time.Now()))
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to check monthly session quota")
			return
		}
		sessionsUsed = n
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(buildCurrentUserResponse(userID, userRecord, sessionsUsed))
}
