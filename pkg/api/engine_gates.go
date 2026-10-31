package api

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/ywacch/cloud9/pkg/engines"
	"github.com/ywacch/cloud9/pkg/store"
)

// EnginePairGate is the tier gate for one job-engine pair in the picker.
type EnginePairGate string

const (
	EnginePairGateNone EnginePairGate = "none"
	EnginePairGateTier EnginePairGate = "tier"
)

// EngineGateEntry is one row in GET /me/engine-gates.
type EngineGateEntry struct {
	Job    string         `json:"job"`
	Engine string         `json:"engine"`
	Gate   EnginePairGate `json:"gate"`
}

type engineGatesResponse struct {
	Pairs []EngineGateEntry `json:"pairs"`
}

// buildEngineGates returns gate state for every catalog pair.
func buildEngineGates(limits store.TierLimits, cat *engines.Catalog) []EngineGateEntry {
	if cat == nil {
		return nil
	}
	defaults := defaultEnginesByJob(cat)
	weights := engineWeights(cat)
	out := make([]EngineGateEntry, 0, len(cat.Pairs))
	for _, p := range cat.Pairs {
		gate := pairGate(limits, p, defaults[p.Job], weights[p.Engine])
		out = append(out, EngineGateEntry{
			Job:    p.Job,
			Engine: p.Engine,
			Gate:   gate,
		})
	}
	return out
}

func pairGate(limits store.TierLimits, pair engines.Pair, defaultEngine, weight string) EnginePairGate {
	if pair.Fit == engines.FitImpossible {
		return EnginePairGateNone
	}
	if weight == "heavy" && !limits.AllowHeavyEngines {
		return EnginePairGateTier
	}
	if !limits.AllowEngineSwap {
		def := strings.ToLower(strings.TrimSpace(defaultEngine))
		eng := strings.ToLower(strings.TrimSpace(pair.Engine))
		if def != "" && eng != def {
			return EnginePairGateTier
		}
	}
	return EnginePairGateNone
}

func defaultEnginesByJob(cat *engines.Catalog) map[string]string {
	out := make(map[string]string)
	for _, p := range cat.Pairs {
		if p.Fit != engines.FitNatural || p.Build != engines.BuildAvailable {
			continue
		}
		if _, ok := out[p.Job]; !ok {
			out[p.Job] = p.Engine
		}
	}
	return out
}

func engineWeights(cat *engines.Catalog) map[string]string {
	out := make(map[string]string, len(cat.Engines))
	for _, e := range cat.Engines {
		out[e.ID] = e.Weight
	}
	return out
}

func (h *Handler) handleEngineGates(w http.ResponseWriter, r *http.Request) {
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
	limits := store.GetTierLimits(store.ResolveEffectiveTier(userRecord))
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(engineGatesResponse{
		Pairs: buildEngineGates(limits, engines.Default()),
	})
}
