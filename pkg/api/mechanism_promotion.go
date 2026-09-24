package api

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/rules"
	"github.com/ywacch/cloud9/pkg/rules/verify"
	"github.com/ywacch/cloud9/pkg/store"
)

// recordInferredMechanismPromotions persists verified inferred claims for the promotion loop.
func recordInferredMechanismPromotions(
	ctx context.Context,
	st mechanismPromotionStore,
	template, namespace, userID string,
	claims []rules.Claim,
	ledger *metrics.Ledger,
) {
	if st == nil || ledger == nil {
		return
	}
	for _, claim := range claims {
		parts, ok := verify.PromotionSignaturePartsForClaim(claim, ledger)
		if !ok {
			continue
		}
		sig, err := verify.PromotionSignature(parts)
		if err != nil {
			log.Printf("promotion %s: signature: %v", namespace, err)
			continue
		}
		pm := claim.ProposedMechanism
		if pm == nil {
			continue
		}
		pmJSON, err := proposedMechanismToMap(pm)
		if err != nil {
			log.Printf("promotion %s: proposed mechanism: %v", namespace, err)
			continue
		}
		row := store.InferredMechanismPromotionRow{
			Signature:           sig,
			TriggerRule:         parts.Trigger,
			ConfigPrecondition:  parts.ConfigPrecondition,
			FactKinds:           parts.FactKinds,
			ProposedMechanism:   pmJSON,
			ClaimAspect:         string(claim.Aspect),
			ClaimText:           claim.Text,
			Template:            template,
			DeploymentNamespace: namespace,
			UserID:              userID,
		}
		if err := st.InsertInferredMechanismPromotion(ctx, row); err != nil {
			log.Printf("promotion %s: insert: %v", namespace, err)
		}
	}
}

func proposedMechanismToMap(pm *rules.ProposedMechanism) (map[string]any, error) {
	b, err := json.Marshal(pm)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}

type mechanismPromotionStore interface {
	InsertInferredMechanismPromotion(ctx context.Context, row store.InferredMechanismPromotionRow) error
	ListInferredMechanismPromotionSummaries(ctx context.Context, limit int) ([]store.InferredMechanismPromotionSummary, error)
}

func (h *Handler) handleListInferredMechanismPromotions(w http.ResponseWriter, r *http.Request) {
	if h == nil || h.store == nil {
		http.Error(w, "store unavailable", http.StatusServiceUnavailable)
		return
	}
	st, ok := h.store.(mechanismPromotionStore)
	if !ok {
		http.Error(w, "promotion store unavailable", http.StatusServiceUnavailable)
		return
	}
	limit := 500
	if q := r.URL.Query().Get("limit"); q != "" {
		if n, err := strconv.Atoi(q); err == nil && n > 0 {
			limit = n
		}
	}
	summaries, err := st.ListInferredMechanismPromotionSummaries(r.Context(), limit)
	if err != nil {
		log.Printf("list inferred mechanism promotions: %v", err)
		http.Error(w, "failed to load promotion log", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{"summaries": summaries})
}
