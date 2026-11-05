package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// InferredMechanismPromotionRow is one persisted inferred claim that passed verification.
type InferredMechanismPromotionRow struct {
	ID                  string         `json:"id"`
	Signature           string         `json:"signature"`
	TriggerRule         string         `json:"trigger_rule"`
	ConfigPrecondition  string         `json:"config_precondition"`
	FactKinds           []string       `json:"fact_kinds"`
	ProposedMechanism   map[string]any `json:"proposed_mechanism"`
	ClaimAspect         string         `json:"claim_aspect"`
	ClaimText           string         `json:"claim_text"`
	Template            string         `json:"template"`
	DeploymentNamespace string         `json:"deployment_namespace"`
	UserID              string         `json:"user_id,omitempty"`
	CreatedAt           time.Time      `json:"created_at"`
}

// InferredMechanismPromotionSummary groups log rows by signature for manual review.
type InferredMechanismPromotionSummary struct {
	Signature          string         `json:"signature"`
	TriggerRule        string         `json:"trigger_rule"`
	ConfigPrecondition string         `json:"config_precondition"`
	FactKinds          []string       `json:"fact_kinds"`
	OccurrenceCount    int            `json:"occurrence_count"`
	FirstSeenAt        time.Time      `json:"first_seen_at"`
	LastSeenAt         time.Time      `json:"last_seen_at"`
	SampleMechanism    map[string]any `json:"sample_proposed_mechanism,omitempty"`
	SampleClaimText    string         `json:"sample_claim_text,omitempty"`
}

// InsertInferredMechanismPromotion appends one verified inferred claim row.
func (s *DeploymentStore) InsertInferredMechanismPromotion(ctx context.Context, row InferredMechanismPromotionRow) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("store.InsertInferredMechanismPromotion: %w", err)
	}
	if s == nil || s.client == nil {
		return fmt.Errorf("store.InsertInferredMechanismPromotion: store unavailable")
	}
	if row.Signature == "" || row.TriggerRule == "" || row.ProposedMechanism == nil {
		return fmt.Errorf("store.InsertInferredMechanismPromotion: missing required fields")
	}
	kinds := row.FactKinds
	if kinds == nil {
		kinds = []string{}
	}
	payload := map[string]any{
		"signature":            row.Signature,
		"trigger_rule":         row.TriggerRule,
		"config_precondition":  row.ConfigPrecondition,
		"fact_kinds":           kinds,
		"proposed_mechanism":   row.ProposedMechanism,
		"claim_aspect":         row.ClaimAspect,
		"claim_text":           row.ClaimText,
		"template":             row.Template,
		"deployment_namespace": row.DeploymentNamespace,
	}
	if row.UserID != "" {
		payload["user_id"] = row.UserID
	}
	s.observe("insert_inferred_mechanism_promotion")
	_, _, err := s.client.From("inferred_mechanism_promotions").Insert([]map[string]any{payload}, false, "", "", "").Execute()
	if err != nil {
		return fmt.Errorf("store.InsertInferredMechanismPromotion: %w", err)
	}
	return nil
}

// ListInferredMechanismPromotionSummaries returns signature groups for review.
// limit caps raw rows scanned before aggregation (default 2000 when limit <= 0).
func (s *DeploymentStore) ListInferredMechanismPromotionSummaries(ctx context.Context, limit int) ([]InferredMechanismPromotionSummary, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("store.ListInferredMechanismPromotionSummaries: %w", err)
	}
	if s == nil || s.client == nil {
		return nil, fmt.Errorf("store.ListInferredMechanismPromotionSummaries: store unavailable")
	}
	if limit <= 0 {
		limit = 2000
	}
	s.observe("list_inferred_mechanism_promotion_summaries")
	data, _, err := s.client.From("inferred_mechanism_promotions").
		Select("signature,trigger_rule,config_precondition,fact_kinds,proposed_mechanism,claim_text,created_at", "exact", false).
		Limit(limit, "").
		Execute()
	if err != nil {
		return nil, fmt.Errorf("store.ListInferredMechanismPromotionSummaries: %w", err)
	}
	var rows []InferredMechanismPromotionRow
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, fmt.Errorf("store.ListInferredMechanismPromotionSummaries: parse: %w", err)
	}
	return aggregatePromotionSummaries(rows), nil
}

func aggregatePromotionSummaries(rows []InferredMechanismPromotionRow) []InferredMechanismPromotionSummary {
	bySig := make(map[string]*InferredMechanismPromotionSummary)
	for _, row := range rows {
		if row.Signature == "" {
			continue
		}
		sum, ok := bySig[row.Signature]
		if !ok {
			sum = &InferredMechanismPromotionSummary{
				Signature:          row.Signature,
				TriggerRule:        row.TriggerRule,
				ConfigPrecondition: row.ConfigPrecondition,
				FactKinds:          append([]string(nil), row.FactKinds...),
				OccurrenceCount:    0,
				FirstSeenAt:        row.CreatedAt,
				LastSeenAt:         row.CreatedAt,
				SampleMechanism:    row.ProposedMechanism,
				SampleClaimText:    row.ClaimText,
			}
			bySig[row.Signature] = sum
		}
		sum.OccurrenceCount++
		if row.CreatedAt.Before(sum.FirstSeenAt) {
			sum.FirstSeenAt = row.CreatedAt
		}
		if row.CreatedAt.After(sum.LastSeenAt) {
			sum.LastSeenAt = row.CreatedAt
		}
	}
	out := make([]InferredMechanismPromotionSummary, 0, len(bySig))
	for _, sum := range bySig {
		out = append(out, *sum)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].OccurrenceCount != out[j].OccurrenceCount {
			return out[i].OccurrenceCount > out[j].OccurrenceCount
		}
		return out[i].LastSeenAt.After(out[j].LastSeenAt)
	})
	return out
}
