package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/rules"
	"github.com/ywacch/cloud9/pkg/store"
)

type stubPromotionStore struct {
	inserted  []store.InferredMechanismPromotionRow
	summaries []store.InferredMechanismPromotionSummary
}

func (s *stubPromotionStore) InsertInferredMechanismPromotion(_ context.Context, row store.InferredMechanismPromotionRow) error {
	s.inserted = append(s.inserted, row)
	return nil
}

func (s *stubPromotionStore) ListInferredMechanismPromotionSummaries(context.Context, int) ([]store.InferredMechanismPromotionSummary, error) {
	return s.summaries, nil
}

func TestRecordInferredMechanismPromotions_insertsInferredWithMechanism(t *testing.T) {
	st := &stubPromotionStore{}
	ledger := metrics.BuildLedger(metrics.LedgerInput{})
	claim := rules.Claim{
		Aspect:    rules.AspectWhy,
		Text:      "Async promote lost commits.",
		Grounding: rules.GroundingInferred,
		FactRefs:  []string{"fact:rule:failover_write_loss"},
		ProposedMechanism: &rules.ProposedMechanism{
			Trigger:            "failover_write_loss",
			ConfigPrecondition: "replicationMode: async",
		},
	}
	recordInferredMechanismPromotions(context.Background(), st, "url-shortener", "ns-1", "user-1", []rules.Claim{claim}, ledger)
	if len(st.inserted) != 1 {
		t.Fatalf("inserted %d", len(st.inserted))
	}
	if st.inserted[0].TriggerRule != "failover_write_loss" || st.inserted[0].Signature == "" {
		t.Fatalf("row %+v", st.inserted[0])
	}
}

func TestHandleListInferredMechanismPromotions(t *testing.T) {
	st := newFakeDeploymentStore()
	st.promotionSummaries = []store.InferredMechanismPromotionSummary{{
		Signature:       "abc",
		TriggerRule:     "failover_write_loss",
		OccurrenceCount: 2,
	}}
	h := &Handler{store: st}
	req := httptest.NewRequest(http.MethodGet, "/meta/inferred-mechanism-promotions", nil)
	rec := httptest.NewRecorder()
	h.handleListInferredMechanismPromotions(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	var body map[string][]store.InferredMechanismPromotionSummary
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body["summaries"]) != 1 {
		t.Fatalf("body %v", body)
	}
}
