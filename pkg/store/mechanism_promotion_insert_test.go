package store

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestInsertInferredMechanismPromotion(t *testing.T) {
	t.Parallel()
	var inserted map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rest/v1/inferred_mechanism_promotions" {
			t.Errorf("path %q", r.URL.Path)
		}
		defer r.Body.Close()
		var rows []map[string]any
		if err := json.NewDecoder(r.Body).Decode(&rows); err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 {
			t.Fatalf("rows %d", len(rows))
		}
		inserted = rows[0]
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	st, err := NewDeploymentStore(srv.URL, "service-key")
	if err != nil {
		t.Fatal(err)
	}
	err = st.InsertInferredMechanismPromotion(context.Background(), InferredMechanismPromotionRow{
		Signature:           "deadbeef",
		TriggerRule:         "failover_write_loss",
		ConfigPrecondition:  "replicationMode: async",
		FactKinds:           []string{"rule", "event"},
		ProposedMechanism:   map[string]any{"trigger": "failover_write_loss"},
		ClaimAspect:         "why",
		ClaimText:           "lost writes",
		Template:            "url-shortener",
		DeploymentNamespace: "ns-1",
		UserID:              "user-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if inserted["signature"] != "deadbeef" {
		t.Fatalf("inserted %v", inserted)
	}
}
