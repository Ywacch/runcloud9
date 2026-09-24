package verify

import (
	"testing"

	"github.com/ywacch/cloud9/pkg/rules"
)

func TestPromotionSignature_stableForSameInputs(t *testing.T) {
	t.Parallel()
	parts := PromotionSignatureParts{
		Trigger:            "failover_write_loss",
		ConfigPrecondition: "replicationMode: async",
		FactKinds:          []string{"event", "rule", "violation"},
	}
	a, err := PromotionSignature(parts)
	if err != nil {
		t.Fatal(err)
	}
	b, err := PromotionSignature(parts)
	if err != nil {
		t.Fatal(err)
	}
	if a != b || len(a) != 64 {
		t.Fatalf("signature %q", a)
	}
}

func TestPromotionSignaturePartsForClaim_requiresInferred(t *testing.T) {
	t.Parallel()
	_, ok := PromotionSignaturePartsForClaim(rules.Claim{Grounding: rules.GroundingAuthored}, nil)
	if ok {
		t.Fatal("authored should not produce signature")
	}
}
