package verify

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"

	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/rules"
)

// PromotionSignatureParts are the stable inputs hashed for recurrence grouping.
type PromotionSignatureParts struct {
	Trigger            string   `json:"trigger"`
	ConfigPrecondition string   `json:"config_precondition"`
	FactKinds          []string `json:"fact_kinds"`
}

// FactKindsFromRefs returns sorted unique ledger fact kinds cited by refs.
func FactKindsFromRefs(ledger *metrics.Ledger, refs []string) []string {
	if ledger == nil || len(refs) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	var kinds []string
	for _, ref := range refs {
		f, ok := ledger.Resolve(metrics.FactRef(ref))
		if !ok || f.Kind == "" {
			continue
		}
		if _, dup := seen[f.Kind]; dup {
			continue
		}
		seen[f.Kind] = struct{}{}
		kinds = append(kinds, f.Kind)
	}
	sort.Strings(kinds)
	return kinds
}

// CanonicalConfigPrecondition normalizes the design precondition on a proposed mechanism.
func CanonicalConfigPrecondition(pm *rules.ProposedMechanism) string {
	if pm == nil {
		return ""
	}
	if s := strings.TrimSpace(pm.ConfigPrecondition); s != "" {
		return s
	}
	p := pm.Precondition
	if p.Key == "" {
		return ""
	}
	role := strings.TrimSpace(p.Role)
	key := strings.TrimSpace(p.Key)
	if len(p.AnyOf) == 0 {
		if role != "" {
			return role + "." + key
		}
		return key
	}
	vals := append([]string(nil), p.AnyOf...)
	sort.Strings(vals)
	prefix := key
	if role != "" {
		prefix = role + "." + key
	}
	return prefix + " in (" + strings.Join(vals, ", ") + ")"
}

// PromotionSignaturePartsForClaim builds signature inputs for an inferred claim.
func PromotionSignaturePartsForClaim(claim rules.Claim, ledger *metrics.Ledger) (PromotionSignatureParts, bool) {
	if claim.Grounding != rules.GroundingInferred || claim.ProposedMechanism == nil {
		return PromotionSignatureParts{}, false
	}
	pm := claim.ProposedMechanism
	trigger := strings.TrimSpace(pm.Trigger)
	if trigger == "" {
		return PromotionSignatureParts{}, false
	}
	return PromotionSignatureParts{
		Trigger:            trigger,
		ConfigPrecondition: CanonicalConfigPrecondition(pm),
		FactKinds:          FactKindsFromRefs(ledger, claim.FactRefs),
	}, true
}

// PromotionSignature returns a stable hex SHA-256 over signature parts (for GROUP BY).
func PromotionSignature(parts PromotionSignatureParts) (string, error) {
	b, err := json.Marshal(parts)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}
