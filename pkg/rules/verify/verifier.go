// Package verify implements claim validation against the metrics ledger.
// It lives in a subpackage so pkg/rules does not import pkg/metrics (import cycle).
package verify

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/rules"
)

const numberMatchTolerance = 0.11 // relative slack for prose rounding

var numberTokenRE = regexp.MustCompile(`\d+(?:\.\d+)?`)

// Verify validates a claim against the session ledger, design, and fired rules.
// Failed claims are dropped: the returned Claim is zero when rejections is non-empty.
func Verify(c rules.Claim, ledger *metrics.Ledger, dc deployer.DesignConfig, fired []string) (rules.Claim, []rules.Rejection) {
	var rejections []rules.Rejection

	if c.Grounding == rules.GroundingAbstained || c.Aspect == rules.AspectInsufficient {
		return c, nil
	}

	firedSet := make(map[string]bool, len(fired))
	for _, id := range fired {
		if id != "" {
			firedSet[id] = true
		}
	}

	resolved := make([]metrics.Fact, 0, len(c.FactRefs))
	for _, ref := range c.FactRefs {
		fact, ok := ledger.Resolve(metrics.FactRef(ref))
		if !ok {
			rejections = append(rejections, rules.Rejection{
				Reason: rules.RejectionUnresolvableRef,
				Detail: ref,
			})
			continue
		}
		resolved = append(resolved, fact)
	}
	if len(rejections) > 0 {
		return rules.Claim{}, rejections
	}

	for _, fact := range resolved {
		if fact.Kind != metrics.FactKindRule {
			continue
		}
		ruleID := ruleIDFromFact(fact)
		if ruleID == "" {
			continue
		}
		if !firedSet[ruleID] {
			rejections = append(rejections, rules.Rejection{
				Reason: rules.RejectionRuleNotFired,
				Detail: ruleID,
			})
		}
	}
	if len(rejections) > 0 {
		return rules.Claim{}, rejections
	}

	if err := numbersMatchText(c.Text, resolved); err != "" {
		rejections = append(rejections, rules.Rejection{
			Reason: rules.RejectionNumberMismatch,
			Detail: err,
		})
		return rules.Claim{}, rejections
	}

	out := c
	if downgraded, ok := downgradeCausationIfLagInsufficient(out, resolved); ok {
		out = downgraded
	}

	if scopeErr := scopeComponentsSatisfied(out.Scope, resolved); scopeErr != "" {
		rejections = append(rejections, rules.Rejection{
			Reason: rules.RejectionInsufficientScope,
			Detail: scopeErr,
		})
		return rules.Claim{}, rejections
	}

	switch out.Grounding {
	case rules.GroundingAuthored:
		if err := verifyAuthored(out, dc); err != "" {
			reason := rules.RejectionMechanismUnknown
			if strings.Contains(err, "precondition") {
				reason = rules.RejectionMechanismPrecondition
			}
			rejections = append(rejections, rules.Rejection{Reason: reason, Detail: err})
			return rules.Claim{}, rejections
		}
	case rules.GroundingInferred:
		if err := verifyInferred(out, dc, resolved); err != "" {
			rejections = append(rejections, rules.Rejection{
				Reason: rules.RejectionInferredPrecondition,
				Detail: err,
			})
			return rules.Claim{}, rejections
		}
	}

	return out, nil
}

func verifyAuthored(c rules.Claim, dc deployer.DesignConfig) string {
	if c.MechanismID == "" {
		return "missing mechanism_id"
	}
	m, ok := rules.LookupMechanism(c.MechanismID)
	if !ok {
		return c.MechanismID
	}
	if !m.Precondition.Holds(dc) {
		return fmt.Sprintf("precondition failed for mechanism %s", c.MechanismID)
	}
	return ""
}

func verifyInferred(c rules.Claim, dc deployer.DesignConfig, resolved []metrics.Fact) string {
	if c.ProposedMechanism == nil {
		return ""
	}
	p := c.ProposedMechanism.Precondition
	if p.Role == "" && p.Key == "" && len(p.AnyOf) == 0 {
		return ""
	}
	if !p.Holds(dc) {
		return "proposed precondition contradicts session design_config"
	}
	if !inferredPreconditionSupportedByConfigFacts(p, resolved) {
		return "proposed precondition not supported by cited config facts"
	}
	return ""
}

func inferredPreconditionSupportedByConfigFacts(p rules.Precondition, resolved []metrics.Fact) bool {
	for _, fact := range resolved {
		if fact.Kind != metrics.FactKindConfig {
			continue
		}
		if fact.Component != p.Role && p.Role != "" {
			continue
		}
		ref := string(fact.Ref)
		if !strings.Contains(ref, "."+p.Key) {
			continue
		}
		if rules.SettingMatchesAny(fact.Value, p.AnyOf) {
			return true
		}
	}
	return false
}

func scopeComponentsSatisfied(scope rules.ClaimScope, facts []metrics.Fact) string {
	if scope != rules.ScopePair && scope != rules.ScopeTopology {
		return ""
	}
	components := uniqueFactComponents(facts)
	if scope == rules.ScopePair && len(components) < 2 {
		return fmt.Sprintf("pair claim cites %d component(s), need at least 2", len(components))
	}
	if scope == rules.ScopeTopology && len(components) < 2 {
		return fmt.Sprintf("topology claim cites %d component(s), need at least 2", len(components))
	}
	return ""
}

func uniqueFactComponents(facts []metrics.Fact) []string {
	seen := make(map[string]bool)
	var out []string
	for _, f := range facts {
		c := strings.TrimSpace(f.Component)
		if c == "" || seen[c] {
			continue
		}
		seen[c] = true
		out = append(out, c)
	}
	return out
}

func downgradeCausationIfLagInsufficient(c rules.Claim, facts []metrics.Fact) (rules.Claim, bool) {
	if c.Aspect != rules.AspectWhy || c.ObservedGapSeconds == nil {
		return c, false
	}
	gap := *c.ObservedGapSeconds
	var lagBudget float64
	for _, f := range facts {
		if f.Freshness == nil {
			continue
		}
		lagBudget += f.Freshness.NominalLagSeconds
	}
	if lagBudget <= 0 {
		return c, false
	}
	if lagBudget > gap {
		c.Aspect = rules.AspectCorrelation
		return c, true
	}
	return c, false
}

func numbersMatchText(text string, facts []metrics.Fact) string {
	tokens := numberTokenRE.FindAllString(text, -1)
	if len(tokens) == 0 {
		return ""
	}
	var cited []float64
	for _, f := range facts {
		v, ok := numericFactValue(f)
		if ok {
			cited = append(cited, v)
		}
	}
	if len(cited) == 0 {
		return ""
	}
	for _, tok := range tokens {
		n, err := strconv.ParseFloat(tok, 64)
		if err != nil {
			continue
		}
		if !numberMatchesAnyCited(n, cited) {
			return fmt.Sprintf("text number %s does not match cited facts", tok)
		}
	}
	return ""
}

func numberMatchesAnyCited(n float64, cited []float64) bool {
	for _, v := range cited {
		if v == 0 {
			if n == 0 {
				return true
			}
			continue
		}
		rel := math.Abs(n-v) / math.Abs(v)
		if rel <= numberMatchTolerance || math.Abs(n-v) <= 1 {
			return true
		}
	}
	return false
}

func numericFactValue(f metrics.Fact) (float64, bool) {
	switch v := f.Value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case map[string]any:
		if raw, ok := v["peak"]; ok {
			return numericFactValue(metrics.Fact{Value: raw})
		}
		if raw, ok := v["last"]; ok {
			return numericFactValue(metrics.Fact{Value: raw})
		}
	}
	return 0, false
}

func ruleIDFromFact(f metrics.Fact) string {
	if m, ok := f.Value.(map[string]any); ok {
		if id, ok := m["ruleId"].(string); ok {
			return id
		}
	}
	ref := string(f.Ref)
	const prefix = "fact:rule:"
	if strings.HasPrefix(ref, prefix) {
		return strings.TrimPrefix(ref, prefix)
	}
	return ""
}
