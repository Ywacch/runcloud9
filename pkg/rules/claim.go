package rules

// ClaimScope names how many components a claim spans.
type ClaimScope string

const (
	ScopeComponent ClaimScope = "component"
	ScopePair      ClaimScope = "pair"
	ScopeTopology  ClaimScope = "topology"
)

// ClaimAspect names what kind of explanation the claim offers.
type ClaimAspect string

const (
	AspectWhat         ClaimAspect = "what"
	AspectWhy          ClaimAspect = "why"
	AspectHow          ClaimAspect = "how"
	AspectCorrelation  ClaimAspect = "correlation" // downgraded when lag blocks causation
	AspectInsufficient ClaimAspect = "insufficient_evidence"
)

// ClaimGrounding distinguishes library mechanisms from model-inferred chains.
type ClaimGrounding string

const (
	GroundingAuthored  ClaimGrounding = "authored"
	GroundingInferred  ClaimGrounding = "inferred"
	GroundingAbstained ClaimGrounding = "abstained"
)

// ProposedMechanism is a model-authored causal story when no library row matched.
type ProposedMechanism struct {
	Trigger            string       `json:"trigger"`
	ConfigPrecondition string       `json:"config_precondition,omitempty"`
	Precondition       Precondition `json:"precondition,omitempty"`
	CausalChain        []string     `json:"causal_chain,omitempty"`
}

// Claim is one grounded statement the brain may ship after verification.
// Validation is implemented by verify.Verify in pkg/rules/verify (subpackage avoids
// an import cycle: metrics already imports rules for rule IDs).
type Claim struct {
	Scope              ClaimScope         `json:"scope"`
	Components         []string           `json:"components"`
	Aspect             ClaimAspect        `json:"aspect"`
	Text               string             `json:"text"`
	FactRefs           []string           `json:"fact_refs"`
	Grounding          ClaimGrounding     `json:"grounding"`
	MechanismID        string             `json:"mechanism_id,omitempty"`
	ProposedMechanism  *ProposedMechanism `json:"proposed_mechanism,omitempty"`
	Confidence         string             `json:"confidence,omitempty"`
	ObservedGapSeconds *float64           `json:"observed_gap_seconds,omitempty"`
}

// RejectionReason is a stable verifier failure code.
type RejectionReason string

const (
	RejectionUnresolvableRef       RejectionReason = "unresolvable_fact_ref"
	RejectionRuleNotFired          RejectionReason = "rule_not_fired"
	RejectionNumberMismatch        RejectionReason = "number_mismatch"
	RejectionInsufficientScope     RejectionReason = "insufficient_scope"
	RejectionMechanismUnknown      RejectionReason = "mechanism_unknown"
	RejectionMechanismPrecondition RejectionReason = "mechanism_precondition_failed"
	RejectionInferredPrecondition  RejectionReason = "inferred_precondition_contradicts_config"
)

// Rejection records why a claim was dropped.
type Rejection struct {
	Reason RejectionReason `json:"reason"`
	Detail string          `json:"detail,omitempty"`
}
