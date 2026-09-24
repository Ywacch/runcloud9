package inference

// PostmortemResponse is the runcloud9-ai POST /postmortem JSON body (camelCase aliases).
type PostmortemResponse struct {
	Status               string      `json:"status"`
	Claims               []WireClaim `json:"claims"`
	Summary              string      `json:"summary"`
	InsufficientEvidence []string    `json:"insufficientEvidence"`
	SchemaVersion        int         `json:"schemaVersion"`
	CatalogVersion       string      `json:"catalogVersion"`
	PromptVersion        string      `json:"promptVersion"`
	Model                string      `json:"model"`
}

// WireClaim mirrors runcloud9-ai claim JSON for decode; convert with ToRulesClaim.
type WireClaim struct {
	Scope              string                 `json:"scope"`
	Components         []string               `json:"components"`
	Aspect             string                 `json:"aspect"`
	Text               string                 `json:"text"`
	FactRefs           []string               `json:"factRefs"`
	Grounding          string                 `json:"grounding"`
	MechanismID        string                 `json:"mechanismId"`
	ProposedMechanism  *WireProposedMechanism `json:"proposedMechanism"`
	Confidence         string                 `json:"confidence"`
	ObservedGapSeconds *float64               `json:"observedGapSeconds"`
	RequestID          string                 `json:"requestId"`
}

// WireProposedMechanism mirrors model-inferred mechanism JSON from runcloud9-ai.
type WireProposedMechanism struct {
	Trigger            string           `json:"trigger"`
	ConfigPrecondition string           `json:"config_precondition"`
	Precondition       WirePrecondition `json:"precondition"`
	CausalChain        []string         `json:"causalChain"`
}

// WirePrecondition is a design key precondition on inferred claims.
type WirePrecondition struct {
	Role  string   `json:"role"`
	Key   string   `json:"key"`
	AnyOf []string `json:"anyOf"`
}
