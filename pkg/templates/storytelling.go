package templates

// StorytellingSpec is template-authored copy for roles, request journeys, and detector narratives.
// Loaded from embedded YAML; exposed on TemplateSpec for GET /templates and tooling.
type StorytellingSpec struct {
	Roles            map[string]RoleStorySpec        `yaml:"roles" json:"roles,omitempty"`
	Journeys         []JourneySpec                   `yaml:"journeys" json:"journeys,omitempty"`
	NarrativeContext map[string]NarrativeRuleContext `yaml:"narrativeContext" json:"narrativeContext,omitempty"`
}

// RoleStorySpec describes a component role line for the canvas / tooltips.
type RoleStorySpec struct {
	Base     string            `yaml:"base" json:"base"`
	Variants []RoleVariantSpec `yaml:"variants" json:"variants,omitempty"`
}

// RoleVariantSpec overrides base when When matches design topology / cache strategy.
type RoleVariantSpec struct {
	When  WhenSpec `yaml:"when" json:"when"`
	Label string   `yaml:"label" json:"label"`
}

// WhenSpec is a simple matcher: optional topology expression and/or cache strategy.
// Topology uses AND of tokens: "hasMQ+hasWorker", negation "!", e.g. "!hasMQ+hasCache".
type WhenSpec struct {
	Topology string `yaml:"topology,omitempty" json:"topology,omitempty"`
	Strategy string `yaml:"strategy,omitempty" json:"strategy,omitempty"`
}

// JourneySpec is one request-type journey for educational UI.
type JourneySpec struct {
	ID                  string               `yaml:"id" json:"id"`
	Method              string               `yaml:"method" json:"method"`
	Path                string               `yaml:"path" json:"path"`
	Label               string               `yaml:"label" json:"label"`
	TrafficPercent      int                  `yaml:"trafficPercent" json:"trafficPercent"`
	Steps               []JourneyStepSpec    `yaml:"steps" json:"steps,omitempty"`
	Explanation         string               `yaml:"explanation" json:"explanation,omitempty"`
	ExplanationVariants []ExplanationVariant `yaml:"explanationVariants" json:"explanationVariants,omitempty"`
}

// ExplanationVariant swaps the journey footer text for matching strategy/topology.
type ExplanationVariant struct {
	When WhenSpec `yaml:"when" json:"when"`
	Text string   `yaml:"text" json:"text"`
}

// JourneyStepSpec is one hop in a request journey diagram.
type JourneyStepSpec struct {
	Component   string    `yaml:"component" json:"component"`
	Action      string    `yaml:"action" json:"action"`
	Detail      string    `yaml:"detail,omitempty" json:"detail,omitempty"`
	MetricsKey  string    `yaml:"metricsKey,omitempty" json:"metricsKey,omitempty"`
	Requires    []string  `yaml:"requires,omitempty" json:"requires,omitempty"`
	Condition   string    `yaml:"condition,omitempty" json:"condition,omitempty"`
	OnlyWhen    *WhenSpec `yaml:"onlyWhen,omitempty" json:"onlyWhen,omitempty"`
	ExcludeWhen *WhenSpec `yaml:"excludeWhen,omitempty" json:"excludeWhen,omitempty"`
	// Lesson is optional walkthrough copy for this step only. When empty, the UI should
	// not repeat the journey-level explanation here (show journey.explanation on the last step only).
	Lesson string `yaml:"lesson,omitempty" json:"lesson,omitempty"`
}

// NarrativeRuleContext augments detector narratives (keys = rule ids, e.g. "mq_backpressure").
type NarrativeRuleContext struct {
	Cause      string `yaml:"cause,omitempty" json:"cause,omitempty"`
	Suggestion string `yaml:"suggestion,omitempty" json:"suggestion,omitempty"`
}
