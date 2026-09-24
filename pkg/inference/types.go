package inference

import "github.com/ywacch/cloud9/pkg/rules"

// ReelCaptionsRequest is the POST /reel/captions body from cloud9-api.
type ReelCaptionsRequest struct {
	Context    any              `json:"context"`
	Highlights []map[string]any `json:"highlights"`
}

// ReelCaptionItem pairs one highlight rule id with a model claim.
type ReelCaptionItem struct {
	RuleID string      `json:"ruleId"`
	Claim  rules.Claim `json:"claim"`
}

// TokenUsage reports LLM token consumption for session budgeting.
type TokenUsage struct {
	PromptTokens     int `json:"promptTokens"`
	CompletionTokens int `json:"completionTokens"`
	TotalTokens      int `json:"totalTokens"`
}

// ReelCaptionsResponse is the runcloud9-ai response for highlight captions.
type ReelCaptionsResponse struct {
	Throughline string            `json:"throughline"`
	Captions    []ReelCaptionItem `json:"captions"`
	Usage       TokenUsage        `json:"usage"`
}
