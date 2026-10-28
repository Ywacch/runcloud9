package api

import (
	"context"
	"fmt"
	"log"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/inference"
	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/rules"
	"github.com/ywacch/cloud9/pkg/rules/verify"
	"github.com/ywacch/cloud9/pkg/templates"
)

// ReelCaptionView is a verified caption attached to one events_summary row.
type ReelCaptionView struct {
	Text        string               `json:"text"`
	Grounding   rules.ClaimGrounding `json:"grounding"`
	FactRefs    []string             `json:"factRefs,omitempty"`
	MechanismID string               `json:"mechanismId,omitempty"`
}

// ApplyReelCaptions verifies inference claims and merges accepted captions onto highlight rows.
// Rejected captions leave the row unchanged (authored narrative remains the fallback).
func ApplyReelCaptions(
	highlights []map[string]any,
	resp *inference.ReelCaptionsResponse,
	ledger *metrics.Ledger,
	dc deployer.DesignConfig,
	fired []string,
) ([]map[string]any, string) {
	if resp == nil || len(highlights) == 0 {
		return highlights, ""
	}
	byRule := make(map[string]rules.Claim, len(resp.Captions))
	for _, item := range resp.Captions {
		if item.RuleID == "" {
			continue
		}
		byRule[item.RuleID] = item.Claim
	}
	out := make([]map[string]any, len(highlights))
	for i, row := range highlights {
		out[i] = cloneHighlightRow(row)
		ruleID := highlightRuleID(row)
		claim, ok := byRule[ruleID]
		if !ok {
			continue
		}
		verified, rejections := verify.Verify(claim, ledger, dc, fired)
		if len(rejections) > 0 {
			continue
		}
		out[i]["caption"] = reelCaptionViewFromClaim(verified)
	}
	return out, resp.Throughline
}

func reelCaptionViewFromClaim(c rules.Claim) ReelCaptionView {
	view := ReelCaptionView{
		Text:      c.Text,
		Grounding: c.Grounding,
	}
	if len(c.FactRefs) > 0 {
		view.FactRefs = append([]string(nil), c.FactRefs...)
	}
	if c.MechanismID != "" {
		view.MechanismID = c.MechanismID
	}
	return view
}

func cloneHighlightRow(row map[string]any) map[string]any {
	dup := make(map[string]any, len(row)+1)
	for k, v := range row {
		dup[k] = v
	}
	return dup
}

func highlightRuleID(row map[string]any) string {
	if v, ok := row["ruleId"].(string); ok && v != "" {
		return v
	}
	if v, ok := row["ruleID"].(string); ok && v != "" {
		return v
	}
	if v, ok := row["rule"].(string); ok && v != "" {
		return v
	}
	return ""
}

func (h *Handler) enrichReelCaptionsInSummary(
	ctx context.Context,
	namespace string,
	userID string,
	summary map[string]any,
	design deployer.DesignConfig,
	template string,
	hist []metrics.DeploymentMetrics,
	latest *metrics.DeploymentMetrics,
) {
	if h == nil || h.inferenceClient == nil || h.inferenceTokenBudget == nil || summary == nil {
		return
	}
	if !h.inferenceEnabledForUser(ctx, userID) {
		return
	}
	raw, ok := summary["events_summary"]
	if !ok || raw == nil {
		return
	}
	highlights, err := decodeEventsSummary(raw)
	if err != nil || len(highlights) == 0 {
		return
	}
	if h.inferenceTokenBudget.Remaining(namespace) <= 0 {
		summary["reel_captions_skipped"] = "token_budget_exhausted"
		return
	}

	var tmpl *templates.TemplateSpec
	if template != "" {
		tmpl = templates.GetTemplate(template)
	}
	final := finalSnapshot(hist, latest)
	aiCtx := BuildAIContext(AIContextInput{
		SessionID:    namespace,
		DesignConfig: design,
		Hist:         hist,
		Final:        final,
		Template:     tmpl,
	})
	violations := metrics.BuildViolations(hist, design, tmpl)
	fired := metrics.FiredRuleIDs(hist, violations)
	ledger := metrics.BuildLedger(metrics.LedgerInput{
		DesignConfig: design,
		Hist:         hist,
		Final:        final,
		Template:     tmpl,
	})

	resp, err := h.inferenceClient.ReelCaptions(ctx, inference.ReelCaptionsRequest{
		Context:    aiCtx,
		Highlights: highlights,
	})
	if err != nil {
		log.Printf("reel captions %s: %v", namespace, err)
		summary["reel_captions_skipped"] = "inference_error"
		return
	}
	tokens := resp.Usage.TotalTokens
	if tokens <= 0 {
		tokens = resp.Usage.PromptTokens + resp.Usage.CompletionTokens
	}
	if tokens > 0 && !h.inferenceTokenBudget.TrySpend(namespace, tokens) {
		summary["reel_captions_skipped"] = "token_budget_exhausted"
		return
	}

	enriched, throughline := ApplyReelCaptions(highlights, resp, ledger, design, fired)
	summary["events_summary"] = enriched
	if throughline != "" {
		summary["reel_throughline"] = throughline
	}
	summary["inference_tokens_spent"] = h.inferenceTokenBudget.Spent(namespace)
}

func decodeEventsSummary(raw any) ([]map[string]any, error) {
	switch v := raw.(type) {
	case []map[string]any:
		return v, nil
	case []any:
		out := make([]map[string]any, 0, len(v))
		for _, item := range v {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			out = append(out, m)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("events_summary has unexpected type %T", raw)
	}
}
