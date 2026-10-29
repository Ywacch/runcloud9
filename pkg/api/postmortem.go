package api

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/inference"
	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/rules"
	"github.com/ywacch/cloud9/pkg/rules/verify"
	"github.com/ywacch/cloud9/pkg/templates"
)

// PostmortemClaimView is one verifier-approved claim persisted for the session summary UI.
type PostmortemClaimView struct {
	Scope              rules.ClaimScope         `json:"scope"`
	Components         []string                 `json:"components,omitempty"`
	Aspect             rules.ClaimAspect        `json:"aspect"`
	Text               string                   `json:"text"`
	FactRefs           []string                 `json:"factRefs,omitempty"`
	Grounding          rules.ClaimGrounding     `json:"grounding"`
	MechanismID        string                   `json:"mechanismId,omitempty"`
	ProposedMechanism  *rules.ProposedMechanism `json:"proposedMechanism,omitempty"`
	Confidence         string                   `json:"confidence,omitempty"`
	ObservedGapSeconds *float64                 `json:"observedGapSeconds,omitempty"`
	RequestID          string                   `json:"requestId,omitempty"`
}

// AIPostmortem is stored at metrics_summary.ai_postmortem after async generation.
type AIPostmortem struct {
	Status               string                `json:"status"`
	Summary              string                `json:"summary"`
	Claims               []PostmortemClaimView `json:"claims"`
	InsufficientEvidence []string              `json:"insufficientEvidence,omitempty"`
	SchemaVersion        int                   `json:"schemaVersion"`
	CatalogVersion       string                `json:"catalogVersion"`
	PromptVersion        string                `json:"promptVersion"`
	Model                string                `json:"model"`
	GeneratedAt          string                `json:"generatedAt"`
}

func wireClaimToRules(w inference.WireClaim) rules.Claim {
	c := rules.Claim{
		Scope:              rules.ClaimScope(w.Scope),
		Components:         append([]string(nil), w.Components...),
		Aspect:             rules.ClaimAspect(w.Aspect),
		Text:               w.Text,
		FactRefs:           append([]string(nil), w.FactRefs...),
		Grounding:          rules.ClaimGrounding(w.Grounding),
		MechanismID:        w.MechanismID,
		Confidence:         w.Confidence,
		ObservedGapSeconds: w.ObservedGapSeconds,
	}
	if w.ProposedMechanism != nil {
		p := w.ProposedMechanism
		c.ProposedMechanism = &rules.ProposedMechanism{
			Trigger:            p.Trigger,
			ConfigPrecondition: p.ConfigPrecondition,
			CausalChain:        append([]string(nil), p.CausalChain...),
		}
		if p.Precondition.Role != "" || p.Precondition.Key != "" || len(p.Precondition.AnyOf) > 0 {
			c.ProposedMechanism.Precondition = rules.Precondition{
				Role:  p.Precondition.Role,
				Key:   p.Precondition.Key,
				AnyOf: append([]string(nil), p.Precondition.AnyOf...),
			}
		}
	}
	return c
}

func claimViewFromRules(c rules.Claim, requestID string) PostmortemClaimView {
	view := PostmortemClaimView{
		Scope:              c.Scope,
		Components:         append([]string(nil), c.Components...),
		Aspect:             c.Aspect,
		Text:               c.Text,
		FactRefs:           append([]string(nil), c.FactRefs...),
		Grounding:          c.Grounding,
		MechanismID:        c.MechanismID,
		Confidence:         c.Confidence,
		ObservedGapSeconds: c.ObservedGapSeconds,
		RequestID:          requestID,
	}
	if c.ProposedMechanism != nil {
		pm := *c.ProposedMechanism
		view.ProposedMechanism = &pm
	}
	return view
}

// FilterVerifiedPostmortemClaims runs the session verifier on each wire claim.
func FilterVerifiedPostmortemClaims(
	wire []inference.WireClaim,
	ledger *metrics.Ledger,
	dc deployer.DesignConfig,
	fired []string,
) []PostmortemClaimView {
	views, _ := filterVerifiedPostmortemClaims(wire, ledger, dc, fired)
	return views
}

func filterVerifiedPostmortemClaims(
	wire []inference.WireClaim,
	ledger *metrics.Ledger,
	dc deployer.DesignConfig,
	fired []string,
) ([]PostmortemClaimView, []rules.Claim) {
	var out []PostmortemClaimView
	var rulesOut []rules.Claim
	for _, w := range wire {
		claim := wireClaimToRules(w)
		verified, rejections := verify.Verify(claim, ledger, dc, fired)
		if len(rejections) > 0 {
			continue
		}
		out = append(out, claimViewFromRules(verified, w.RequestID))
		rulesOut = append(rulesOut, verified)
	}
	return out, rulesOut
}

func buildAIPostmortem(resp *inference.PostmortemResponse, claims []PostmortemClaimView) AIPostmortem {
	pm := AIPostmortem{
		Status:         resp.Status,
		Summary:        resp.Summary,
		Claims:         claims,
		SchemaVersion:  resp.SchemaVersion,
		CatalogVersion: resp.CatalogVersion,
		PromptVersion:  resp.PromptVersion,
		Model:          resp.Model,
		GeneratedAt:    time.Now().UTC().Format(time.RFC3339Nano),
	}
	if len(resp.InsufficientEvidence) > 0 {
		pm.InsufficientEvidence = append([]string(nil), resp.InsufficientEvidence...)
	}
	return pm
}

func (h *Handler) schedulePostmortemAfterTeardown(namespace string) {
	if h == nil || h.inferenceClient == nil || h.store == nil {
		return
	}
	go h.runPostmortemAfterTeardown(namespace)
}

func (h *Handler) runPostmortemAfterTeardown(namespace string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	row, err := h.store.GetDeployment(ctx, namespace)
	if err != nil {
		log.Printf("postmortem %s: load deployment: %v", namespace, err)
		return
	}
	if row.Status != "terminated" {
		return
	}
	if !h.inferenceEnabledForUser(ctx, row.UserID) {
		return
	}

	design, _ := deployer.DesignFromMap(row.Design)
	if design == nil {
		design = &deployer.DeploymentDesign{}
	}
	dc := deployer.DesignConfigFromDesign(design)
	var tmpl *templates.TemplateSpec
	if row.Template != "" {
		tmpl = templates.GetTemplate(row.Template)
	}

	hist, err := SessionHistoryFromSummary(row.MetricsSummary)
	if err != nil {
		log.Printf("postmortem %s: session history: %v", namespace, err)
		return
	}
	final := finalSnapshot(hist, nil)
	aiCtx, err := aiContextFromSummary(&row, dc, design)
	if err != nil {
		log.Printf("postmortem %s: ai context: %v", namespace, err)
		return
	}

	resp, err := h.inferenceClient.Postmortem(ctx, aiCtx)
	if err != nil {
		log.Printf("postmortem %s: inference: %v", namespace, err)
		_ = h.persistAIPostmortemMap(ctx, namespace, map[string]any{
			"status":      "error",
			"generatedAt": time.Now().UTC().Format(time.RFC3339Nano),
		})
		return
	}
	if resp.Status == "skipped" {
		pm := buildAIPostmortem(resp, nil)
		_ = h.persistAIPostmortem(ctx, namespace, pm)
		return
	}

	traceSamples, _ := metrics.TraceSamplesFromSummary(row.MetricsSummary)
	violations := metrics.BuildViolations(hist, dc, tmpl)
	fired := metrics.FiredRuleIDs(hist, violations)
	ledger := metrics.BuildLedger(metrics.LedgerInput{
		DesignConfig: dc,
		Design:       design,
		Hist:         hist,
		Final:        final,
		Template:     tmpl,
		TraceSamples: traceSamples,
	})
	verified, verifiedRules := filterVerifiedPostmortemClaims(resp.Claims, ledger, dc, fired)
	pm := buildAIPostmortem(resp, verified)
	if err := h.persistAIPostmortem(ctx, namespace, pm); err != nil {
		log.Printf("postmortem %s: persist: %v", namespace, err)
	}
	if st, ok := h.store.(mechanismPromotionStore); ok {
		recordInferredMechanismPromotions(ctx, st, row.Template, namespace, row.UserID, verifiedRules, ledger)
	}
}

func (h *Handler) persistAIPostmortem(ctx context.Context, namespace string, pm AIPostmortem) error {
	raw, err := aiPostmortemToMap(pm)
	if err != nil {
		return err
	}
	return h.persistAIPostmortemMap(ctx, namespace, raw)
}

func aiPostmortemToMap(pm AIPostmortem) (map[string]any, error) {
	b, err := json.Marshal(pm)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (h *Handler) persistAIPostmortemMap(ctx context.Context, namespace string, aiPostmortem map[string]any) error {
	row, err := h.store.GetDeployment(ctx, namespace)
	if err != nil {
		return err
	}
	merged := make(map[string]any, len(row.MetricsSummary)+1)
	for k, v := range row.MetricsSummary {
		merged[k] = v
	}
	merged["ai_postmortem"] = aiPostmortem
	return h.store.UpdateMetricsSummary(ctx, namespace, merged)
}
