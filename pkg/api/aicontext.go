package api

import (
	"encoding/json"
	"fmt"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/placement"
	"github.com/ywacch/cloud9/pkg/rules"
	"github.com/ywacch/cloud9/pkg/templateapp/trace"
	"github.com/ywacch/cloud9/pkg/templates"
)

// AIContextSchemaVersion is the current ai-context JSON contract version.
const AIContextSchemaVersion = 2

const (
	RunRoleBaseline  = "baseline"
	RunRoleCandidate = "candidate"
)

// AIContext is the versioned payload served to runcloud9-ai and the claim verifier.
type AIContext struct {
	SchemaVersion  int    `json:"schemaVersion"`
	CatalogVersion string `json:"catalogVersion"`
	SessionID      string `json:"sessionId"`
	Runs           []Run  `json:"runs"`
}

// Run is one design variant in a session. Only baseline is populated today; the slice
// shape leaves room for comparison-mode candidate runs without a schema change.
type Run struct {
	Role               string                     `json:"role"`
	DesignConfig       deployer.DesignConfig      `json:"designConfig"`
	Placement          *placement.Topology        `json:"placement,omitempty"`
	Facts              []metrics.Fact             `json:"facts"`
	Descriptors        []metrics.SignalDescriptor `json:"descriptors"`
	ComponentInternals metrics.ComponentInternals `json:"componentInternals"`
	Violations         []metrics.ViolationRollup  `json:"violations"`
	Mechanisms         []rules.Mechanism          `json:"mechanisms"`
	Events             []map[string]any           `json:"events"`
	Trajectories       map[string]any             `json:"trajectories"`
}

// AIContextInput feeds BuildAIContext from session design and metrics evidence.
type AIContextInput struct {
	SessionID    string
	RunRole      string // empty → baseline in BuildAIContext
	DesignConfig deployer.DesignConfig
	Design       *deployer.DeploymentDesign
	Hist         []metrics.DeploymentMetrics
	Final        *metrics.DeploymentMetrics
	Template     *templates.TemplateSpec
	TraceSamples []trace.JourneySample
}

// BuildAIContext assembles the baseline run from design_config, the fact ledger,
// descriptors, component internals, violations, mechanism priors, highlight events,
// and trajectories.
func BuildAIContext(in AIContextInput) AIContext {
	final := in.Final
	if final == nil && len(in.Hist) > 0 {
		final = &in.Hist[len(in.Hist)-1]
	}

	violations := metrics.BuildViolations(in.Hist, in.DesignConfig, in.Template)
	fired := metrics.FiredRuleIDs(in.Hist, violations)
	internals := metrics.BuildComponentInternals(final)

	ledger := metrics.BuildLedger(metrics.LedgerInput{
		DesignConfig: in.DesignConfig,
		Design:       in.Design,
		Hist:         in.Hist,
		Final:        final,
		Template:     in.Template,
		TraceSamples: in.TraceSamples,
	})
	facts := []metrics.Fact{}
	if ledger != nil {
		facts = ledger.Facts()
	}

	runRole := RunRoleBaseline
	if in.RunRole != "" {
		runRole = in.RunRole
	} else if final != nil && final.RunRole != "" {
		runRole = final.RunRole
	}

	pl := placementForAIContext(in)

	return AIContext{
		SchemaVersion:  AIContextSchemaVersion,
		CatalogVersion: rules.CatalogVersion,
		SessionID:      in.SessionID,
		Runs: []Run{{
			Role:               runRole,
			DesignConfig:       in.DesignConfig,
			Placement:          pl,
			Facts:              facts,
			Descriptors:        metrics.BuildDescriptors(internals),
			ComponentInternals: internals,
			Violations:         violations,
			Mechanisms:         rules.MechanismsFor(in.DesignConfig, fired),
			Events:             metrics.BuildEventsSummary(in.Hist, 10),
			Trajectories:       metrics.BuildTrajectories(in.Hist),
		}},
	}
}

// AIContextInputFromDeployment maps a stored deployment row into builder input.
func AIContextInputFromDeployment(namespace, template string, dc deployer.DesignConfig, hist []metrics.DeploymentMetrics, final *metrics.DeploymentMetrics) AIContextInput {
	return AIContextInputFromDeploymentDesign(namespace, template, dc, nil, hist, final)
}

// AIContextInputFromDeploymentDesign builds AI context input with optional full design for wiring facts.
func AIContextInputFromDeploymentDesign(
	namespace, template string,
	dc deployer.DesignConfig,
	design *deployer.DeploymentDesign,
	hist []metrics.DeploymentMetrics,
	final *metrics.DeploymentMetrics,
) AIContextInput {
	if dc.Template == "" {
		dc.Template = template
	}
	var tmpl *templates.TemplateSpec
	if template != "" {
		tmpl = templates.GetTemplate(template)
	}
	return AIContextInput{
		SessionID:    namespace,
		DesignConfig: dc,
		Design:       design,
		Hist:         hist,
		Final:        final,
		Template:     tmpl,
	}
}

// SessionHistoryFromSummary decodes persisted metrics_summary.session_history.
func SessionHistoryFromSummary(summary map[string]any) ([]metrics.DeploymentMetrics, error) {
	if summary == nil {
		return nil, nil
	}
	raw, ok := summary["session_history"]
	if !ok || raw == nil {
		return nil, nil
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("marshal session_history: %w", err)
	}
	var hist []metrics.DeploymentMetrics
	if err := json.Unmarshal(b, &hist); err != nil {
		return nil, fmt.Errorf("unmarshal session_history: %w", err)
	}
	return hist, nil
}

func placementForAIContext(in AIContextInput) *placement.Topology {
	if in.Final != nil && in.Final.Placement != nil {
		return in.Final.Placement
	}
	if in.Design == nil || in.Template == nil {
		return nil
	}
	topo, err := deployer.ResolvePlacement(*in.Design, in.Template)
	if err != nil {
		return nil
	}
	return &topo
}

func finalSnapshot(hist []metrics.DeploymentMetrics, latest *metrics.DeploymentMetrics) *metrics.DeploymentMetrics {
	if latest != nil && !latest.CollectPending {
		return latest
	}
	if len(hist) == 0 {
		return latest
	}
	return &hist[len(hist)-1]
}
