package api

import (
	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/metrics"
	"github.com/ywacch/cloud9/pkg/rules"
	"github.com/ywacch/cloud9/pkg/templates"
)

// AIContextSchemaVersion is the current ai-context JSON contract version.
const AIContextSchemaVersion = 1

const (
	RunRoleBaseline  = "baseline"
	RunRoleCandidate = "candidate"
)

// AIContext is the versioned payload served to ganymede-brain and the claim verifier.
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
	DesignConfig deployer.DesignConfig
	Hist         []metrics.DeploymentMetrics
	Final        *metrics.DeploymentMetrics
	Template     *templates.TemplateSpec
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
		Hist:         in.Hist,
		Final:        final,
		Template:     in.Template,
	})
	facts := []metrics.Fact{}
	if ledger != nil {
		facts = ledger.Facts()
	}

	return AIContext{
		SchemaVersion:  AIContextSchemaVersion,
		CatalogVersion: rules.CatalogVersion,
		SessionID:      in.SessionID,
		Runs: []Run{{
			Role:               RunRoleBaseline,
			DesignConfig:       in.DesignConfig,
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
