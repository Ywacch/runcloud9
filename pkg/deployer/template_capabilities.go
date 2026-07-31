package deployer

import (
	"fmt"

	"github.com/ywacch/cloud9/pkg/templates"
)

// WarningCodeTemplateNoAsync is emitted when a design includes mq/worker nodes but the
// template has no worker image — those components deploy as idle placeholders.
const WarningCodeTemplateNoAsync = "template_no_async"

// TopologyWarning is a non-blocking deploy assessment notice (verdict remains full).
type TopologyWarning struct {
	Code    string   `json:"code"`
	Message string   `json:"message"`
	NodeIDs []string `json:"nodeIds,omitempty"`
	EdgeIDs []string `json:"edgeIds,omitempty"`
}

// TemplateCapabilityWarnings returns template-specific warnings for a deployable design.
// Decorative MQ/worker nodes on templates without a worker image are warned, not stripped.
func TemplateCapabilityWarnings(design DeploymentDesign) []TopologyWarning {
	if templates.GetWorkerImage(design.Template) != "" {
		return nil
	}

	var nodeIDs []string
	for _, n := range design.Nodes {
		if n.Type == "mq" || n.Type == "worker" {
			nodeIDs = append(nodeIDs, n.ID)
		}
	}
	if len(nodeIDs) == 0 {
		return nil
	}

	return []TopologyWarning{{
		Code: WarningCodeTemplateNoAsync,
		Message: fmt.Sprintf(
			"Template %q does not run async workers — message queue and worker nodes will deploy but stay idle",
			design.Template,
		),
		NodeIDs: nodeIDs,
	}}
}

// AssessmentMap builds the JSON assessment object for create responses.
func AssessmentMap(assessment TopologyAssessment, warnings []TopologyWarning) map[string]any {
	m := map[string]any{
		"verdict":     assessment.Verdict,
		"explanation": assessment.Explanation,
	}
	if len(warnings) > 0 {
		m["warnings"] = warnings
	}
	return m
}
