package chaos

import (
	"fmt"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/engines"
)

// pairAllowsScenario reports whether the capability manifest allows chaos on this target.
// When placement is missing, the registry match alone is enough (legacy sessions).
func pairAllowsScenario(scenario, targetComponent string, design deployer.DeploymentDesign) error {
	if design.ResolvedPlacement == nil || len(design.ResolvedPlacement.Nodes) == 0 {
		return nil
	}
	node, ok := design.ResolvedPlacement.NodeForComponent(targetComponent)
	if !ok {
		return nil
	}
	if len(node.Jobs) == 0 {
		return nil
	}
	cat := engines.Default()
	allowed := make(map[string]struct{})
	for _, jobID := range node.Jobs {
		j, ok := design.ResolvedPlacement.JobByID(jobID)
		if !ok {
			continue
		}
		pair, ok := cat.Pair(j.Job, j.Engine)
		if !ok {
			continue
		}
		for _, id := range pair.Chaos {
			allowed[id] = struct{}{}
		}
	}
	if _, ok := allowed[scenario]; ok {
		return nil
	}
	return fmt.Errorf(
		"chaos: scenario %q is not allowed for %q on this layout (manifest chaos list)",
		scenario, targetComponent,
	)
}
