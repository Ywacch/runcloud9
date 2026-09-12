// Package chaos implements one-shot chaos scenarios against deployment namespaces.
package chaos

import (
	"context"

	"github.com/ywacch/cloud9/pkg/deployer"
)

// ChaosScenario applies a disruption in a deployment namespace.
type ChaosScenario interface {
	Apply(ctx context.Context, namespace string, design deployer.DeploymentDesign, targetComponent string) error
	// Revert undoes a stateful scenario. One-shot scenarios return nil.
	Revert(ctx context.Context, namespace string, design deployer.DeploymentDesign, targetComponent string) error
	// Stateful reports whether the scenario leaves cluster state until Revert runs.
	Stateful() bool
	Description(targetComponent string) string
}
