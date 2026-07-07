package deployer

import "context"

type DeploymentDesign struct {
	Version        string           `json:"version"`
	Template       string           `json:"template"`
	Archetype      string           `json:"archetype"`
	Nodes          []DeploymentNode `json:"nodes"`
	Connections    []DeploymentConn `json:"connections"`
	SessionMinutes *int             `json:"sessionMinutes,omitempty"`
}

type DeploymentNode struct {
	ID     string                 `json:"id"`
	Type   string                 `json:"type"`
	Name   string                 `json:"name"`
	Config map[string]interface{} `json:"config"`
}

type DeploymentConn struct {
	SourceID string `json:"sourceId"`
	TargetID string `json:"targetId"`
	Protocol string `json:"protocol"`
}

// DeployConfig is what the orchestrator passes to each ComponentDeployer.
// It carries the namespace (isolates this deployment in K8s) and the
// specific node being deployed.
type DeployConfig struct {
	Namespace string
	Node      DeploymentNode
	Design    DeploymentDesign // full design for context (e.g. connections)
}

type ComponentDeployer interface {
	// Deploy provisions a single component (e.g. a Postgres pod) and returns
	// when it is ready to accept traffic, or an error if it fails.
	Deploy(ctx context.Context, cfg DeployConfig) error

	// Teardown removes the component and all associated resources.
	// Must be idempotent — safe to call even if Deploy never completed.
	Teardown(ctx context.Context, namespace string) error
}

// DeploymentResult holds the outcome after all components are deployed.
type DeploymentResult struct {
	DeploymentID string `json:"deploymentId"`
	Namespace    string `json:"namespace"`
	Status       string `json:"status"` // "running" | "failed"
	Error        string `json:"error,omitempty"`
	Seeded       bool   `json:"seeded"`
}
