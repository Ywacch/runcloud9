package deployer

import (
	"context"
	"log"
)

type DeploymentDesign struct {
	Version        string            `json:"version"`
	Template       string            `json:"template"`
	Archetype      string            `json:"archetype"`
	Nodes          []DeploymentNode  `json:"nodes"`
	Connections    []DeploymentConn  `json:"connections"`
	SessionMinutes *int              `json:"sessionMinutes,omitempty"`
	JobBindings    map[string]string `json:"jobBindings,omitempty"` // job id -> node id
}

type DeploymentNode struct {
	ID       string                 `json:"id"`
	Type     string                 `json:"type"`
	Name     string                 `json:"name"`
	Config   map[string]interface{} `json:"config"`
	Position []float64              `json:"position,omitempty"`
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
	// OnWaitProgress is invoked while waiting for pods to become Ready (nil = no-op).
	OnWaitProgress func(nodeID string, pods []PodWaitProgress)
	// AfterPrimaryReady runs when the primary can accept seed writes for this
	// replication mode (see seedReplicaReadyBeforeRestore). Nil = no-op.
	// Seed errors must not fail Deploy (handler returns nil from the hook).
	AfterPrimaryReady func(ctx context.Context) error
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

// invokeAfterPrimaryReady calls cfg.AfterPrimaryReady when set. Errors are logged
// but never fail Deploy; the handler hook swallows seed failures.
func invokeAfterPrimaryReady(ctx context.Context, cfg DeployConfig) {
	if cfg.AfterPrimaryReady == nil {
		return
	}
	if err := cfg.AfterPrimaryReady(ctx); err != nil {
		log.Printf("AfterPrimaryReady: %v", err)
	}
}
