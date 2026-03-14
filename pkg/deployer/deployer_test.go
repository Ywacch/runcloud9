package deployer

import (
	"context"
	"testing"
)

func TestLocalDeployer_Deploy(t *testing.T) {
	d := &LocalDeployer{NodeType: "db"}

	cfg := DeployConfig{
		Namespace: "test-namespace",
		Node: DeploymentNode{
			ID:   "node-1",
			Type: "db",
			Name: "PostgreSQL",
			Config: map[string]interface{}{
				"engine": "postgresql",
			},
		},
	}

	if err := d.Deploy(context.Background(), cfg); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestLocalDeployer_Teardown(t *testing.T) {
	d := &LocalDeployer{NodeType: "db"}

	if err := d.Teardown(context.Background(), "test-namespace"); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestLocalDeployer_CancelledContext(t *testing.T) {
	// This tests that our deployer respects context cancellation
	// We create an already cancelled context and verify the deployer returns
	// an error instead of proceeding
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	d := &LocalDeployer{NodeType: "db"}
	cfg := DeployConfig{Namespace: "ns", Node: DeploymentNode{Name: "test"}}

	err := d.Deploy(ctx, cfg)
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}
