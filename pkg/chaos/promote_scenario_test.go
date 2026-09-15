package chaos

import (
	"context"
	"errors"
	"testing"

	"github.com/ywacch/cloud9/pkg/deployer"
	"k8s.io/client-go/kubernetes/fake"
)

func TestPromoteScenario_Stateful(t *testing.T) {
	t.Parallel()

	s := &PromoteScenario{Prefer: deployer.PreferMostCurrent}
	if s.Stateful() {
		t.Fatal("promotion should not be stateful")
	}
}

func TestResolve_promoteScenarios(t *testing.T) {
	t.Parallel()

	client := fake.NewClientset(runningReplicaPod("ns", "postgres-replica-0"))
	design := dbDesignWithReplicas()

	for _, name := range []string{scenarioPromoteReplica, scenarioPromoteLaggingReplica} {
		s, err := Resolve(name, "db", design, client)
		if err != nil {
			t.Fatalf("Resolve(%q): %v", name, err)
		}
		if s.Stateful() {
			t.Fatalf("%q should not be stateful", name)
		}
		ps, ok := s.(*PromoteScenario)
		if !ok {
			t.Fatalf("Resolve(%q) returned %T", name, s)
		}
		want := deployer.PreferMostCurrent
		if name == scenarioPromoteLaggingReplica {
			want = deployer.PreferMostLagging
		}
		if ps.Prefer != want {
			t.Fatalf("Prefer: got %q want %q", ps.Prefer, want)
		}
	}
}

func TestPromoteScenario_zeroReplicasError(t *testing.T) {
	t.Parallel()

	client := fake.NewClientset()
	design := deployer.DeploymentDesign{
		Nodes: []deployer.DeploymentNode{
			{Type: "db", Name: "DB", Config: map[string]any{"engine": "postgresql", "readReplicas": 0}},
		},
	}
	s := &PromoteScenario{Client: client, Prefer: deployer.PreferMostCurrent}
	if err := s.Apply(context.Background(), "ns", design, "db"); err == nil {
		t.Fatal("expected error for zero replicas")
	}
}

func TestPromoteScenario_unsupportedEngineError(t *testing.T) {
	t.Parallel()

	client := fake.NewClientset()
	design := deployer.DeploymentDesign{
		Nodes: []deployer.DeploymentNode{
			{Type: "db", Name: "DB", Config: map[string]any{"engine": "mysql", "readReplicas": 1}},
		},
	}
	s := &PromoteScenario{Client: client, Prefer: deployer.PreferMostCurrent}
	if err := s.Apply(context.Background(), "ns", design, "db"); err == nil {
		t.Fatal("expected error for unsupported engine")
	}
}

func TestPromoteScenario_rejectsActivePartition(t *testing.T) {
	t.Parallel()

	ns := "cloud9-promote-partition"
	client := fake.NewClientset(runningReplicaPod(ns, "postgres-replica-0"))
	design := dbDesignWithReplicas()

	stall := &PartitionScenario{Client: client, Scenario: scenarioStallReplica}
	if err := stall.Apply(context.Background(), ns, design, "db"); err != nil {
		t.Fatalf("stall Apply: %v", err)
	}

	promote := &PromoteScenario{Client: client, Prefer: deployer.PreferMostCurrent}
	err := promote.Apply(context.Background(), ns, design, "db")
	if !errors.Is(err, ErrPartitionAlreadyActive) {
		t.Fatalf("expected ErrPartitionAlreadyActive, got %v", err)
	}
}
