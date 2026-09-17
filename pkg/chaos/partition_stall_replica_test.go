package chaos

import (
	"context"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

func TestValidateReplicaPodName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		pod     string
		wantErr bool
	}{
		{name: "valid", pod: "postgres-replica-0", wantErr: false},
		{name: "valid multi digit", pod: "postgres-replica-12", wantErr: false},
		{name: "underscore", pod: "postgres_replica_0", wantErr: true},
		{name: "empty", pod: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateReplicaPodName(tt.pod)
			if tt.wantErr && err == nil {
				t.Fatal("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestCutStalledReplicaReplication_fakeClientSkipsLiveCut(t *testing.T) {
	t.Parallel()
	var slept time.Duration
	var dropped bool
	s := &PartitionScenario{
		Client:   fake.NewClientset(),
		Scenario: scenarioStallReplica,
		Sleep:    func(d time.Duration) { slept = d },
		DropReplication: func(context.Context, kubernetes.Interface, string, string) error {
			dropped = true
			return nil
		},
	}
	if err := s.cutStalledReplicaReplication(context.Background(), "ns", "postgres-replica-0"); err != nil {
		t.Fatalf("cut: %v", err)
	}
	if slept != stallReplicaEnforceDelay {
		t.Fatalf("sleep: got %v want %v", slept, stallReplicaEnforceDelay)
	}
	if !dropped {
		t.Fatal("expected explicit drop hook to run")
	}

	s = &PartitionScenario{
		Client:   fake.NewClientset(),
		Scenario: scenarioStallReplica,
	}
	if err := s.cutStalledReplicaReplication(context.Background(), "ns", "postgres-replica-0"); err != nil {
		t.Fatalf("cut with fake client: %v", err)
	}
}

func TestCutStalledReplicaReplication_invalidPodName(t *testing.T) {
	t.Parallel()
	s := &PartitionScenario{
		Client:   fake.NewClientset(),
		Scenario: scenarioStallReplica,
		Sleep:    func(time.Duration) {},
		DropReplication: func(context.Context, kubernetes.Interface, string, string) error {
			t.Fatal("drop should not run")
			return nil
		},
	}
	if err := s.cutStalledReplicaReplication(context.Background(), "ns", "bad-name"); err == nil {
		t.Fatal("expected error for invalid pod name")
	}
}
