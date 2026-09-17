package chaos

import (
	"context"
	"strings"
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

func TestRewriteOverlayConninfo(t *testing.T) {
	t.Parallel()
	auto := strings.Join([]string{
		"primary_conninfo = 'user=syscrack passfile=''/root/.pgpass'' channel_binding=prefer host=db port=5432 sslmode=prefer'",
		"primary_conninfo = 'host=db port=5432 user=syscrack application_name=postgres-replica-0 sslmode=disable'",
		"primary_slot_name = 'postgres_replica_0'",
		"",
	}, "\n")

	stalled, err := rewriteOverlayConninfo(auto, "postgres-replica-0", stallBlackholeHost)
	if err != nil {
		t.Fatalf("blackhole: %v", err)
	}
	if !strings.Contains(stalled, "host=192.0.2.1 port=5432 user=syscrack application_name=postgres-replica-0 sslmode=disable") {
		t.Fatalf("overlay not blackholed:\n%s", stalled)
	}
	if !strings.Contains(stalled, "passfile=''/root/.pgpass''") {
		t.Fatal("basebackup conninfo line must stay")
	}
	if !strings.Contains(stalled, "primary_slot_name = 'postgres_replica_0'") {
		t.Fatal("primary_slot_name GUC must stay")
	}

	restored, err := rewriteOverlayConninfo(stalled, "postgres-replica-0", stallPrimaryHost)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if restored != auto {
		t.Fatalf("restore mismatch\ngot:\n%s\nwant:\n%s", restored, auto)
	}
}

func TestRewriteOverlayConninfo_missingOverlay(t *testing.T) {
	t.Parallel()
	_, err := rewriteOverlayConninfo("primary_slot_name = 'postgres_replica_0'\n", "postgres-replica-0", stallBlackholeHost)
	if err == nil {
		t.Fatal("expected error when overlay conninfo is missing")
	}
}

func TestRewriteOverlayConninfo_ignoresOtherReplica(t *testing.T) {
	t.Parallel()
	auto := "primary_conninfo = 'host=db port=5432 user=syscrack application_name=postgres-replica-1 sslmode=disable'\n"
	_, err := rewriteOverlayConninfo(auto, "postgres-replica-0", stallBlackholeHost)
	if err == nil {
		t.Fatal("expected error when overlay belongs to another replica")
	}
}

func TestApplyStallReplica_fakeClientSkipsLiveCut(t *testing.T) {
	t.Parallel()
	ns := "ns-fake-stall"
	client := fake.NewClientset(runningReplicaPod(ns, "postgres-replica-0"))
	s := &PartitionScenario{
		Client:   client,
		Scenario: scenarioStallReplica,
	}
	if err := s.Apply(context.Background(), ns, dbDesignWithReplicas(), "db"); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if err := s.Revert(context.Background(), ns, dbDesignWithReplicas(), "db"); err != nil {
		t.Fatalf("revert: %v", err)
	}
}

func TestApplyStallReplica_invalidPodName(t *testing.T) {
	t.Parallel()
	s := &PartitionScenario{
		Client:   fake.NewClientset(),
		Scenario: scenarioStallReplica,
		SetReplicaPrimaryHost: func(context.Context, kubernetes.Interface, string, string, string) error {
			t.Fatal("set host should not run")
			return nil
		},
		DropReplication: func(context.Context, kubernetes.Interface, string, string) error {
			t.Fatal("drop should not run")
			return nil
		},
	}
	if err := s.dropStalledReplica(context.Background(), "ns", "bad-name"); err == nil {
		t.Fatal("expected error for invalid pod name")
	}
}

func TestApplyStallReplica_explicitHooks(t *testing.T) {
	t.Parallel()
	var hosts []string
	var dropped bool
	s := &PartitionScenario{
		Client:   fake.NewClientset(),
		Scenario: scenarioStallReplica,
		SetReplicaPrimaryHost: func(_ context.Context, _ kubernetes.Interface, _, _, host string) error {
			hosts = append(hosts, host)
			return nil
		},
		DropReplication: func(context.Context, kubernetes.Interface, string, string) error {
			dropped = true
			return nil
		},
	}
	if err := s.setReplicaHost(context.Background(), "ns", "postgres-replica-0", stallBlackholeHost); err != nil {
		t.Fatalf("set host: %v", err)
	}
	if err := s.dropStalledReplica(context.Background(), "ns", "postgres-replica-0"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if len(hosts) != 1 || hosts[0] != stallBlackholeHost {
		t.Fatalf("hosts: %v", hosts)
	}
	if !dropped {
		t.Fatal("expected drop hook to run")
	}
}

func TestBuildStallStateConfigMap(t *testing.T) {
	t.Parallel()
	expires := time.Date(2026, 9, 17, 20, 0, 0, 0, time.UTC)
	cm := buildStallStateConfigMap("ns", "postgres-replica-0", "db", expires)
	if cm.Name != "c9-chaos-stall-replica" {
		t.Fatalf("name: %q", cm.Name)
	}
	if cm.Labels[ChaosLabelScenario] != scenarioStallReplica || cm.Labels[ChaosLabelTarget] != "db" {
		t.Fatalf("labels: %+v", cm.Labels)
	}
	if cm.Annotations[ChaosAnnotationReplicaPod] != "postgres-replica-0" {
		t.Fatalf("replica annotation: %+v", cm.Annotations)
	}
	if cm.Annotations[ChaosAnnotationExpiresAt] != expires.Format(time.RFC3339) {
		t.Fatalf("expires: %q", cm.Annotations[ChaosAnnotationExpiresAt])
	}
}
