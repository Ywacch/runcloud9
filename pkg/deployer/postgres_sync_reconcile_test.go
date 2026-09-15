package deployer

import (
	"context"
	"testing"
)

func TestRequiresPostgresSynchronousReplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  map[string]any
		want bool
	}{
		{
			name: "async",
			cfg: map[string]any{
				"replicationMode": "async",
				"readReplicas":    2,
			},
			want: false,
		},
		{
			name: "semi-sync",
			cfg: map[string]any{
				"replicationMode": "semi-sync",
				"readReplicas":    2,
				"syncCommitLevel": "remote_apply",
			},
			want: true,
		},
		{
			name: "sync no replicas",
			cfg: map[string]any{
				"replicationMode": "sync",
				"readReplicas":    0,
			},
			want: false,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := RequiresPostgresSynchronousReplication(tc.cfg); got != tc.want {
				t.Fatalf("RequiresPostgresSynchronousReplication() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDatabaseNodeConfig(t *testing.T) {
	t.Parallel()

	cfg := map[string]any{"engine": "postgresql"}
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{Type: "app-server"},
			{Type: "db", Config: cfg},
		},
	}
	got, ok := DatabaseNodeConfig(design)
	if !ok {
		t.Fatal("expected db config")
	}
	if got["engine"] != cfg["engine"] {
		t.Fatalf("DatabaseNodeConfig: got %v want engine postgresql", got)
	}
	if _, ok := DatabaseNodeConfig(DeploymentDesign{Nodes: []DeploymentNode{{Type: "cache"}}}); ok {
		t.Fatal("expected false without db node")
	}
}

func TestReconcilePostgresSynchronousReplication_asyncNoOp(t *testing.T) {
	t.Parallel()

	err := ReconcilePostgresSynchronousReplication(context.Background(), nil, "ns", map[string]any{
		"replicationMode": "async",
		"readReplicas":    2,
	})
	if err != nil {
		t.Fatalf("async reconcile should no-op: %v", err)
	}
}

func TestReconcilePostgresSynchronousReplication_requiresClient(t *testing.T) {
	t.Parallel()

	err := ReconcilePostgresSynchronousReplication(context.Background(), nil, "ns", map[string]any{
		"replicationMode": "semi-sync",
		"readReplicas":    2,
		"syncCommitLevel": "remote_apply",
	})
	if err == nil {
		t.Fatal("expected error without kubernetes client")
	}
}

func TestReconcilePostgresSynchronousReplicationOnPod_asyncNoOp(t *testing.T) {
	t.Parallel()

	err := ReconcilePostgresSynchronousReplicationOnPod(context.Background(), nil, "ns", "postgres-replica-0", map[string]any{
		"replicationMode": "async",
		"readReplicas":    2,
	})
	if err != nil {
		t.Fatalf("async reconcile should no-op: %v", err)
	}
}

func TestReconcilePostgresSynchronousReplicationOnPod_requiresClient(t *testing.T) {
	t.Parallel()

	err := ReconcilePostgresSynchronousReplicationOnPod(context.Background(), nil, "ns", "postgres-replica-0", map[string]any{
		"replicationMode": "semi-sync",
		"readReplicas":    2,
		"syncCommitLevel": "remote_apply",
	})
	if err == nil {
		t.Fatal("expected error without kubernetes client")
	}
}

func TestReconcilePostgresSynchronousReplicationOnPod_requiresPodName(t *testing.T) {
	t.Parallel()

	err := ReconcilePostgresSynchronousReplicationOnPod(context.Background(), nil, "ns", "", map[string]any{
		"replicationMode": "semi-sync",
		"readReplicas":    2,
		"syncCommitLevel": "remote_apply",
	})
	if err == nil {
		t.Fatal("expected error without primary pod name")
	}
}

func TestReplicationModeLabel(t *testing.T) {
	t.Parallel()

	if got := ReplicationModeLabel(map[string]any{"replicationMode": "semi-sync"}); got != "semi-sync" {
		t.Fatalf("ReplicationModeLabel: got %q", got)
	}
	if got := ReplicationModeLabel(nil); got != "async" {
		t.Fatalf("nil config: got %q", got)
	}
}

func TestSyncCommitLevelLabel(t *testing.T) {
	t.Parallel()

	if got := SyncCommitLevelLabel(map[string]any{"syncCommitLevel": "remote_apply"}); got != "remote_apply" {
		t.Fatalf("SyncCommitLevelLabel: got %q", got)
	}
}
