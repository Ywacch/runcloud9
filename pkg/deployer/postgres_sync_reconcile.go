package deployer

import (
	"context"
	"fmt"

	"k8s.io/client-go/kubernetes"
)

const reconcilePostgresSyncTimeout = 3 * enablePostgresSyncTimeout

// RequiresPostgresSynchronousReplication reports whether the db node design enables
// semi-sync or sync replication with at least one read replica.
func RequiresPostgresSynchronousReplication(nodeConfig map[string]any) bool {
	_, ok := postgresSyncGUCsFromConfig(nodeConfig)
	return ok
}

// DatabaseNodeConfig returns the single db node config from a deployment design.
func DatabaseNodeConfig(design DeploymentDesign) (map[string]any, bool) {
	for _, n := range design.Nodes {
		if n.Type == "db" {
			return n.Config, true
		}
	}
	return nil, false
}

// ReconcilePostgresSynchronousReplication re-applies synchronous replication GUCs after
// primary pod recreation. emptyDir PGDATA wipes postgresql.auto.conf; the primary boots
// async until this runs. Call after chaos pod_kill on the database or node reschedule.
func ReconcilePostgresSynchronousReplication(ctx context.Context, clientset kubernetes.Interface, namespace string, nodeConfig map[string]any) error {
	mode, err := parseReplicationMode(nodeConfig)
	if err != nil {
		return err
	}
	rr := parseReadReplicaCount(nodeConfig)
	minReady := seedReplicaReadyBeforeRestore(mode, rr)
	if minReady == 0 {
		return nil
	}
	if _, ok := postgresSyncGUCsFromConfig(nodeConfig); !ok {
		return nil
	}

	if clientset == nil {
		return fmt.Errorf("kubernetes client is required")
	}
	if namespace == "" {
		return fmt.Errorf("namespace is required")
	}

	ctx, cancel := context.WithTimeout(ctx, reconcilePostgresSyncTimeout)
	defer cancel()

	d := &K8sDeployer{clientset: clientset}
	cfg := DeployConfig{Namespace: namespace}

	if err := d.waitForReady(ctx, cfg, postgresPrimaryAppLabel, 1, waitProgressOpts{}); err != nil {
		return fmt.Errorf("wait for postgres primary after recreate: %w", err)
	}
	prefix := []PodWaitProgress{{Ordinal: 1, Ready: true, WaitCode: WaitCodeReady}}
	if err := d.waitForReady(ctx, cfg, dbReplicaAppLabel, minReady, waitProgressOpts{prefix: prefix}); err != nil {
		return fmt.Errorf("wait for postgres replicas after recreate: %w", err)
	}

	admin := d.newPostgresAdmin(namespace)
	if err := enablePostgresSynchronousReplication(ctx, admin, nodeConfig, minReady); err != nil {
		return fmt.Errorf("re-enable postgres synchronous replication: %w", err)
	}
	return nil
}

// ReconcilePostgresSynchronousReplicationOnPod re-applies synchronous replication GUCs on a
// promoted standby pod acting as the write primary. The postgres Deployment is scaled to
// zero after promotion, so reconciliation cannot use the app=postgres pod lookup.
func ReconcilePostgresSynchronousReplicationOnPod(ctx context.Context, clientset kubernetes.Interface, namespace, primaryPodName string, nodeConfig map[string]any) error {
	mode, err := parseReplicationMode(nodeConfig)
	if err != nil {
		return err
	}
	rr := parseReadReplicaCount(nodeConfig)
	return reconcilePostgresSynchronousReplicationOnPod(ctx, clientset, namespace, primaryPodName, nodeConfig, seedReplicaReadyBeforeRestore(mode, rr))
}

// ReconcilePostgresSynchronousReplicationOnPodWithMinReady re-applies sync GUCs after
// promotion using an explicit streaming standby count (post-promote leftovers only).
func ReconcilePostgresSynchronousReplicationOnPodWithMinReady(ctx context.Context, clientset kubernetes.Interface, namespace, primaryPodName string, nodeConfig map[string]any, minReady int32) error {
	return reconcilePostgresSynchronousReplicationOnPod(ctx, clientset, namespace, primaryPodName, nodeConfig, minReady)
}

func reconcilePostgresSynchronousReplicationOnPod(ctx context.Context, clientset kubernetes.Interface, namespace, primaryPodName string, nodeConfig map[string]any, minReady int32) error {
	if minReady <= 0 {
		return nil
	}
	if _, ok := postgresSyncGUCsFromConfig(nodeConfig); !ok {
		return nil
	}
	if clientset == nil {
		return fmt.Errorf("kubernetes client is required")
	}
	if namespace == "" {
		return fmt.Errorf("namespace is required")
	}
	if primaryPodName == "" {
		return fmt.Errorf("primary pod name is required")
	}

	ctx, cancel := context.WithTimeout(ctx, reconcilePostgresSyncTimeout)
	defer cancel()

	d := &K8sDeployer{clientset: clientset}
	cfg := DeployConfig{Namespace: namespace}
	if err := d.waitForReady(ctx, cfg, dbReplicaAppLabel, minReady, waitProgressOpts{}); err != nil {
		return fmt.Errorf("wait for postgres replicas after promotion: %w", err)
	}

	admin := NewPostgresPodAdmin(clientset, namespace, primaryPodName, dbReplicaAppLabel)
	if err := enablePostgresSynchronousReplication(ctx, admin, nodeConfig, minReady); err != nil {
		return fmt.Errorf("re-enable postgres synchronous replication: %w", err)
	}
	return nil
}

// ReplicationModeLabel returns the normalized replication mode from db node config.
func ReplicationModeLabel(nodeConfig map[string]any) string {
	mode, err := parseReplicationMode(nodeConfig)
	if err != nil {
		return replicationModeAsync
	}
	return mode
}

// SyncCommitLevelLabel returns synchronous_commit level from db node config.
func SyncCommitLevelLabel(nodeConfig map[string]any) string {
	return parseSyncCommitLevel(nodeConfig)
}
