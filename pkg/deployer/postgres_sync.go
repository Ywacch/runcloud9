package deployer

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/util/homedir"
)

const (
	postgresPrimaryAppLabel    = "postgres"
	postgresPrimaryContainer   = "postgres"
	postgresDBUser             = "syscrack"
	postgresDBName             = "syscrack"
	enablePostgresSyncTimeout  = 90 * time.Second
	postgresSyncPollInterval   = waitReadyPollInterval
	sqlCountStreamingReplicas  = "SELECT count(*)::text FROM pg_stat_replication WHERE state = 'streaming'"
	sqlCountSyncQuorumReplicas = "SELECT count(*)::text FROM pg_stat_replication WHERE sync_state IN ('sync','quorum')"
)

// postgresSyncGUCs holds synchronous replication settings applied after standbys stream.
type postgresSyncGUCs struct {
	StandbyNames string
	CommitLevel  string
}

// postgresAdmin runs SQL against the postgres primary (test seam; production uses pod exec).
type postgresAdmin interface {
	execSQL(ctx context.Context, query string) (stdout string, err error)
}

// postgresSyncGUCsFromConfig derives standby names and commit level for semi-sync/sync modes.
func postgresSyncGUCsFromConfig(nodeConfig map[string]any) (postgresSyncGUCs, bool) {
	readReplicas := parseReadReplicaCount(nodeConfig)
	if readReplicas < 1 {
		return postgresSyncGUCs{}, false
	}
	mode, err := parseReplicationMode(nodeConfig)
	if err != nil || !requiresSynchronousReplication(mode) {
		return postgresSyncGUCs{}, false
	}
	level := parseSyncCommitLevel(nodeConfig)
	var standbyNames string
	switch mode {
	case replicationModeSemiSync:
		standbyNames = "ANY 1 (*)"
	case replicationModeSync:
		standbyNames = fmt.Sprintf("FIRST %d (*)", readReplicas)
	default:
		return postgresSyncGUCs{}, false
	}
	return postgresSyncGUCs{
		StandbyNames: standbyNames,
		CommitLevel:  level,
	}, true
}

// enableSynchronousReplicationSQL returns ALTER SYSTEM + reload statements.
// Writes postgresql.auto.conf in ephemeral PGDATA; survives container restart in the
// same pod but is lost if the pod is recreated (workstream 2).
func enableSynchronousReplicationSQL(gucs postgresSyncGUCs) string {
	return fmt.Sprintf(
		"ALTER SYSTEM SET synchronous_standby_names = '%s'; "+
			"ALTER SYSTEM SET synchronous_commit = '%s'; "+
			"SELECT pg_reload_conf();",
		escapePostgresSQLLiteral(gucs.StandbyNames),
		escapePostgresSQLLiteral(gucs.CommitLevel),
	)
}

func escapePostgresSQLLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func parseReplicationCount(stdout string) (int, error) {
	raw := strings.TrimSpace(stdout)
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("parse replication count %q: %w", raw, err)
	}
	return n, nil
}

func normalizeShowGUC(stdout string) string {
	return strings.Trim(strings.TrimSpace(stdout), "'")
}

func gucsMatchReadBack(want postgresSyncGUCs, standbyNames, commitLevel string) bool {
	return normalizeShowGUC(standbyNames) == want.StandbyNames &&
		normalizeShowGUC(commitLevel) == want.CommitLevel
}

func waitForReplicationCount(ctx context.Context, admin postgresAdmin, query string, minCount int) error {
	ticker := time.NewTicker(postgresSyncPollInterval)
	defer ticker.Stop()

	for {
		stdout, err := admin.execSQL(ctx, query)
		if err != nil {
			return fmt.Errorf("poll replication: %w", err)
		}
		count, err := parseReplicationCount(stdout)
		if err != nil {
			return err
		}
		if count >= minCount {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for %d replication rows (got %d): %w", minCount, count, ctx.Err())
		case <-ticker.C:
		}
	}
}

func enablePostgresSynchronousReplication(ctx context.Context, admin postgresAdmin, nodeConfig map[string]any, minReady int32) error {
	if minReady <= 0 {
		return nil
	}
	gucs, ok := postgresSyncGUCsFromConfig(nodeConfig)
	if !ok {
		return fmt.Errorf("synchronous replication requested but GUCs could not be derived from config")
	}

	ctx, cancel := context.WithTimeout(ctx, enablePostgresSyncTimeout)
	defer cancel()

	if err := waitForReplicationCount(ctx, admin, sqlCountStreamingReplicas, int(minReady)); err != nil {
		return fmt.Errorf("wait for streaming standbys: %w", err)
	}

	if _, err := admin.execSQL(ctx, enableSynchronousReplicationSQL(gucs)); err != nil {
		return fmt.Errorf("apply synchronous replication GUCs: %w", err)
	}

	if err := waitForReplicationCount(ctx, admin, sqlCountSyncQuorumReplicas, int(minReady)); err != nil {
		return fmt.Errorf("wait for sync/quorum standbys: %w", err)
	}

	gotStandby, err := admin.execSQL(ctx, "SHOW synchronous_standby_names")
	if err != nil {
		return fmt.Errorf("read back synchronous_standby_names: %w", err)
	}
	gotCommit, err := admin.execSQL(ctx, "SHOW synchronous_commit")
	if err != nil {
		return fmt.Errorf("read back synchronous_commit: %w", err)
	}
	if !gucsMatchReadBack(gucs, gotStandby, gotCommit) {
		return fmt.Errorf(
			"synchronous replication read-back mismatch: got standby_names=%q commit=%q want standby_names=%q commit=%q",
			normalizeShowGUC(gotStandby), normalizeShowGUC(gotCommit), gucs.StandbyNames, gucs.CommitLevel,
		)
	}
	return nil
}

type k8sPostgresAdmin struct {
	client    kubernetes.Interface
	namespace string
}

func (k *K8sDeployer) newPostgresAdmin(namespace string) *k8sPostgresAdmin {
	return &k8sPostgresAdmin{client: k.clientset, namespace: namespace}
}

func (a *k8sPostgresAdmin) execSQL(ctx context.Context, query string) (string, error) {
	podName, err := a.primaryPodName(ctx)
	if err != nil {
		return "", err
	}
	cmd := []string{
		"psql",
		"-U", postgresDBUser,
		"-d", postgresDBName,
		"-v", "ON_ERROR_STOP=1",
		"-A", "-t",
		"-c", query,
	}
	return execInPod(ctx, a.client, a.namespace, podName, postgresPrimaryContainer, cmd)
}

func (a *k8sPostgresAdmin) primaryPodName(ctx context.Context) (string, error) {
	labelSelector := "deployment=" + a.namespace + ",app=" + postgresPrimaryAppLabel
	pods, err := a.client.CoreV1().Pods(a.namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		return "", fmt.Errorf("list postgres pods: %w", err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil {
			continue
		}
		return pod.Name, nil
	}
	return "", fmt.Errorf("no postgres primary pod in namespace %s", a.namespace)
}

func execInPod(ctx context.Context, client kubernetes.Interface, namespace, podName, container string, cmd []string) (string, error) {
	execCfg, err := kubeExecConfig()
	if err != nil {
		return "", fmt.Errorf("kube exec config: %w", err)
	}

	req := client.CoreV1().RESTClient().
		Post().
		Resource("pods").
		Namespace(namespace).
		Name(podName).
		SubResource("exec")

	opts := &corev1.PodExecOptions{
		Container: container,
		Command:   cmd,
		Stdin:     false,
		Stdout:    true,
		Stderr:    true,
		TTY:       false,
	}
	req.VersionedParams(opts, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(execCfg, "POST", req.URL())
	if err != nil {
		return "", fmt.Errorf("new spdy executor: %w", err)
	}

	var stdout, stderr bytes.Buffer
	if err := executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	}); err != nil {
		return "", fmt.Errorf("exec %v: %w: %s", cmd, err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

func kubeExecConfig() (*rest.Config, error) {
	if kubeconfigPath := os.Getenv("KUBECONFIG"); kubeconfigPath != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	}
	defaultPath := filepath.Join(homedir.HomeDir(), ".kube", "config")
	if _, err := os.Stat(defaultPath); err == nil {
		return clientcmd.BuildConfigFromFlags("", defaultPath)
	}
	return rest.InClusterConfig()
}
