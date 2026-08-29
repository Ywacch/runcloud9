package seeder

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	cloudstorage "cloud.google.com/go/storage"
	"github.com/ywacch/cloud9/pkg/templates"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/util/homedir"
)

type Seeder struct {
	clientset  kubernetes.Interface
	bucketName string
}

func NewSeeder(clientset kubernetes.Interface, bucketName string) *Seeder {
	return &Seeder{clientset: clientset, bucketName: bucketName}
}

// DBCredentials are used for pg_restore inside the postgres pod.
type DBCredentials struct {
	User     string
	Password string
	Database string
}

func defaultDBCredentials() DBCredentials {
	user := os.Getenv("SEED_DB_USER")
	if user == "" {
		user = "syscrack"
	}
	pass := os.Getenv("SEED_DB_PASSWORD")
	if pass == "" {
		pass = "syscrack"
	}
	db := os.Getenv("SEED_DB_NAME")
	if db == "" {
		db = "syscrack"
	}
	return DBCredentials{User: user, Password: pass, Database: db}
}

func (s *Seeder) SeedDatabase(ctx context.Context, namespace, template string) error {
	dumpFile, err := s.PreDownloadDump(ctx, template)
	if err != nil {
		return err
	}
	return s.SeedWithDump(ctx, namespace, template, dumpFile)
}

func (s *Seeder) PreDownloadDump(ctx context.Context, template string) (string, error) {
	tmpl := templates.GetTemplate(template)
	if tmpl == nil || tmpl.SeedConfig == nil {
		return "", fmt.Errorf("seeder: no seed config for template %q", template)
	}
	cfg := tmpl.SeedConfig
	if cfg.DBType != "" && cfg.DBType != "postgres" {
		return "", fmt.Errorf("seeder: unsupported db type %q", cfg.DBType)
	}

	dumpFile, err := s.downloadDump(ctx, cfg.DumpPath)
	if err != nil {
		return "", fmt.Errorf("seeder: download dump: %w", err)
	}
	return dumpFile, nil
}

func (s *Seeder) SeedWithDump(ctx context.Context, namespace, template, dumpFile string) error {
	tmpl := templates.GetTemplate(template)
	if tmpl == nil || tmpl.SeedConfig == nil {
		return fmt.Errorf("seeder: no seed config for template %q", template)
	}
	cfg := tmpl.SeedConfig
	defer os.Remove(dumpFile)

	pods, err := s.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: cfg.PodLabel,
	})
	if err != nil {
		return fmt.Errorf("seeder: list pods: %w", err)
	}
	if len(pods.Items) == 0 {
		return fmt.Errorf("seeder: no pod matching %q in namespace=%s", cfg.PodLabel, namespace)
	}
	podName := pods.Items[0].Name

	dumpBytes := int64(-1)
	if st, err := os.Stat(dumpFile); err == nil {
		dumpBytes = st.Size()
	}

	var copyMs, restoreMs, checkpointMs int64
	seedStart := time.Now()
	logTimings := func() {
		log.Printf(
			"seeder: timings namespace=%s template=%s dump_bytes=%d copy_ms=%d restore_ms=%d checkpoint_ms=%d total_ms=%d",
			namespace, template, dumpBytes, copyMs, restoreMs, checkpointMs, time.Since(seedStart).Milliseconds(),
		)
	}

	copyStart := time.Now()
	if err := s.copyFileToPod(ctx, namespace, podName, cfg.Container, dumpFile); err != nil {
		copyMs = time.Since(copyStart).Milliseconds()
		logTimings()
		return fmt.Errorf("seeder: copy dump to pod: %w", err)
	}
	copyMs = time.Since(copyStart).Milliseconds()

	restoreStart := time.Now()
	if err := s.runPgRestore(ctx, namespace, podName, cfg.Container); err != nil {
		restoreMs = time.Since(restoreStart).Milliseconds()
		logTimings()
		return fmt.Errorf("seeder: pg_restore: %w", err)
	}
	restoreMs = time.Since(restoreStart).Milliseconds()

	checkpointStart := time.Now()
	if err := s.runCheckpoint(ctx, namespace, podName, cfg.Container); err != nil {
		checkpointMs = time.Since(checkpointStart).Milliseconds()
		log.Printf("seeder: CHECKPOINT after restore namespace=%s pod=%s: %v (pg_basebackup --checkpoint=fast still applies)", namespace, podName, err)
	} else {
		checkpointMs = time.Since(checkpointStart).Milliseconds()
	}
	logTimings()

	return nil
}

func (s *Seeder) downloadDump(ctx context.Context, dumpPath string) (string, error) {
	client, err := cloudstorage.NewClient(ctx)
	if err != nil {
		return "", fmt.Errorf("gcs client: %w", err)
	}
	defer client.Close()

	tmp, err := os.CreateTemp("", "cloud9-seed-*.dump")
	if err != nil {
		return "", fmt.Errorf("create temp file: %w", err)
	}
	defer tmp.Close()

	rc, err := client.Bucket(s.bucketName).Object(dumpPath).NewReader(ctx)
	if err != nil {
		return "", fmt.Errorf("gcs read %s: %w", dumpPath, err)
	}
	defer rc.Close()

	if _, err := io.Copy(tmp, rc); err != nil {
		return "", fmt.Errorf("download dump: %w", err)
	}

	return tmp.Name(), nil
}

func (s *Seeder) copyFileToPod(ctx context.Context, namespace, podName, container, localPath string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open dump file: %w", err)
	}
	defer f.Close()

	execCfg, err := kubeExecConfig()
	if err != nil {
		return fmt.Errorf("kube exec config: %w", err)
	}

	req := s.clientset.CoreV1().RESTClient().
		Post().
		Resource("pods").
		Namespace(namespace).
		Name(podName).
		SubResource("exec")

	opts := &corev1.PodExecOptions{
		Container: container,
		Command:   []string{"sh", "-c", "cat > /tmp/seed.dump"},
		Stdin:     true,
		Stdout:    false,
		Stderr:    true,
		TTY:       false,
	}
	req.VersionedParams(opts, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(execCfg, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("new spdy executor: %w", err)
	}

	var stderr bytes.Buffer
	if err := executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  f,
		Stdout: nil,
		Stderr: &stderr,
	}); err != nil {
		return fmt.Errorf("exec cat failed: %w: %s", err, stderr.String())
	}

	return nil
}

func (s *Seeder) runPgRestore(ctx context.Context, namespace, podName, container string) error {
	creds := defaultDBCredentials()
	if err := s.execArgv(ctx, namespace, podName, container, pgRestoreCommand(creds)); err != nil {
		return fmt.Errorf("pg_restore failed: %w", err)
	}
	return nil
}

func (s *Seeder) runCheckpoint(ctx context.Context, namespace, podName, container string) error {
	creds := defaultDBCredentials()
	if err := s.execArgv(ctx, namespace, podName, container, checkpointCommand(creds)); err != nil {
		return fmt.Errorf("CHECKPOINT failed: %w", err)
	}
	return nil
}

func pgRestoreCommand(creds DBCredentials) []string {
	return []string{
		"env",
		fmt.Sprintf("PGPASSWORD=%s", creds.Password),
		"pg_restore",
		"-U", creds.User,
		"-d", creds.Database,
		"--clean",
		"--if-exists",
		"/tmp/seed.dump",
	}
}

func checkpointCommand(creds DBCredentials) []string {
	return []string{
		"env",
		fmt.Sprintf("PGPASSWORD=%s", creds.Password),
		"psql",
		"-U", creds.User,
		"-d", creds.Database,
		"-v", "ON_ERROR_STOP=1",
		"-c", "CHECKPOINT",
	}
}

func (s *Seeder) execArgv(ctx context.Context, namespace, podName, container string, cmd []string) error {
	execCfg, err := kubeExecConfig()
	if err != nil {
		return fmt.Errorf("kube exec config: %w", err)
	}

	req := s.clientset.CoreV1().RESTClient().
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
		return fmt.Errorf("new spdy executor: %w", err)
	}

	var stdout, stderr bytes.Buffer
	if err := executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  nil,
		Stdout: &stdout,
		Stderr: &stderr,
	}); err != nil {
		return fmt.Errorf("%w: %s", err, stderr.String())
	}
	return nil
}

func kubeExecConfig() (*rest.Config, error) {
	// Prefer KUBECONFIG when set (also lets you target GKE with a specific config file).
	if kubeconfigPath := os.Getenv("KUBECONFIG"); kubeconfigPath != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	}

	// If running outside a cluster, ~/.kube/config is the typical default.
	defaultPath := filepath.Join(homedir.HomeDir(), ".kube", "config")
	if _, err := os.Stat(defaultPath); err == nil {
		return clientcmd.BuildConfigFromFlags("", defaultPath)
	}

	// Fallback for in-cluster usage.
	return rest.InClusterConfig()
}
