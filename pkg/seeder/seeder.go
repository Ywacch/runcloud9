package seeder

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	cloudstorage "cloud.google.com/go/storage"
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

func (s *Seeder) SeedDatabase(ctx context.Context, namespace, template string) error {
	dumpPath, err := dumpPathForTemplate(template)
	if err != nil {
		return fmt.Errorf("seeder: dumpPathForTemplate: %w", err)
	}

	dumpFile, err := s.downloadDump(ctx, dumpPath)
	if err != nil {
		return fmt.Errorf("seeder: download dump: %w", err)
	}
	defer os.Remove(dumpFile)

	pods, err := s.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=postgres",
	})
	if err != nil {
		return fmt.Errorf("seeder: list pods: %w", err)
	}
	if len(pods.Items) == 0 {
		return fmt.Errorf("seeder: postgres pod not found in namespace=%s", namespace)
	}
	podName := pods.Items[0].Name

	// Copy dump into the postgres pod.
	if err := s.copyFileToPod(ctx, namespace, podName, dumpFile); err != nil {
		return fmt.Errorf("seeder: copy dump to pod: %w", err)
	}

	// Run pg_restore inside the pod.
	if err := s.runPgRestore(ctx, namespace, podName); err != nil {
		return fmt.Errorf("seeder: pg_restore: %w", err)
	}

	return nil
}

func (s *Seeder) downloadDump(ctx context.Context, dumpPath string) (string, error) {
	client, err := cloudstorage.NewClient(ctx)
	if err != nil {
		return "", fmt.Errorf("gcs client: %w", err)
	}
	defer client.Close()

	tmp, err := os.CreateTemp("", "ganymede-seed-*.dump")
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

func (s *Seeder) copyFileToPod(ctx context.Context, namespace, podName, localPath string) error {
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
		Container: "postgres",
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

func (s *Seeder) runPgRestore(ctx context.Context, namespace, podName string) error {
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
		Container: "postgres",
		Command: []string{"sh", "-c", "PGPASSWORD=syscrack pg_restore -U syscrack -d syscrack --clean --if-exists /tmp/seed.dump"},
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
		return fmt.Errorf("pg_restore failed: %w: %s", err, stderr.String())
	}

	return nil
}

func dumpPathForTemplate(template string) (string, error) {
	switch template {
	case "url-shortener":
		return "url-shortener/urls.dump", nil
	default:
		return "", fmt.Errorf("unsupported template for seeding: %s", template)
	}
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

