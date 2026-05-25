package chaos

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/util/homedir"
)

// execInPod runs a command in a pod container (SPDYExecutor, same pattern as pkg/seeder).
func execInPod(ctx context.Context, client kubernetes.Interface, namespace, podName, container string, cmd []string) (stdout, stderr string, err error) {
	execCfg, err := kubeExecConfig()
	if err != nil {
		return "", "", fmt.Errorf("kube exec config: %w", err)
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
		return "", "", fmt.Errorf("new spdy executor: %w", err)
	}

	var outBuf, errBuf bytes.Buffer
	if err := executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  nil,
		Stdout: &outBuf,
		Stderr: &errBuf,
	}); err != nil {
		return outBuf.String(), errBuf.String(), fmt.Errorf("exec %v: %w: %s", cmd, err, errBuf.String())
	}
	return outBuf.String(), errBuf.String(), nil
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
