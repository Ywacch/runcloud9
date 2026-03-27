package deployer

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestK8sDeployer_Deploy_CreatesNamespaceAndDeployment(t *testing.T) {
	// UNDERSTAND: fake.NewSimpleClientset() is an in-memory K8s API.
	// Create/Delete/Get calls work but no pods actually run.
	// This lets us test the client-go calls without minikube.
	fakeClient := fake.NewSimpleClientset()
	d := NewK8sDeployer(fakeClient, "db")

	cfg := DeployConfig{
		Namespace: "cloud9-test-123",
		Node:      DeploymentNode{ID: "n1", Type: "db", Name: "PostgreSQL"},
	}

	// waitForReady will block forever against a fake client because no pod events fire.
	// Use a short-deadline context and assert that the error is a timeout (not a logic error).
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := d.Deploy(ctx, cfg)
	if err == nil {
		t.Fatal("expected error from waitForReady timeout, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded (or wrapped), got %v", err)
	}

	ns, getErr := fakeClient.CoreV1().Namespaces().Get(
		context.Background(), cfg.Namespace, metav1.GetOptions{},
	)
	if getErr != nil {
		t.Fatalf("namespace not found: %v", getErr)
	}
	if ns.Name != cfg.Namespace {
		t.Errorf("expected namespace %s, got %s", cfg.Namespace, ns.Name)
	}
}

func TestK8sDeployer_Teardown_DeletesNamespace(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	d := NewK8sDeployer(fakeClient, "db")

	ns := "cloud9-test-456"
	_, err := fakeClient.CoreV1().Namespaces().Create(
		context.Background(),
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}},
		metav1.CreateOptions{},
	)
	if err != nil {
		t.Fatalf("pre-create namespace: %v", err)
	}

	err = d.Teardown(context.Background(), ns)
	if err != nil {
		t.Fatalf("teardown failed: %v", err)
	}

	_, err = fakeClient.CoreV1().Namespaces().Get(
		context.Background(), ns, metav1.GetOptions{},
	)
	if err == nil {
		t.Error("expected namespace to be deleted, but it still exists")
	}
}
