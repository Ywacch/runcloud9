package deployer

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestEnsureNamespaceResourceQuota_createsQuota(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	ns := "cloud9-test-quota"
	design := urlShortenerDefaultDesign()

	if err := EnsureNamespaceResourceQuota(context.Background(), client, ns, design, "explorer"); err != nil {
		t.Fatalf("EnsureNamespaceResourceQuota: %v", err)
	}

	_, err := client.CoreV1().Namespaces().Get(context.Background(), ns, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("namespace not found: %v", err)
	}

	rq, err := client.CoreV1().ResourceQuotas(ns).Get(
		context.Background(),
		namespaceResourceQuotaName,
		metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("resource quota not found: %v", err)
	}
	if rq.Spec.Hard == nil {
		t.Fatal("expected hard quota limits")
	}
	gotCPU := rq.Spec.Hard[corev1.ResourceLimitsCPU]
	wantCPU := resource.MustParse("925m")
	if gotCPU.Cmp(wantCPU) != 0 {
		t.Fatalf("quota cpu: got %v want %v", gotCPU, wantCPU)
	}
	gotMem := rq.Spec.Hard[corev1.ResourceLimitsMemory]
	wantMem := resource.MustParse("1472Mi")
	if gotMem.Cmp(wantMem) != 0 {
		t.Fatalf("quota memory: got %v want %v", gotMem, wantMem)
	}
}

func TestEnsureNamespaceResourceQuota_idempotent(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	ns := "cloud9-test-quota-idempotent"
	design := urlShortenerDefaultDesign()

	if err := EnsureNamespaceResourceQuota(context.Background(), client, ns, design, "explorer"); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if err := EnsureNamespaceResourceQuota(context.Background(), client, ns, design, "explorer"); err != nil {
		t.Fatalf("second call: %v", err)
	}
}
