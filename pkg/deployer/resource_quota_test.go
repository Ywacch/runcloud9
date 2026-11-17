package deployer

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestEnsureNamespaceResourceQuota_createsQuota(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	ns := "cloud9-test-quota"
	design := urlShortenerDefaultDesign()

	if err := EnsureNamespaceResourceQuota(context.Background(), client, ns, design, "explorer", time.Time{}); err != nil {
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
	wantMem := resource.MustParse("2112Mi")
	if gotMem.Cmp(wantMem) != 0 {
		t.Fatalf("quota memory: got %v want %v", gotMem, wantMem)
	}
}

func TestEnsureNamespaceResourceQuota_setsExpiryLabelOnCreate(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	ns := "cloud9-test-quota-expiry"
	expires := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	if err := EnsureNamespaceResourceQuota(context.Background(), client, ns, urlShortenerDefaultDesign(), "explorer", expires); err != nil {
		t.Fatal(err)
	}
	got, err := client.CoreV1().Namespaces().Get(context.Background(), ns, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := namespaceExpiresAtLabelValue(expires)
	if got.Labels[NamespaceExpiresAtLabel] != want {
		t.Fatalf("label = %q want %q", got.Labels[NamespaceExpiresAtLabel], want)
	}
}

func TestSetNamespaceExpiresAtLabel_RetriesConflict(t *testing.T) {
	t.Parallel()

	const ns = "cloud9-label-conflict"
	client := fake.NewSimpleClientset(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns, ResourceVersion: "1"},
	})
	updates := 0
	client.PrependReactor("update", "namespaces", func(clienttesting.Action) (bool, runtime.Object, error) {
		updates++
		if updates == 1 {
			return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "namespaces"}, ns, fmt.Errorf("modified"))
		}
		return false, nil, nil
	})
	expires := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	if err := setNamespaceExpiresAtLabel(context.Background(), client, ns, expires); err != nil {
		t.Fatal(err)
	}
	if updates != 2 {
		t.Fatalf("updates = %d want 2", updates)
	}
	got, err := client.CoreV1().Namespaces().Get(context.Background(), ns, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := namespaceExpiresAtLabelValue(expires)
	if got.Labels[NamespaceExpiresAtLabel] != want {
		t.Fatalf("label = %q want %q", got.Labels[NamespaceExpiresAtLabel], want)
	}
}

func TestEnsureNamespaceResourceQuota_idempotent(t *testing.T) {
	t.Parallel()

	client := fake.NewSimpleClientset()
	ns := "cloud9-test-quota-idempotent"
	design := urlShortenerDefaultDesign()

	if err := EnsureNamespaceResourceQuota(context.Background(), client, ns, design, "explorer", time.Time{}); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if err := EnsureNamespaceResourceQuota(context.Background(), client, ns, design, "explorer", time.Time{}); err != nil {
		t.Fatalf("second call: %v", err)
	}
}
