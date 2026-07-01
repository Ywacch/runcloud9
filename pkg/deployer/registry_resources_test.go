package deployer

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestDBResources_matchesSmallProfile(t *testing.T) {
	t.Parallel()

	got := DBResources()
	want := ResourceProfile("small")
	assertResourceRequirementsEqual(t, got, want)
}

func TestComponentRegistry_dbUsesSmallPostgresSizing(t *testing.T) {
	t.Parallel()

	spec, ok := componentRegistry["db"]
	if !ok {
		t.Fatal("componentRegistry missing db entry")
	}
	assertResourceRequirementsEqual(t, spec.Resources, DBResources())
}

func assertResourceRequirementsEqual(t *testing.T, got, want corev1.ResourceRequirements) {
	t.Helper()

	if got.Limits.Cpu().Cmp(*want.Limits.Cpu()) != 0 {
		t.Fatalf("cpu limit: got %s want %s", got.Limits.Cpu(), want.Limits.Cpu())
	}
	if got.Limits.Memory().Cmp(*want.Limits.Memory()) != 0 {
		t.Fatalf("memory limit: got %s want %s", got.Limits.Memory(), want.Limits.Memory())
	}
	if got.Requests.Cpu().String() != "125m" {
		t.Fatalf("cpu request: got %s want 125m", got.Requests.Cpu())
	}
	if got.Requests.Memory().String() != "256Mi" {
		t.Fatalf("memory request: got %s want 256Mi", got.Requests.Memory())
	}
}
