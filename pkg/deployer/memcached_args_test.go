package deployer

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestMemcachedServerArgs_MemoryLimit(t *testing.T) {
	t.Parallel()
	res := corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
	}
	got := memcachedServerArgs(nil, res)
	if len(got) != 2 || got[0] != "-m" {
		t.Fatalf("got %v", got)
	}
	// 90% of 256Mi ≈ 230Mi
	if got[1] != "230" && got[1] != "229" {
		t.Fatalf("unexpected -m value %q", got[1])
	}
}
