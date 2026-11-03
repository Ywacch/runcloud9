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
	if len(got) != 3 || got[0] != "-m" || got[2] != "-v" {
		t.Fatalf("got %v", got)
	}
	// 90% of 256Mi ≈ 230Mi
	if got[1] != "230" && got[1] != "229" {
		t.Fatalf("unexpected -m value %q", got[1])
	}
}

func TestMemcachedServerArgs_Floor32(t *testing.T) {
	t.Parallel()
	res := corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("16Mi"),
		},
	}
	got := memcachedServerArgs(nil, res)
	if len(got) < 2 || got[1] != "32" {
		t.Fatalf("small limit should floor at 32 MiB, got %v", got)
	}
}

func TestMemcachedServerArgs_Verbose(t *testing.T) {
	t.Parallel()
	got := memcachedServerArgs(nil, corev1.ResourceRequirements{})
	if len(got) < 2 || got[len(got)-1] != "-v" {
		t.Fatalf("args %v want trailing -v", got)
	}
	for _, arg := range got {
		if arg == "-vv" {
			t.Fatal("memcached args must not include -vv")
		}
	}
}
