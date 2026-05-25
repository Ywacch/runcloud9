package chaos

import (
	"testing"

	"k8s.io/client-go/kubernetes/fake"
)

func TestNewScenario(t *testing.T) {
	client := fake.NewClientset()

	for _, name := range []string{"pod_kill", "cache_flush"} {
		s, err := NewScenario(name, client)
		if err != nil {
			t.Fatalf("NewScenario(%q): %v", name, err)
		}
		if s == nil {
			t.Fatalf("NewScenario(%q) returned nil", name)
		}
	}

	if _, err := NewScenario("unknown", client); err == nil {
		t.Fatal("expected error for unknown scenario")
	}
}
