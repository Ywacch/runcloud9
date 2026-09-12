package chaos

import (
	"strings"
	"testing"

	"github.com/ywacch/cloud9/pkg/deployer"
	"k8s.io/client-go/kubernetes/fake"
)

func TestResolve_knownScenarios(t *testing.T) {
	client := fake.NewClientset()
	design := deployer.DeploymentDesign{
		Nodes: []deployer.DeploymentNode{
			{Type: "app-server", Name: "App"},
			{Type: "cache", Name: "Cache"},
		},
	}

	for _, tc := range []struct {
		scenario string
		target   string
	}{
		{scenario: "pod_kill", target: "app-server"},
		{scenario: "cache_flush", target: "cache"},
	} {
		s, err := Resolve(tc.scenario, tc.target, design, client)
		if err != nil {
			t.Fatalf("Resolve(%q, %q): %v", tc.scenario, tc.target, err)
		}
		if s == nil {
			t.Fatalf("Resolve(%q, %q) returned nil", tc.scenario, tc.target)
		}
	}
}

func TestResolve_unknownScenario(t *testing.T) {
	design := deployer.DeploymentDesign{
		Nodes: []deployer.DeploymentNode{{Type: "app-server", Name: "App"}},
	}
	_, err := Resolve("unknown", "app-server", design, fake.NewClientset())
	if err == nil || !strings.Contains(err.Error(), "unknown scenario") {
		t.Fatalf("expected unknown scenario error, got %v", err)
	}
}
