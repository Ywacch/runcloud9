package chaos

import (
	"strings"
	"testing"

	"github.com/ywacch/cloud9/pkg/deployer"
)

func TestResolveTarget(t *testing.T) {
	cacheAside := deployer.DeploymentDesign{
		Nodes: []deployer.DeploymentNode{
			{ID: "app1", Type: "app-server", Name: "App"},
			{ID: "c1", Type: "cache", Name: "Redis", Config: map[string]interface{}{"strategy": "cache-aside"}},
		},
	}
	cacheProxy := deployer.DeploymentDesign{
		Nodes: []deployer.DeploymentNode{
			{ID: "app1", Type: "app-server", Name: "App"},
			{ID: "c1", Type: "cache", Name: "Cache", Config: map[string]interface{}{"strategy": "write-through"}},
			{ID: "db1", Type: "db", Name: "DB"},
		},
	}

	tests := []struct {
		name      string
		design    deployer.DeploymentDesign
		target    string
		wantApp   string
		wantCont  string
		wantError string
	}{
		{
			name:     "app-server",
			design:   cacheAside,
			target:   "app-server",
			wantApp:  "app-server",
			wantCont: "app-server",
		},
		{
			name:     "cache-aside redis",
			design:   cacheAside,
			target:   "cache",
			wantApp:  "redis",
			wantCont: "redis",
		},
		{
			name:     "proxy redis-internal",
			design:   cacheProxy,
			target:   "cache",
			wantApp:  "redis-internal",
			wantCont: "redis",
		},
		{
			name:      "loadgen blocked",
			design:    cacheAside,
			target:    "loadgen",
			wantError: "cannot target loadgen",
		},
		{
			name:      "missing cache node",
			design:    deployer.DeploymentDesign{Nodes: []deployer.DeploymentNode{{Type: "app-server"}}},
			target:    "cache",
			wantError: "not found in deployment design",
		},
		{
			name:      "unknown component",
			design:    cacheAside,
			target:    "cdn",
			wantError: "unsupported target component",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveTarget(tt.target, tt.design)
			if tt.wantError != "" {
				if err == nil {
					t.Fatal("expected error")
				}
				if !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("error %q want substring %q", err.Error(), tt.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveTarget: %v", err)
			}
			if got.AppLabel != tt.wantApp || got.Container != tt.wantCont {
				t.Fatalf("got %+v want app=%s container=%s", got, tt.wantApp, tt.wantCont)
			}
		})
	}
}
