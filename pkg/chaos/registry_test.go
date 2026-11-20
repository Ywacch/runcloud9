package chaos

import (
	"context"
	"strings"
	"testing"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/engines"
	"github.com/ywacch/cloud9/pkg/rules"
	"k8s.io/client-go/kubernetes/fake"
)

func TestResolve(t *testing.T) {
	client := fake.NewClientset()
	designWithCache := deployer.DeploymentDesign{
		Template: "url-shortener",
		Nodes: []deployer.DeploymentNode{
			{Type: "app-server", Name: "App"},
			{Type: "cache", Name: "Cache"},
			{Type: "db", Name: "DB", Config: map[string]any{"engine": "postgresql"}},
		},
	}

	tests := []struct {
		name      string
		scenario  string
		target    string
		design    deployer.DeploymentDesign
		wantErr   bool
		errSubstr string
	}{
		{
			name:     "pod_kill app-server",
			scenario: "pod_kill",
			target:   "app-server",
			design:   designWithCache,
		},
		{
			name:     "pod_kill db postgresql",
			scenario: "pod_kill",
			target:   "db",
			design:   designWithCache,
		},
		{
			name:     "cache_flush redis",
			scenario: "cache_flush",
			target:   "cache",
			design:   designWithCache,
		},
		{
			name:     "stall_replica postgresql db",
			scenario: scenarioStallReplica,
			target:   "db",
			design:   designWithCache,
		},
		{
			name:     "split_primary_replica postgresql db",
			scenario: scenarioSplitPrimaryReplica,
			target:   "db",
			design:   designWithCache,
		},
		{
			name:     "kill_replica postgresql db",
			scenario: ScenarioKillReplica,
			target:   "db",
			design: deployer.DeploymentDesign{
				Template: "url-shortener",
				Nodes: []deployer.DeploymentNode{
					{Type: "app-server", Name: "App"},
					{Type: "db", Name: "DB", Config: map[string]any{
						"engine":       "postgresql",
						"readReplicas": 1,
					}},
				},
			},
		},
		{
			name:     "split_app_store redis cache",
			scenario: scenarioSplitAppStore,
			target:   "cache",
			design:   designWithCache,
		},
		{
			name:      "unknown scenario",
			scenario:  "network_blackhole",
			target:    "db",
			design:    designWithCache,
			wantErr:   true,
			errSubstr: "unknown scenario",
		},
		{
			name:      "unsupported target for cache_flush",
			scenario:  "cache_flush",
			target:    "app-server",
			design:    designWithCache,
			wantErr:   true,
			errSubstr: "supported:",
		},
		{
			name:     "pod_kill cache engine-agnostic",
			scenario: "pod_kill",
			target:   "cache",
			design:   designWithCache,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := Resolve(tc.scenario, tc.target, tc.design, client)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				if tc.errSubstr != "" && !strings.Contains(err.Error(), tc.errSubstr) {
					t.Fatalf("error %q should contain %q", err.Error(), tc.errSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			if s == nil {
				t.Fatal("nil scenario")
			}
		})
	}
}

func TestResolve_CacheFlushMemcached(t *testing.T) {
	design := deployer.DeploymentDesign{
		Template: "url-shortener",
		Nodes: []deployer.DeploymentNode{
			{Type: "cache", Name: "Memcached", Config: map[string]any{"engine": "memcached"}},
		},
	}
	s, err := Resolve("cache_flush", "cache", design, fake.NewClientset())
	if err != nil {
		t.Fatalf("Resolve cache_flush memcached: %v", err)
	}
	if s == nil {
		t.Fatal("nil scenario")
	}
}

func TestResolve_SplitAppStoreMemcached(t *testing.T) {
	design := deployer.DeploymentDesign{
		Template: "url-shortener",
		Nodes: []deployer.DeploymentNode{
			{Type: "app-server", Name: "App"},
			{Type: "cache", Name: "Memcached", Config: map[string]any{"engine": "memcached"}},
		},
	}
	s, err := Resolve(scenarioSplitAppStore, "cache", design, fake.NewClientset())
	if err != nil {
		t.Fatalf("Resolve split_app_store memcached: %v", err)
	}
	if s == nil || !s.Stateful() {
		t.Fatal("split_app_store memcached should be stateful")
	}
}

func TestResolve_requiresClient(t *testing.T) {
	design := deployer.DeploymentDesign{
		Nodes: []deployer.DeploymentNode{{Type: "app-server", Name: "App"}},
	}
	_, err := Resolve("pod_kill", "app-server", design, nil)
	if err == nil || !strings.Contains(err.Error(), "kubernetes client is required") {
		t.Fatalf("expected client error, got %v", err)
	}
}

func TestResolve_supportedCombinationsInError(t *testing.T) {
	design := deployer.DeploymentDesign{
		Nodes: []deployer.DeploymentNode{{Type: "app-server", Name: "App"}},
	}
	_, err := Resolve("cache_flush", "app-server", design, fake.NewClientset())
	if err == nil {
		t.Fatal("expected error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "cache_flush/redis/cache") {
		t.Fatalf("expected supported combination in error, got %q", msg)
	}
}

func TestOneShotScenarios_revertNoOp(t *testing.T) {
	client := fake.NewClientset()
	design := deployer.DeploymentDesign{
		Nodes: []deployer.DeploymentNode{
			{Type: "app-server", Name: "App"},
			{Type: "cache", Name: "Cache"},
		},
	}

	podKill, err := Resolve("pod_kill", "app-server", design, client)
	if err != nil {
		t.Fatalf("Resolve pod_kill: %v", err)
	}
	if podKill.Stateful() {
		t.Fatal("pod_kill should not be stateful")
	}
	if err := podKill.Revert(context.Background(), "ns", design, "app-server"); err != nil {
		t.Fatalf("Revert pod_kill: %v", err)
	}

	cacheFlush, err := Resolve("cache_flush", "cache", design, client)
	if err != nil {
		t.Fatalf("Resolve cache_flush: %v", err)
	}
	if cacheFlush.Stateful() {
		t.Fatal("cache_flush should not be stateful")
	}
	if err := cacheFlush.Revert(context.Background(), "ns", design, "cache"); err != nil {
		t.Fatalf("Revert cache_flush: %v", err)
	}

	killReplica, err := Resolve(ScenarioKillReplica, "db", deployer.DeploymentDesign{
		Nodes: []deployer.DeploymentNode{
			{Type: "db", Config: map[string]any{"engine": "postgresql", "readReplicas": 1}},
		},
	}, client)
	if err != nil {
		t.Fatalf("Resolve kill_replica: %v", err)
	}
	if killReplica.Stateful() {
		t.Fatal("kill_replica should not be stateful")
	}
	if err := killReplica.Revert(context.Background(), "ns", design, "db"); err != nil {
		t.Fatalf("Revert kill_replica: %v", err)
	}
}

func TestCatalog_PairChaosExists(t *testing.T) {
	t.Parallel()
	reg := map[string]struct{}{}
	for _, r := range scenarioRegistry {
		reg[r.scenario] = struct{}{}
	}
	for _, p := range engines.Default().Pairs {
		for _, id := range p.Chaos {
			if _, ok := reg[id]; !ok {
				t.Fatalf("pair %s/%s chaos %q not in registry", p.Job, p.Engine, id)
			}
		}
	}
}

func TestScenarioFidelity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		scenario string
		want     string
	}{
		{scenario: "pod_kill", want: rules.FidelityReal},
		{scenario: "cache_flush", want: rules.FidelityReal},
		{scenario: scenarioSplitAppStore, want: rules.FidelityCompressed},
		{scenario: "not-a-scenario", want: rules.FidelityReal},
	}
	for _, tc := range tests {
		t.Run(tc.scenario, func(t *testing.T) {
			t.Parallel()
			if got := ScenarioFidelity(tc.scenario); got != tc.want {
				t.Fatalf("ScenarioFidelity(%q) = %q want %q", tc.scenario, got, tc.want)
			}
		})
	}
}
