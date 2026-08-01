package deployer

import (
	"strings"
	"testing"
)

func TestNewTopologyRulesResponse_Completeness(t *testing.T) {
	t.Parallel()
	resp := NewTopologyRulesResponse()

	if len(resp.ComponentTypes) != 6 {
		t.Fatalf("componentTypes: want 6, got %d", len(resp.ComponentTypes))
	}
	if len(resp.Connections) != 36 {
		t.Fatalf("connections: want 36, got %d", len(resp.Connections))
	}

	seen := make(map[[2]string]ExportedConnectionRule, 36)
	for _, c := range resp.Connections {
		key := [2]string{c.Source, c.Target}
		if _, dup := seen[key]; dup {
			t.Fatalf("duplicate connection %s -> %s", c.Source, c.Target)
		}
		seen[key] = c
		rule, ok := connectionRules[key]
		if !ok {
			t.Fatalf("exported pair missing from connectionRules: %s -> %s", c.Source, c.Target)
		}
		if c.Valid != rule.Valid || c.Supported != rule.Supported || c.Explanation != rule.Explanation {
			t.Fatalf("mismatch for %s -> %s: export=%+v rule=%+v", c.Source, c.Target, c, rule)
		}
	}
	for key := range connectionRules {
		if _, ok := seen[key]; !ok {
			t.Fatalf("connectionRules pair not exported: %s -> %s", key[0], key[1])
		}
	}

	lbDB, ok := seen[[2]string{"lb", "db"}]
	if !ok || lbDB.Valid {
		t.Fatalf("lb -> db should be invalid, got %+v", lbDB)
	}
	lbApp, ok := seen[[2]string{"lb", "app-server"}]
	if !ok || !lbApp.Valid || !lbApp.Supported {
		t.Fatalf("lb -> app-server should be valid+supported, got %+v", lbApp)
	}

	if len(resp.Presence) != 3 {
		t.Fatalf("presence: want 3, got %d", len(resp.Presence))
	}
	if len(resp.Singletons) != len(platformSingletons) {
		t.Fatalf("singletons: want %d, got %d", len(platformSingletons), len(resp.Singletons))
	}
	byType := make(map[string]SingletonRule, len(resp.Singletons))
	for _, s := range resp.Singletons {
		byType[s.NodeType] = s
	}
	for _, want := range platformSingletons {
		got, ok := byType[want.NodeType]
		if !ok {
			t.Fatalf("missing singleton export for %s", want.NodeType)
		}
		if got.ScaleWithReplicas != want.ScaleWithReplicas || got.Message != want.Message {
			t.Fatalf("singleton %s: got %+v want scale=%v msg=%q", want.NodeType, got, want.ScaleWithReplicas, want.Message)
		}
	}
	if _, ok := byType["lb"]; ok {
		t.Fatal("lb must not be a platform singleton (chained-lb simulate path)")
	}
	if len(resp.RequiredOutbound) != 1 || resp.RequiredOutbound[0].NodeType != "worker" {
		t.Fatalf("requiredOutbound: %+v", resp.RequiredOutbound)
	}
	if got := singletonMessage("app-server", 2); got != formatTopologyMessage(msgSingletonAppServer, map[string]string{"count": "2"}) {
		t.Fatalf("singletonMessage: %q", got)
	}
	if got := singletonMessage("cache", 2); !strings.Contains(got, "cache") || !strings.Contains(got, "2") {
		t.Fatalf("singletonMessage cache: %q", got)
	}
	if got := workerMissingDBMessage("Fan-out"); got != `worker "Fan-out" must connect to a db node (add worker -> db edge)` {
		t.Fatalf("workerMissingDBMessage: %q", got)
	}
}
