package deployer

import (
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
	if len(resp.Singletons) != 2 {
		t.Fatalf("singletons: want 2, got %d", len(resp.Singletons))
	}
	if len(resp.RequiredOutbound) != 1 || resp.RequiredOutbound[0].NodeType != "worker" {
		t.Fatalf("requiredOutbound: %+v", resp.RequiredOutbound)
	}
	if got := singletonMessage("app-server", 2); got != formatTopologyMessage(msgSingletonAppServer, map[string]string{"count": "2"}) {
		t.Fatalf("singletonMessage: %q", got)
	}
	if got := workerMissingDBMessage("Fan-out"); got != `worker "Fan-out" must connect to a db node (add worker -> db edge)` {
		t.Fatalf("workerMissingDBMessage: %q", got)
	}
}
