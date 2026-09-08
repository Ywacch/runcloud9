package store

import "testing"

func TestCompactTopologyFromDesign(t *testing.T) {
	t.Parallel()

	topology := CompactTopologyFromDesign(map[string]any{
		"nodes": []any{
			map[string]any{
				"id":       "app-1",
				"type":     "app-server",
				"position": []any{4.0, 0.3, -2.0},
			},
			map[string]any{
				"id":   "db-1",
				"type": "database",
			},
		},
		"connections": []any{
			map[string]any{
				"sourceId": "app-1",
				"targetId": "db-1",
				"protocol": "http",
			},
		},
	})
	if topology == nil {
		t.Fatal("expected topology")
	}
	if len(topology.Nodes) != 2 {
		t.Fatalf("nodes = %d, want 2", len(topology.Nodes))
	}
	if len(topology.Nodes[0].Position) != 3 || topology.Nodes[0].Position[0] != 4 {
		t.Fatalf("position = %#v", topology.Nodes[0].Position)
	}
	if len(topology.Nodes[1].Position) != 0 {
		t.Fatalf("missing position should omit field, got %#v", topology.Nodes[1].Position)
	}
	if len(topology.Connections) != 1 || topology.Connections[0].SourceID != "app-1" {
		t.Fatalf("connections = %#v", topology.Connections)
	}
}

func TestCompactTopologyFromDesign_empty(t *testing.T) {
	t.Parallel()
	if CompactTopologyFromDesign(nil) != nil {
		t.Fatal("expected nil for nil design")
	}
	if CompactTopologyFromDesign(map[string]any{"nodes": []any{}}) != nil {
		t.Fatal("expected nil for empty nodes")
	}
}
