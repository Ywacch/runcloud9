package deployer

import (
	"encoding/json"
	"testing"
)

func TestDesignFromMap(t *testing.T) {
	t.Parallel()
	m := map[string]any{
		"version":  "1.0",
		"template": "social-feed",
		"nodes": []any{
			map[string]any{"id": "lb-1", "type": "load_balancer", "name": "LB", "config": map[string]any{}},
			map[string]any{"id": "cache-1", "type": "cache", "name": "Redis", "config": map[string]any{"strategy": "write-through"}},
		},
		"connections": []any{},
	}
	d, err := DesignFromMap(m)
	if err != nil {
		t.Fatal(err)
	}
	if d == nil {
		t.Fatal("expected design")
	}
	if d.Template != "social-feed" || len(d.Nodes) != 2 {
		t.Fatalf("unexpected design: %+v", d)
	}
	if d.Nodes[1].Config["strategy"] != "write-through" {
		t.Fatalf("config: %+v", d.Nodes[1].Config)
	}
	if _, err := DesignFromMap(nil); err != nil {
		t.Fatal(err)
	}
}

func TestDesignFromMap_InvalidJSON(t *testing.T) {
	t.Parallel()
	m := map[string]any{
		"version":  "1.0",
		"template": "x",
		"nodes":    "not-an-array",
	}
	_, err := DesignFromMap(m)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDesignFromMap_RoundTrip(t *testing.T) {
	t.Parallel()
	d := DeploymentDesign{
		Version:  "1",
		Template: "url-shortener",
		Nodes: []DeploymentNode{
			{ID: "n1", Type: "cache", Name: "c", Config: map[string]interface{}{"strategy": "cache-aside"}},
		},
	}
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	got, err := DesignFromMap(m)
	if err != nil {
		t.Fatal(err)
	}
	if got.Template != d.Template || len(got.Nodes) != 1 {
		t.Fatalf("got %+v", got)
	}
}
