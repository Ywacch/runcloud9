package deployer

import (
	"testing"
)

func TestStorytellingTopologyFromDesign(t *testing.T) {
	t.Parallel()
	d := &DeploymentDesign{
		Template: "social-feed",
		Nodes: []DeploymentNode{
			{Type: "load_balancer"},
			{Type: "cache", Config: map[string]interface{}{"strategy": "timeline-store"}},
			{Type: "message_queue"},
		},
	}
	got := StorytellingTopologyFromDesign(d, "")
	if !got.HasLB || !got.HasCache || !got.HasMQ {
		t.Fatalf("flags: %+v", got)
	}
	if got.CacheStrategy != "timeline-store" {
		t.Fatalf("CacheStrategy: want timeline-store, got %q", got.CacheStrategy)
	}
	if got.Template != "social-feed" {
		t.Fatalf("Template: %q", got.Template)
	}
	if StorytellingTopologyFromDesign(nil, "x").Template != "x" {
		t.Fatal("nil design should preserve template arg")
	}
}

func TestStorytellingTopologyFromDesign_Storage(t *testing.T) {
	t.Parallel()
	d := &DeploymentDesign{
		Nodes: []DeploymentNode{
			{Type: "db", Config: map[string]interface{}{"engine": "postgresql", "replicationMode": "read-replica"}},
			{Type: "cache", Config: map[string]interface{}{"strategy": "write-through"}},
		},
	}
	got := StorytellingTopologyFromDesign(d, "")
	if !got.HasDB || got.DBEngine != "postgresql" || got.DBRole != "replica" {
		t.Fatalf("db: %+v", got)
	}
	if got.CacheEngine != "redis" || got.CacheRole != "write-through" {
		t.Fatalf("cache: engine=%q role=%q", got.CacheEngine, got.CacheRole)
	}
}

func TestStorytellingTopologyFromDesign_RedisOnlyPrimary(t *testing.T) {
	t.Parallel()
	d := &DeploymentDesign{
		Nodes: []DeploymentNode{{Type: "cache"}},
	}
	got := StorytellingTopologyFromDesign(d, "")
	if got.HasDB || got.CacheRole != "primary" {
		t.Fatalf("want redis primary: %+v", got)
	}
}
