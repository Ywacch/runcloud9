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
