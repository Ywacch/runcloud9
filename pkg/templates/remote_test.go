package templates

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCloudTemplateRow_toTemplateDef_StorytellingAndWorker(t *testing.T) {
	t.Parallel()

	story := StorytellingSpec{
		Edges: map[string]RoleStorySpec{
			"app-server->cache": {Base: "Remote premium cache edge copy"},
		},
		Roles: map[string]RoleStorySpec{
			"cache": {Base: "Remote cache role"},
		},
	}
	storyRaw, err := json.Marshal(story)
	if err != nil {
		t.Fatal(err)
	}

	row := cloudTemplateRow{
		ID:             "premium-feed",
		Name:           "Premium Feed",
		Description:    "remote",
		Components:     4,
		Bottleneck:     "cache",
		Available:      true,
		Premium:        true,
		Tags:           []string{"premium"},
		Simulatable:    true,
		Deployable:     true,
		ImageKey:       "social-feed",
		WorkerImageKey: "fanout-worker",
		Storytelling:   storyRaw,
	}

	def, err := row.toTemplateDef()
	if err != nil {
		t.Fatalf("toTemplateDef: %v", err)
	}
	if def.WorkerImageKey != "fanout-worker" {
		t.Fatalf("WorkerImageKey: %q", def.WorkerImageKey)
	}
	if def.Storytelling == nil || def.Storytelling.Edges["app-server->cache"].Base == "" {
		t.Fatalf("storytelling edges missing: %+v", def.Storytelling)
	}

	spec := defToSpec(def)
	if !spec.SupportsWorker {
		t.Fatal("SupportsWorker should be true when worker_image_key is set")
	}
	if spec.Storytelling == nil || spec.Storytelling.Edges["app-server->cache"].Base != "Remote premium cache edge copy" {
		t.Fatalf("spec storytelling: %+v", spec.Storytelling)
	}
}

func TestCloudTemplateRow_toTemplateDef_NullStorytelling(t *testing.T) {
	t.Parallel()
	row := cloudTemplateRow{
		ID:           "bare",
		Name:         "Bare",
		Available:    true,
		Premium:      true,
		ImageKey:     "url-shortener",
		Storytelling: json.RawMessage("null"),
	}
	def, err := row.toTemplateDef()
	if err != nil {
		t.Fatal(err)
	}
	if def.Storytelling != nil {
		t.Fatalf("expected nil storytelling, got %+v", def.Storytelling)
	}
	spec := defToSpec(def)
	if spec.SupportsWorker {
		t.Fatal("SupportsWorker should be false without worker_image_key")
	}
}

func TestCloudTemplatesSelectIncludesStorytelling(t *testing.T) {
	t.Parallel()
	if !strings.Contains(cloudTemplatesSelect, "storytelling") {
		t.Fatalf("select missing storytelling: %q", cloudTemplatesSelect)
	}
	if !strings.Contains(cloudTemplatesSelect, "worker_image_key") {
		t.Fatalf("select missing worker_image_key: %q", cloudTemplatesSelect)
	}
}
