package templates

import (
	"testing"

	"github.com/ywacch/cloud9/pkg/loadgen/vu"
)

func TestTemplateVUActionsValid(t *testing.T) {
	for _, id := range []string{"social-feed", "social-feed-pull", "url-shortener", "ecommerce"} {
		spec, ok := LoadgenVU(id)
		if !ok {
			continue
		}
		for name, a := range spec.Actions {
			act := vu.Action{Method: a.Method, Path: a.Path, Body: a.Body, IDField: a.IDField}
			if err := act.Validate(); err != nil {
				t.Fatalf("template %q action %q: %v", id, name, err)
			}
		}
	}
	actions, ok := LoadgenVU("social-feed")
	if !ok {
		t.Fatal("social-feed vu missing")
	}
	reg := vu.DefaultRegistry()
	cfg := vu.Config{Behaviors: []vu.BehaviorConfig{
		{Name: "conversation_starter", Count: 1},
		{Name: "replier", Count: 1},
	}}
	converted := make(map[string]vu.Action, len(actions.Actions))
	for k, a := range actions.Actions {
		converted[k] = vu.Action{Method: a.Method, Path: a.Path, Body: a.Body, IDField: a.IDField}
	}
	if err := vu.ValidateConfig(reg, cfg, converted); err != nil {
		t.Fatalf("social-feed behaviors: %v", err)
	}
}
