package vu_test

import (
	"testing"

	"github.com/ywacch/cloud9/pkg/loadgen/vu"
	"github.com/ywacch/cloud9/pkg/templates"
)

func TestDefaultConversations_SocialFeedShape(t *testing.T) {
	spec, ok := templates.LoadgenVU("social-feed")
	if !ok {
		t.Fatal("social-feed vu missing")
	}
	names := actionNameSet(spec.Actions)
	cfg, has := vu.DefaultConversations(names)
	if !has {
		t.Fatal("expected conversations block")
	}
	if len(cfg.Behaviors) != 3 {
		t.Fatalf("behaviors: %+v", cfg.Behaviors)
	}
	want := []vu.BehaviorConfig{
		{Name: "conversation_starter", Count: 2},
		{Name: "replier", Count: 2},
		{Name: "thread_observer", Count: 1},
	}
	for i, w := range want {
		if cfg.Behaviors[i].Name != w.Name || cfg.Behaviors[i].Count != w.Count {
			t.Fatalf("behavior[%d]: got %+v want %+v", i, cfg.Behaviors[i], w)
		}
		if len(cfg.Behaviors[i].Params) != 0 {
			t.Fatalf("behavior[%d] params should be empty", i)
		}
	}
	converted := make(map[string]vu.Action, len(spec.Actions))
	for k, a := range spec.Actions {
		converted[k] = vu.Action{Method: a.Method, Path: a.Path, Body: a.Body, IDField: a.IDField}
	}
	if err := vu.ValidateConfig(vu.DefaultRegistry(), cfg, converted); err != nil {
		t.Fatalf("ValidateConfig: %v", err)
	}
}

func TestDefaultConversations_OmitsObserverWithoutReadThread(t *testing.T) {
	names := map[string]struct{}{
		"create_root":  {},
		"create_reply": {},
	}
	cfg, ok := vu.DefaultConversations(names)
	if !ok {
		t.Fatal("expected ok")
	}
	if len(cfg.Behaviors) != 2 {
		t.Fatalf("behaviors: %+v", cfg.Behaviors)
	}
	for _, b := range cfg.Behaviors {
		if b.Name == "thread_observer" {
			t.Fatal("unexpected observer")
		}
	}
}

func TestDefaultConversations_FalseWithoutReply(t *testing.T) {
	_, ok := vu.DefaultConversations(map[string]struct{}{"create_root": {}})
	if ok {
		t.Fatal("expected false without create_reply")
	}
}

func actionNameSet(actions map[string]templates.LoadgenVUAction) map[string]struct{} {
	out := make(map[string]struct{}, len(actions))
	for k := range actions {
		out[k] = struct{}{}
	}
	return out
}
