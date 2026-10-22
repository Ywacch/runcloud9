package vu

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestValidateConfig(t *testing.T) {
	reg := DefaultRegistry()
	actions := map[string]Action{
		actionCreateRoot:  {Method: "POST", Path: "/post", Body: "{}", IDField: "post_id"},
		actionCreateReply: {Method: "POST", Path: "/post", Body: "{}", IDField: "post_id"},
	}
	t.Run("missing action", func(t *testing.T) {
		err := ValidateConfig(reg, Config{Behaviors: []BehaviorConfig{
			{Name: behaviorReplier, Count: 1},
		}}, map[string]Action{})
		var ma *MissingActionError
		if !errors.As(err, &ma) || ma.Behavior != behaviorReplier {
			t.Fatalf("err: %v", err)
		}
	})
	t.Run("unknown behavior", func(t *testing.T) {
		err := ValidateConfig(reg, Config{Behaviors: []BehaviorConfig{
			{Name: "nope", Count: 1},
		}}, actions)
		if err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("51 vus", func(t *testing.T) {
		err := ValidateConfig(reg, Config{Behaviors: []BehaviorConfig{
			{Name: behaviorConversationStarter, Count: 51},
		}}, actions)
		if err == nil {
			t.Fatal("expected cap error")
		}
	})
	t.Run("bad params field", func(t *testing.T) {
		raw, _ := json.Marshal(map[string]int{"replyWithinMs": 300, "extra": 1})
		err := ValidateConfig(reg, Config{Behaviors: []BehaviorConfig{
			{Name: behaviorReplier, Count: 1, Params: raw},
		}}, actions)
		if err == nil {
			t.Fatal("expected unknown field error")
		}
	})
	t.Run("two observers", func(t *testing.T) {
		err := ValidateConfig(reg, Config{Behaviors: []BehaviorConfig{
			{Name: behaviorThreadObserver, Count: 2},
		}}, map[string]Action{
			actionReadThread: {Method: "GET", Path: "/threads/{{.RootID}}"},
		})
		if err == nil {
			t.Fatal("expected cap error")
		}
	})
	t.Run("bad placeholder", func(t *testing.T) {
		bad := map[string]Action{
			actionCreateRoot: {Method: "POST", Path: "/{{.Unknown}}", Body: ""},
		}
		err := ValidateConfig(reg, Config{Behaviors: []BehaviorConfig{
			{Name: behaviorConversationStarter, Count: 1},
		}}, bad)
		if err == nil {
			t.Fatal("expected placeholder error")
		}
	})
}
