package vu

import (
	"fmt"
)

// MissingActionError means the template did not declare an action a behavior needs.
type MissingActionError struct {
	Behavior string
	Action   string
}

func (e *MissingActionError) Error() string {
	return fmt.Sprintf("vu: behavior %q requires action %q", e.Behavior, e.Action)
}

// DefaultRegistry returns the built-in behaviors.
func DefaultRegistry() Registry {
	return Registry{
		behaviorConversationStarter: BehaviorSpec{
			New:      newConversationStarter,
			Requires: []string{actionCreateRoot},
		},
		behaviorReplier: BehaviorSpec{
			New:      newReplier,
			Requires: []string{actionCreateReply},
		},
	}
}

// ValidateConfig checks cfg against reg and the template's declared actions.
func ValidateConfig(reg Registry, cfg Config, actions map[string]Action) error {
	if len(cfg.Behaviors) == 0 {
		return fmt.Errorf("vu: at least one behavior required")
	}
	seen := make(map[string]struct{})
	total := 0
	for _, b := range cfg.Behaviors {
		if b.Count < 1 {
			return fmt.Errorf("vu: behavior %q count must be >= 1", b.Name)
		}
		if _, dup := seen[b.Name]; dup {
			return fmt.Errorf("vu: duplicate behavior %q", b.Name)
		}
		seen[b.Name] = struct{}{}
		total += b.Count
		spec, ok := reg[b.Name]
		if !ok {
			return fmt.Errorf("vu: unknown behavior %q", b.Name)
		}
		if _, err := spec.New(b.Params); err != nil {
			return err
		}
		for _, req := range spec.Requires {
			if _, ok := actions[req]; !ok {
				return &MissingActionError{Behavior: b.Name, Action: req}
			}
		}
	}
	if total > MaxVUs {
		return fmt.Errorf("vu: total vus %d exceeds max %d", total, MaxVUs)
	}
	for name, a := range actions {
		if err := a.Validate(); err != nil {
			return fmt.Errorf("vu: action %q: %w", name, err)
		}
	}
	return nil
}
