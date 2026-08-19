package templates

import (
	"fmt"
	"strings"
)

// InvariantRule defines a YAML-driven L1 check against a templateMetrics key.
type InvariantRule struct {
	ID          string  `yaml:"id" json:"id"`
	Kind        string  `yaml:"kind" json:"kind"`
	Metric      string  `yaml:"metric" json:"metric"`
	Op          string  `yaml:"op" json:"op"`
	Threshold   float64 `yaml:"threshold" json:"threshold"`
	Title       string  `yaml:"title" json:"title"`
	Description string  `yaml:"description" json:"description"`
	Narrative   string  `yaml:"narrative" json:"narrative"`
	Suggestion  string  `yaml:"suggestion" json:"suggestion"`
}

var allowedInvariantOps = map[string]struct{}{
	">":  {},
	">=": {},
	"!=": {},
}

// validateInvariantRules returns rules unchanged when all entries are valid.
func validateInvariantRules(rules []InvariantRule) ([]InvariantRule, error) {
	if len(rules) == 0 {
		return nil, nil
	}
	out := make([]InvariantRule, 0, len(rules))
	seen := make(map[string]struct{}, len(rules))
	for _, rule := range rules {
		id := strings.TrimSpace(rule.ID)
		metric := strings.TrimSpace(rule.Metric)
		op := strings.TrimSpace(rule.Op)
		if id == "" {
			return nil, fmt.Errorf("invariant rule missing id")
		}
		if metric == "" {
			return nil, fmt.Errorf("invariant %q: metric is required", id)
		}
		if op == "" {
			return nil, fmt.Errorf("invariant %q: op is required", id)
		}
		if _, ok := allowedInvariantOps[op]; !ok {
			return nil, fmt.Errorf("invariant %q: unsupported op %q", id, op)
		}
		if _, dup := seen[id]; dup {
			return nil, fmt.Errorf("invariant %q: duplicate id", id)
		}
		seen[id] = struct{}{}
		out = append(out, InvariantRule{
			ID:          id,
			Kind:        strings.TrimSpace(rule.Kind),
			Metric:      metric,
			Op:          op,
			Threshold:   rule.Threshold,
			Title:       strings.TrimSpace(rule.Title),
			Description: strings.TrimSpace(rule.Description),
			Narrative:   strings.TrimSpace(rule.Narrative),
			Suggestion:  strings.TrimSpace(rule.Suggestion),
		})
	}
	return out, nil
}
