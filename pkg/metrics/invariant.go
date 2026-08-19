package metrics

import (
	"fmt"
	"sync"
	"time"

	"github.com/ywacch/cloud9/pkg/templates"
)

// InvariantChecker emits at most one "invariant" event per rule while a condition
// stays true; it resets when the condition clears so a later crossing fires again.
type InvariantChecker struct {
	mu         sync.Mutex
	suppressed map[string]map[string]bool
}

// NewInvariantChecker creates a checker with empty state.
func NewInvariantChecker() *InvariantChecker {
	return &InvariantChecker{
		suppressed: make(map[string]map[string]bool),
	}
}

// Eval evaluates template-configured invariant rules against dm.TemplateMetrics.
func (ic *InvariantChecker) Eval(namespace string, dm *DeploymentMetrics, topo TopologyFlags, tmpl *templates.TemplateSpec) []Event {
	if ic == nil || dm == nil || namespace == "" || tmpl == nil || len(tmpl.InvariantCheckers) == 0 {
		return nil
	}
	var out []Event
	now := time.Now()
	for _, rule := range tmpl.InvariantCheckers {
		value := dm.TemplateMetrics[rule.Metric]
		if !invariantViolated(value, rule.Op, rule.Threshold) {
			ic.clear(namespace, rule.ID)
			continue
		}
		if !ic.fire(namespace, rule.ID) {
			continue
		}
		title := rule.Title
		if title == "" {
			title = "Invariant violated"
		}
		desc := rule.Description
		if desc == "" {
			desc = fmt.Sprintf("%s %s %v (value %.4f)", rule.Metric, rule.Op, rule.Threshold, value)
		}
		ev := Event{
			Timestamp:   now,
			Type:        "invariant",
			Source:      "detector",
			RuleID:      rule.ID,
			Title:       title,
			Description: desc,
			Narrative:   rule.Narrative,
			Suggestion:  rule.Suggestion,
			Detail: map[string]string{
				"kind":  rule.Kind,
				"value": fmt.Sprintf("%.4f", value),
			},
		}
		out = append(out, ev)
	}
	return out
}

// Cleanup removes invariant latch state for a namespace.
func (ic *InvariantChecker) Cleanup(namespace string) {
	if ic == nil || namespace == "" {
		return
	}
	ic.mu.Lock()
	defer ic.mu.Unlock()
	delete(ic.suppressed, namespace)
}

func invariantViolated(value float64, op string, threshold float64) bool {
	switch op {
	case ">":
		return value > threshold
	case ">=":
		return value >= threshold
	case "!=":
		return value != threshold
	default:
		return false
	}
}

func (ic *InvariantChecker) fire(namespace, rule string) bool {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	m := ic.suppressed[namespace]
	if m == nil {
		m = make(map[string]bool)
		ic.suppressed[namespace] = m
	}
	if m[rule] {
		return false
	}
	m[rule] = true
	return true
}

func (ic *InvariantChecker) clear(namespace, rule string) {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	m := ic.suppressed[namespace]
	if m == nil {
		return
	}
	delete(m, rule)
	if len(m) == 0 {
		delete(ic.suppressed, namespace)
	}
}
