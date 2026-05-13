package deployer

import (
	"fmt"
)

// StorytellingTopology summarizes canvas node presence and primary cache strategy
// for metrics narratives and UI. Derived only from the saved deployment design.
type StorytellingTopology struct {
	HasLB         bool
	HasMQ         bool
	HasWorker     bool
	HasCache      bool
	CacheStrategy string
	Template      string
}

// StorytellingTopologyFromDesign walks design nodes (load_balancer, message_queue, worker, cache).
// template is used when design.Template is empty.
func StorytellingTopologyFromDesign(d *DeploymentDesign, template string) StorytellingTopology {
	t := StorytellingTopology{Template: template}
	if d == nil {
		return t
	}
	if t.Template == "" && d.Template != "" {
		t.Template = d.Template
	}
	for _, n := range d.Nodes {
		switch n.Type {
		case "load_balancer":
			t.HasLB = true
		case "message_queue":
			t.HasMQ = true
		case "worker":
			t.HasWorker = true
		case "cache":
			t.HasCache = true
			if t.CacheStrategy == "" {
				t.CacheStrategy = configStringFromNodeConfig(n.Config, "strategy")
			}
			if t.CacheStrategy == "" {
				t.CacheStrategy = configStringFromNodeConfig(n.Config, "cacheStrategy")
			}
		}
	}
	return t
}

func configStringFromNodeConfig(cfg map[string]interface{}, key string) string {
	if cfg == nil {
		return ""
	}
	v, ok := cfg[key]
	if !ok {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	case int:
		return fmt.Sprintf("%d", x)
	case int64:
		return fmt.Sprintf("%d", x)
	case float64:
		if x == float64(int64(x)) {
			return fmt.Sprintf("%.0f", x)
		}
		return fmt.Sprintf("%g", x)
	default:
		return fmt.Sprintf("%v", x)
	}
}
