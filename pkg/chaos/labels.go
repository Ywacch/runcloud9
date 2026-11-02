package chaos

import (
	"fmt"

	"github.com/ywacch/cloud9/pkg/deployer"
)

// ResolvedTarget maps a canvas component name to K8s pod selection labels.
type ResolvedTarget struct {
	AppLabel  string // K8s app= label
	Container string // container name for exec (cache flush)
}

// ResolveTarget maps canvas targetComponent to K8s labels and validates the design.
func ResolveTarget(targetComponent string, design deployer.DeploymentDesign) (ResolvedTarget, error) {
	return resolveTarget(targetComponent, design)
}

// resolveTarget maps canvas targetComponent to K8s labels and validates the design.
func resolveTarget(targetComponent string, design deployer.DeploymentDesign) (ResolvedTarget, error) {
	if targetComponent == "" {
		return ResolvedTarget{}, fmt.Errorf("targetComponent is required")
	}
	if targetComponent == "loadgen" {
		return ResolvedTarget{}, fmt.Errorf("cannot target loadgen")
	}

	nodeType, ok := canvasTypeForComponent(targetComponent)
	if !ok {
		return ResolvedTarget{}, fmt.Errorf("unsupported target component %q", targetComponent)
	}

	var matched *deployer.DeploymentNode
	for i := range design.Nodes {
		n := &design.Nodes[i]
		if n.Type == nodeType {
			matched = n
			break
		}
	}
	if matched == nil {
		return ResolvedTarget{}, fmt.Errorf("target component %q not found in deployment design", targetComponent)
	}

	switch targetComponent {
	case "cache":
		if deployer.RequiresProxy(*matched) {
			return ResolvedTarget{AppLabel: "redis-internal", Container: "redis"}, nil
		}
		switch deployer.CacheEngineFromNode(*matched) {
		case "memcached":
			return ResolvedTarget{AppLabel: "memcached", Container: "memcached"}, nil
		default:
			return ResolvedTarget{AppLabel: "redis", Container: "redis"}, nil
		}
	case "app-server":
		return ResolvedTarget{AppLabel: "app-server", Container: "app-server"}, nil
	case "db":
		return ResolvedTarget{AppLabel: "postgres", Container: "postgres"}, nil
	case "mq":
		return ResolvedTarget{AppLabel: "rabbitmq", Container: "rabbitmq"}, nil
	case "worker":
		return ResolvedTarget{AppLabel: "worker", Container: "worker"}, nil
	case "lb":
		return ResolvedTarget{AppLabel: "lb", Container: "lb"}, nil
	default:
		return ResolvedTarget{}, fmt.Errorf("unsupported target component %q", targetComponent)
	}
}

func canvasTypeForComponent(targetComponent string) (nodeType string, ok bool) {
	switch targetComponent {
	case "app-server":
		return "app-server", true
	case "cache":
		return "cache", true
	case "db":
		return "db", true
	case "mq":
		return "mq", true
	case "worker":
		return "worker", true
	case "lb":
		return "lb", true
	default:
		return "", false
	}
}
