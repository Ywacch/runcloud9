package chaos

import (
	"fmt"
	"strings"

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

func resolveTarget(targetComponent string, design deployer.DeploymentDesign) (ResolvedTarget, error) {
	if targetComponent == "" {
		return ResolvedTarget{}, fmt.Errorf("targetComponent is required")
	}
	if targetComponent == "loadgen" {
		return ResolvedTarget{}, fmt.Errorf("cannot target loadgen")
	}

	if design.ResolvedPlacement != nil && len(design.ResolvedPlacement.Nodes) > 0 {
		return resolveTargetFromPlacement(targetComponent, design)
	}
	return resolveTargetLegacy(targetComponent, design)
}

func resolveTargetFromPlacement(targetComponent string, design deployer.DeploymentDesign) (ResolvedTarget, error) {
	topo := design.ResolvedPlacement
	nodeID := strings.TrimSpace(design.ChaosTargetNodeID)
	if nodeID == "" {
		n, ok := topo.NodeForComponent(targetComponent)
		if !ok {
			return ResolvedTarget{}, fmt.Errorf("target component %q not found in placement", targetComponent)
		}
		nodeID = n.NodeID
	}
	member, ok := topo.PrimaryMember(nodeID)
	if !ok {
		return ResolvedTarget{}, fmt.Errorf("target component %q has no members on node %q", targetComponent, nodeID)
	}
	container := member.Container
	if container == "" {
		container = member.AppLabel
	}
	if member.AppLabel == "" {
		return ResolvedTarget{}, fmt.Errorf("target component %q missing app label on node %q", targetComponent, nodeID)
	}
	return ResolvedTarget{AppLabel: member.AppLabel, Container: container}, nil
}

// resolveTargetLegacy picks the first node of a component type when placement is absent.
func resolveTargetLegacy(targetComponent string, design deployer.DeploymentDesign) (ResolvedTarget, error) {
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
