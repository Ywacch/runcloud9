package api

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/introspect"
	"github.com/ywacch/cloud9/pkg/placement"
)

func introspectEngineForComponent(component string, topo *placement.Topology, spec introspect.ComponentSpec, design map[string]any) string {
	if eng := strings.TrimSpace(spec.Engine); eng != "" {
		return engineAlias(eng)
	}
	if topo != nil && len(topo.Nodes) > 0 {
		if node, ok := topo.NodeForComponent(component); ok {
			if eng := strings.TrimSpace(node.Engine); eng != "" {
				return engineAlias(eng)
			}
		}
		if j, ok := topo.JobByID("work_queue"); ok && j.Component == component && j.Engine != "" {
			return engineAlias(j.Engine)
		}
	}
	if parsed, err := deployer.DesignFromMap(design); err == nil && parsed != nil {
		if component == "mq" {
			if eng := workQueueEngineFromDesign(*parsed); eng != "" {
				return engineAlias(eng)
			}
			for _, n := range parsed.Nodes {
				if n.Type == "mq" {
					return "rabbitmq"
				}
			}
		}
		if component == "cache" {
			for _, n := range parsed.Nodes {
				if n.Type == "cache" {
					return engineAlias(deployer.CacheEngineFromNode(n))
				}
			}
		}
	}
	return engineAlias(spec.Engine)
}

func workQueueEngineFromDesign(d deployer.DeploymentDesign) string {
	if d.JobBindings == nil {
		return ""
	}
	nodeID, ok := d.JobBindings["work_queue"]
	if !ok {
		return ""
	}
	for _, n := range d.Nodes {
		if n.ID == nodeID {
			return deployer.NodeEngine(n)
		}
	}
	return ""
}

func engineAlias(eng string) string {
	switch strings.ToLower(strings.TrimSpace(eng)) {
	case "postgresql":
		return "postgres"
	default:
		return strings.ToLower(strings.TrimSpace(eng))
	}
}

func resolveIntrospectHostPlacement(topo *placement.Topology, component, namespace string) (host string, port int, ok bool) {
	if topo == nil {
		return "", 0, false
	}
	node, found := topo.NodeForComponent(component)
	if !found {
		return "", 0, false
	}
	member, found := topo.PrimaryMember(node.NodeID)
	if !found {
		return "", 0, false
	}
	svc := strings.TrimSpace(member.Service)
	if svc == "" {
		svc = strings.TrimSpace(member.AppLabel)
	}
	if svc == "" {
		return "", 0, false
	}
	host = fmt.Sprintf("%s.%s.svc.cluster.local", svc, namespace)
	port = memberListenPort(member.Port, node.Engine)
	return host, port, true
}

func memberListenPort(raw, engine string) int {
	if p := strings.TrimSpace(raw); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n > 0 {
			return n
		}
	}
	switch strings.ToLower(strings.TrimSpace(engine)) {
	case "redis":
		return 6379
	case "rabbitmq":
		return 5672
	case "memcached":
		return 11211
	case "postgresql", "postgres":
		return 5432
	default:
		return 0
	}
}
