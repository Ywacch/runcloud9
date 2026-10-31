package deployer

import (
	"sort"
	"strings"

	"github.com/ywacch/cloud9/pkg/templates"
)

// Binding is one resolved job-to-node binding for a deployment design.
type Binding struct {
	Job      string
	NodeID   string
	NodeType string
	Engine   string // resolved engine id of the node
	Explicit bool   // true when the design named the node in jobBindings
}

// NodeEngine returns the engine id a node runs.
// db uses dbEngineFromConfig; cache and mq use config "engine" with redis/rabbitmq defaults.
func NodeEngine(node DeploymentNode) string {
	switch node.Type {
	case "db":
		return dbEngineFromConfig(node.Config)
	case "cache":
		eng := configString(node.Config, "engine", "")
		if eng == "" {
			eng = configString(node.Config, "cacheEngine", "")
		}
		if eng == "" {
			return "redis"
		}
		return strings.ToLower(strings.TrimSpace(eng))
	case "mq":
		return strings.ToLower(strings.TrimSpace(configString(node.Config, "engine", "rabbitmq")))
	default:
		return ""
	}
}

// ResolveJobBindings returns one Binding per template job that can be bound.
// Unbound optional jobs are omitted. Unbound required jobs are omitted too; validation reports them later.
func ResolveJobBindings(design DeploymentDesign, tmpl *templates.TemplateSpec) []Binding {
	if tmpl == nil || len(tmpl.Jobs) == 0 {
		return nil
	}
	out := make([]Binding, 0, len(tmpl.Jobs))
	for _, job := range tmpl.Jobs {
		if nodeID, ok := design.JobBindings[job.ID]; ok && strings.TrimSpace(nodeID) != "" {
			nodeID = strings.TrimSpace(nodeID)
			node := nodeByID(design, nodeID)
			if node == nil {
				out = append(out, Binding{
					Job:      job.ID,
					NodeID:   nodeID,
					Explicit: true,
				})
				continue
			}
			out = append(out, Binding{
				Job:      job.ID,
				NodeID:   node.ID,
				NodeType: node.Type,
				Engine:   NodeEngine(*node),
				Explicit: true,
			})
			continue
		}
		node := defaultNodeForJob(job, design)
		if node == nil {
			continue
		}
		out = append(out, Binding{
			Job:      job.ID,
			NodeID:   node.ID,
			NodeType: node.Type,
			Engine:   NodeEngine(*node),
			Explicit: false,
		})
	}
	return out
}

func defaultNodeForJob(job templates.JobRequirement, design DeploymentDesign) *DeploymentNode {
	nodes := nodesSortedByID(design.Nodes)
	if job.ID == "timeline" {
		for i := range nodes {
			n := &nodes[i]
			if n.Type == "cache" && cacheNodeStrategy(*n) == "timeline-store" {
				return n
			}
		}
		if job.DefaultNodeType == "cache" {
			for i := range nodes {
				n := &nodes[i]
				if n.Type == "cache" {
					return n
				}
			}
		}
		return nil
	}
	wantType := strings.TrimSpace(job.DefaultNodeType)
	if wantType == "" {
		return nil
	}
	for i := range nodes {
		n := &nodes[i]
		if n.Type == wantType {
			return n
		}
	}
	return nil
}

func nodesSortedByID(nodes []DeploymentNode) []DeploymentNode {
	out := append([]DeploymentNode(nil), nodes...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})
	return out
}
