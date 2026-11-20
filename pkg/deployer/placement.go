package deployer

import (
	"fmt"
	"sort"
	"strings"

	"github.com/ywacch/cloud9/pkg/engines"
	"github.com/ywacch/cloud9/pkg/placement"
	"github.com/ywacch/cloud9/pkg/templates"
)

// ResolvePlacement maps a design and template into placement.Topology.
func ResolvePlacement(design DeploymentDesign, tmpl *templates.TemplateSpec) (placement.Topology, error) {
	if tmpl == nil {
		return placement.Topology{}, fmt.Errorf("placement: nil template")
	}
	cat := engines.Default()
	bindings := ResolveJobBindings(design, tmpl)
	jobsOnNode := make(map[string]int)
	for _, b := range bindings {
		jobsOnNode[b.NodeID]++
	}

	nodeJobs := make(map[string][]string)
	for _, b := range bindings {
		nodeJobs[b.NodeID] = append(nodeJobs[b.NodeID], b.Job)
	}
	for id := range nodeJobs {
		sort.Strings(nodeJobs[id])
	}

	nodes := nodesSortedByID(design.Nodes)
	topoNodes := make([]placement.Node, 0, len(nodes))
	for _, n := range nodes {
		eng := NodeEngine(n)
		family := ""
		if eng != "" {
			if e, ok := cat.EngineByID(eng); ok {
				family = e.Family
			}
		}
		members, err := membersForNode(n, eng, design)
		if err != nil {
			return placement.Topology{}, err
		}
		topoNodes = append(topoNodes, placement.Node{
			NodeID:   n.ID,
			NodeType: n.Type,
			Engine:   eng,
			Family:   family,
			Members:  members,
			Jobs:     append([]string(nil), nodeJobs[n.ID]...),
		})
	}

	topoJobs := make([]placement.Job, 0, len(bindings))
	for _, b := range bindings {
		job, ok := cat.JobByID(b.Job)
		if !ok {
			return placement.Topology{}, fmt.Errorf("placement: unknown job %q", b.Job)
		}
		topoJobs = append(topoJobs, placement.Job{
			Job:       b.Job,
			Component: job.Component,
			NodeID:    b.NodeID,
			Engine:    b.Engine,
			Shared:    jobsOnNode[b.NodeID] > 1,
			Explicit:  b.Explicit,
		})
	}
	sort.Slice(topoJobs, func(i, j int) bool { return topoJobs[i].Job < topoJobs[j].Job })

	return placement.Topology{Nodes: topoNodes, Jobs: topoJobs}, nil
}

func membersForNode(n DeploymentNode, eng string, design DeploymentDesign) ([]placement.Member, error) {
	switch n.Type {
	case "app-server":
		return []placement.Member{{
			Role:      "primary",
			AppLabel:  "app-server",
			Container: "app-server",
			Service:   "app-server",
		}}, nil
	case "worker":
		return []placement.Member{{
			Role:      "primary",
			AppLabel:  "worker",
			Container: "worker",
		}}, nil
	case "lb":
		return []placement.Member{{
			Role:      "primary",
			AppLabel:  "lb",
			Container: "nginx",
			Service:   "lb",
		}}, nil
	case "db":
		if eng == "" {
			eng = "postgresql"
		}
		if eng != "postgresql" {
			return nil, fmt.Errorf("placement: no deploy identity for engine %q on node %q", eng, n.ID)
		}
		port := enginePort(eng)
		out := []placement.Member{{
			Role:      "primary",
			AppLabel:  "postgres",
			Container: "postgres",
			Service:   "db",
			Port:      port,
		}}
		rr := int(parseReadReplicaCount(n.Config))
		for i := 0; i < rr; i++ {
			out = append(out, placement.Member{
				Role:      "replica",
				Index:     i,
				AppLabel:  "postgres-replica",
				Container: "postgres-replica",
				Service:   "db-replica",
				Port:      port,
			})
		}
		return out, nil
	case "cache":
		return membersForCacheNode(n, eng, design)
	case "mq":
		if eng == "" {
			eng = "rabbitmq"
		}
		if eng != "rabbitmq" {
			return nil, fmt.Errorf("placement: no deploy identity for engine %q on node %q", eng, n.ID)
		}
		return []placement.Member{{
			Role:      "primary",
			AppLabel:  "rabbitmq",
			Container: "rabbitmq",
			Service:   "mq",
			Port:      enginePort(eng),
		}}, nil
	default:
		return nil, fmt.Errorf("placement: no deploy identity for engine %q on node %q", eng, n.ID)
	}
}

func membersForCacheNode(n DeploymentNode, eng string, design DeploymentDesign) ([]placement.Member, error) {
	if eng == "" {
		eng = "redis"
	}
	eng = strings.ToLower(strings.TrimSpace(eng))
	switch eng {
	case "memcached":
		return []placement.Member{{
			Role:      "primary",
			AppLabel:  "memcached",
			Container: "memcached",
			Service:   "cache",
			Port:      enginePort(eng),
		}}, nil
	case "redis":
		if RequiresProxy(n) {
			return []placement.Member{
				{
					Role:      "proxy",
					AppLabel:  "cache-proxy",
					Container: "cache-proxy",
					Service:   "cache",
					Port:      enginePort(eng),
				},
				{
					Role:      "store",
					AppLabel:  "redis-internal",
					Container: "redis",
					Service:   "redis-internal",
					Port:      enginePort(eng),
				},
			}, nil
		}
		_, svc, app := redisCacheDeployIdentity(n, design)
		return []placement.Member{{
			Role:      "primary",
			AppLabel:  app,
			Container: "redis",
			Service:   svc,
			Port:      enginePort(eng),
		}}, nil
	default:
		return nil, fmt.Errorf("placement: no deploy identity for engine %q on node %q", eng, n.ID)
	}
}

func enginePort(engineID string) string {
	engineID = strings.ToLower(strings.TrimSpace(engineID))
	if p, ok := defaultPortsByEngine[engineID]; ok {
		return p
	}
	return ""
}
