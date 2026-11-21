package metrics

import (
	"context"
	"fmt"

	"github.com/ywacch/cloud9/pkg/placement"
)

// NodeMetrics holds container resources and runtime health for one canvas node.
type NodeMetrics struct {
	NodeID         string         `json:"nodeId"`
	CPUUsage       float64        `json:"cpuUsage"`
	MemoryBytes    int64          `json:"memoryUsage"`
	CPULimit       float64        `json:"cpuLimit,omitempty"`
	MemoryLimit    int64          `json:"memoryLimit,omitempty"`
	CPUIsEstimated bool           `json:"cpuIsEstimated,omitempty"`
	MemIsEstimated bool           `json:"memIsEstimated,omitempty"`
	CPUSource      string         `json:"cpuSource,omitempty"`
	MemSource      string         `json:"memSource,omitempty"`
	RuntimeHealth  *RuntimeHealth `json:"runtimeHealth,omitempty"`
}

func podReForAppLabel(app string) string {
	if app == "" {
		return ".*"
	}
	return fmt.Sprintf("%s.*", app)
}

func (c *MetricsCollector) collectNodeMetrics(ctx context.Context, ns string, topo *placement.Topology, dm *DeploymentMetrics) {
	if c == nil || topo == nil || dm == nil {
		return
	}
	if dm.Nodes == nil {
		dm.Nodes = make(map[string]NodeMetrics)
	}
	for _, node := range topo.Nodes {
		member, ok := topo.PrimaryMember(node.NodeID)
		if !ok || member.AppLabel == "" {
			continue
		}
		podRe := podReForAppLabel(member.AppLabel)
		container := member.Container
		if container == "" {
			container = member.AppLabel
		}
		nm := NodeMetrics{NodeID: node.NodeID}
		cpuRes := c.queryContainerResourceCPU(ctx, ns, podRe, container)
		nm.CPUUsage = cpuRes.Usage
		nm.CPUIsEstimated = cpuRes.Estimated
		nm.CPUSource = cpuRes.Source
		memRes := c.queryContainerResourceMemory(ctx, ns, podRe, container)
		nm.MemoryBytes = int64(memRes.Usage)
		nm.MemIsEstimated = memRes.Estimated
		nm.MemSource = memRes.Source
		nm.CPULimit = c.kubeLimitCPU(ctx, ns, podRe, container)
		nm.MemoryLimit = c.kubeLimitMem(ctx, ns, podRe, container)
		dm.Nodes[node.NodeID] = nm
	}
}

func stripSharedNodeResources(dm *DeploymentMetrics) {
	if dm == nil || dm.Placement == nil {
		return
	}
	for _, node := range dm.Placement.Nodes {
		aliases := dm.Placement.ComponentsOnNode(node.NodeID)
		if len(aliases) < 2 {
			continue
		}
		for _, alias := range aliases {
			cm, ok := dm.Components[alias]
			if !ok {
				continue
			}
			clearComponentNodeResources(&cm)
			dm.Components[alias] = cm
		}
	}
}

func clearComponentNodeResources(cm *ComponentMetrics) {
	if cm == nil {
		return
	}
	cm.CPUUsage = 0
	cm.MemoryBytes = 0
	cm.CPULimit = 0
	cm.MemoryLimit = 0
	cm.CPUIsEstimated = false
	cm.MemIsEstimated = false
	cm.CPUSource = ""
	cm.MemSource = ""
	cm.RuntimeHealth = nil
}
