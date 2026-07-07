package deployer

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// ResourceBudgetLine is one row in a pre-deploy resource breakdown.
type ResourceBudgetLine struct {
	Label  string `json:"label"`
	CPU    string `json:"cpu"`
	Memory string `json:"memory"`
}

// ResourceBudget totals requested CPU and memory for a design or quota envelope.
type ResourceBudget struct {
	CPU       resource.Quantity
	Memory    resource.Quantity
	Breakdown []ResourceBudgetLine
}

// ResourceBudgetError is returned when a design exceeds subscription tier caps.
type ResourceBudgetError struct {
	Tier      string
	Requested ResourceBudget
	CapCPU    resource.Quantity
	CapMemory resource.Quantity
}

func (e *ResourceBudgetError) Error() string {
	if e == nil {
		return "resource budget exceeded"
	}
	return fmt.Sprintf(
		"design exceeds %s quota (need %s CPU / %s memory, cap %s CPU / %s memory)",
		e.Tier,
		e.Requested.CPU.String(),
		e.Requested.Memory.String(),
		e.CapCPU.String(),
		e.CapMemory.String(),
	)
}

// ResourceBudgetErrorJSON is the structured 422 payload for the API.
type ResourceBudgetErrorJSON struct {
	Error     string               `json:"error"`
	Tier      string               `json:"tier"`
	Requested map[string]string    `json:"requested"`
	Cap       map[string]string    `json:"cap"`
	Breakdown []ResourceBudgetLine `json:"breakdown"`
}

// JSON returns the API-facing representation of a budget error.
func (e *ResourceBudgetError) JSON() ResourceBudgetErrorJSON {
	if e == nil {
		return ResourceBudgetErrorJSON{Error: "resource budget exceeded"}
	}
	return ResourceBudgetErrorJSON{
		Error: e.Error(),
		Tier:  e.Tier,
		Requested: map[string]string{
			"cpu":    e.Requested.CPU.String(),
			"memory": e.Requested.Memory.String(),
		},
		Cap: map[string]string{
			"cpu":    e.CapCPU.String(),
			"memory": e.CapMemory.String(),
		},
		Breakdown: e.Requested.Breakdown,
	}
}

// TierResourceCaps returns subscription CPU/memory caps for a tier name.
func TierResourceCaps(tier string) (cpu, memory resource.Quantity) {
	switch strings.ToLower(strings.TrimSpace(tier)) {
	case "engineer":
		return resource.MustParse("1500m"), resource.MustParse("3Gi")
	case "architect":
		return resource.MustParse("4"), resource.MustParse("8Gi")
	default: // explorer
		return resource.MustParse("625m"), resource.MustParse("1280Mi")
	}
}

// SumDesignResources totals main workload containers for user-facing validation.
// Sidecars are excluded; loadgen is included because it always deploys in the namespace.
func SumDesignResources(design DeploymentDesign, userTier string) ResourceBudget {
	var total ResourceBudget
	for _, node := range design.Nodes {
		if !isDeployableNodeType(node.Type) {
			continue
		}
		replicas := replicasForBudget(node)
		if replicas < 1 {
			replicas = 1
		}
		res := ResolveNodeResources(node.Type, node.Config)
		label := budgetNodeLabel(node, replicas)
		addRequirements(&total, label, res, replicas)
	}
	addRequirements(&total, "loadgen", loadgenResources(userTier), 1)
	return total
}

// SidecarOverhead returns exporter sidecar resources not shown in canvas labels.
func SidecarOverhead(design DeploymentDesign) ResourceBudget {
	var total ResourceBudget
	for _, node := range design.Nodes {
		if !sidecarOverheadForNode(node) {
			continue
		}
		replicas := replicasForBudget(node)
		if replicas < 1 {
			replicas = 1
		}
		label := budgetNodeLabel(node, replicas) + " exporter"
		addRequirements(&total, label, ExporterResources(), replicas)
	}
	return total
}

// ProxyCacheOverhead adds the second main container per proxy-strategy cache node.
func ProxyCacheOverhead(design DeploymentDesign) ResourceBudget {
	var total ResourceBudget
	for _, node := range design.Nodes {
		if node.Type != "cache" || !RequiresProxy(node) {
			continue
		}
		replicas := replicasForBudget(node)
		if replicas < 1 {
			replicas = 1
		}
		res := ResolveNodeResources(node.Type, node.Config)
		label := budgetNodeLabel(node, replicas) + " cache-proxy"
		addRequirements(&total, label, res, replicas)
	}
	return total
}

// QuotaResourceLimits returns namespace ResourceQuota hard limits for a design.
func QuotaResourceLimits(design DeploymentDesign, userTier string) ResourceBudget {
	total := SumDesignResources(design, userTier)
	mergeBudget(&total, SidecarOverhead(design))
	mergeBudget(&total, ProxyCacheOverhead(design))
	return total
}

// ValidateResourceBudget rejects designs whose canvas+loadgen totals exceed tier caps.
func ValidateResourceBudget(design DeploymentDesign, userTier string) error {
	requested := SumDesignResources(design, userTier)
	capCPU, capMem := TierResourceCaps(userTier)
	if requested.CPU.Cmp(capCPU) <= 0 && requested.Memory.Cmp(capMem) <= 0 {
		return nil
	}
	tier := strings.ToLower(strings.TrimSpace(userTier))
	if tier == "" {
		tier = "explorer"
	}
	return &ResourceBudgetError{
		Tier:      tier,
		Requested: requested,
		CapCPU:    capCPU,
		CapMemory: capMem,
	}
}

func isDeployableNodeType(nodeType string) bool {
	switch nodeType {
	case "db", "mq", "cache", "app-server", "worker", "lb":
		return true
	default:
		return false
	}
}

func sidecarOverheadForNode(node DeploymentNode) bool {
	switch node.Type {
	case "db":
		return true
	case "cache":
		return true
	default:
		return false
	}
}

func replicasForBudget(node DeploymentNode) int32 {
	switch node.Type {
	case "app-server":
		n, err := replicaCountFromConfig(node.Config, 1, minAppServerReplicas, maxAppServerReplicas)
		if err != nil {
			return 1
		}
		return n
	default:
		return parseReplicaCount(node.Config, 1)
	}
}

func budgetNodeLabel(node DeploymentNode, replicas int32) string {
	name := strings.TrimSpace(node.Name)
	if name == "" {
		name = node.Type
	}
	if replicas > 1 {
		return fmt.Sprintf("%s×%d", name, replicas)
	}
	return name
}

func addRequirements(total *ResourceBudget, label string, res corev1.ResourceRequirements, replicas int32) {
	cpu := scaleQuantity(*res.Limits.Cpu(), replicas)
	mem := scaleQuantity(*res.Limits.Memory(), replicas)
	total.CPU.Add(cpu)
	total.Memory.Add(mem)
	total.Breakdown = append(total.Breakdown, ResourceBudgetLine{
		Label:  label,
		CPU:    cpu.String(),
		Memory: mem.String(),
	})
}

func mergeBudget(dst *ResourceBudget, src ResourceBudget) {
	dst.CPU.Add(src.CPU)
	dst.Memory.Add(src.Memory)
	dst.Breakdown = append(dst.Breakdown, src.Breakdown...)
}

func scaleQuantity(q resource.Quantity, replicas int32) resource.Quantity {
	if replicas <= 1 {
		return q.DeepCopy()
	}
	out := q.DeepCopy()
	out.SetMilli(out.MilliValue() * int64(replicas))
	return out
}
