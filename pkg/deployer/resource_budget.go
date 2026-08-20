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
		return resource.MustParse("3375m"), resource.MustParse("6912Mi")
	case "internal":
		return resource.MustParse("4250m"), resource.MustParse("8320Mi")
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
		if node.Type == "app-server" || node.Type == "worker" {
			addPerCopyBudget(&total, node)
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
// Uses CacheProxyResources (asymmetric vs redis-internal) so quota matches deploy.
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
		res := CacheProxyResources(node.Config)
		label := budgetNodeLabel(node, replicas) + " cache-proxy"
		addRequirements(&total, label, res, replicas)
	}
	return total
}

// QuotaResourceLimits returns namespace ResourceQuota hard limits for a design.
// Includes GKE Autopilot admission slack so admitted pod limits fit the quota envelope.
func QuotaResourceLimits(design DeploymentDesign, userTier string) ResourceBudget {
	total := SumDesignResources(design, userTier)
	mergeBudget(&total, SidecarOverhead(design))
	mergeBudget(&total, ProxyCacheOverhead(design))
	mergeBudget(&total, autopilotAdmissionOverhead(design, userTier))
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
	case "worker":
		n, err := replicaCountFromConfig(node.Config, 1, minWorkerReplicas, maxWorkerReplicas)
		if err != nil {
			return 1
		}
		return n
	default:
		return parseReplicaCount(node.Config, 1)
	}
}

func addPerCopyBudget(total *ResourceBudget, node DeploymentNode) {
	replicas := replicasForBudget(node)
	if replicas < 1 {
		replicas = 1
	}
	sizes := instanceSizesFromConfig(node.Config, int(replicas))
	groups := sizeGroups(sizes)
	name := strings.TrimSpace(node.Name)
	if name == "" {
		name = node.Type
	}
	for _, g := range groups {
		cfg := map[string]any{"instanceSize": g.Size}
		res := ResolveNodeResources(node.Type, cfg)
		label := fmt.Sprintf("%s %s", name, g.Size)
		if g.Count > 1 {
			label = fmt.Sprintf("%s %s×%d", name, g.Size, g.Count)
		}
		addRequirements(total, label, res, g.Count)
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

var (
	autopilotSlackMemoryThreshold = resource.MustParse("512Mi")
	autopilotSlackPerContainer    = resource.MustParse("128Mi")
)

// autopilotAdmissionOverhead adds memory slack for GKE Autopilot limit admission adjustments.
// Manifest requests can be admitted higher than requested (see ganymede_appendix); namespace
// ResourceQuota must account for admitted limits, not only SumDesignResources requests.
func autopilotAdmissionOverhead(design DeploymentDesign, userTier string) ResourceBudget {
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
		addAutopilotContainerSlack(&total, budgetNodeLabel(node, replicas)+" autopilot slack", res, replicas)
		if sidecarOverheadForNode(node) {
			addAutopilotContainerSlack(
				&total,
				budgetNodeLabel(node, replicas)+" exporter autopilot slack",
				ExporterResources(),
				replicas,
			)
		}
		if node.Type == "cache" && RequiresProxy(node) {
			addAutopilotContainerSlack(
				&total,
				budgetNodeLabel(node, replicas)+" cache-proxy autopilot slack",
				CacheProxyResources(node.Config),
				replicas,
			)
		}
	}
	addAutopilotContainerSlack(&total, "loadgen autopilot slack", loadgenResources(userTier), 1)
	return total
}

func addAutopilotContainerSlack(total *ResourceBudget, label string, res corev1.ResourceRequirements, replicas int32) {
	slack := autopilotMemorySlackForContainer(res)
	if slack.IsZero() {
		return
	}
	scaled := scaleQuantity(slack, replicas)
	total.Memory.Add(scaled)
	total.Breakdown = append(total.Breakdown, ResourceBudgetLine{
		Label:  label,
		CPU:    "0",
		Memory: scaled.String(),
	})
}

func autopilotMemorySlackForContainer(res corev1.ResourceRequirements) resource.Quantity {
	if res.Limits == nil {
		return resource.Quantity{}
	}
	mem := res.Limits.Memory()
	if mem == nil || mem.IsZero() {
		return resource.Quantity{}
	}
	if mem.Cmp(autopilotSlackMemoryThreshold) < 0 {
		return autopilotSlackPerContainer.DeepCopy()
	}
	return resource.Quantity{}
}
