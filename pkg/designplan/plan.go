// Package designplan builds a dry-run diff before deploy (swap plan sheet).
package designplan

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/ywacch/cloud9/pkg/admission"
	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/engines"
	"github.com/ywacch/cloud9/pkg/templates"
)

// Input configures Build.
type Input struct {
	Base, Proposed deployer.DeploymentDesign
	Template       *templates.TemplateSpec
	Catalog        *engines.Catalog
	QuotaTier      string
	Room           *admission.Config // nil = skip room check
}

// Plan is the swap diff returned to clients.
type Plan struct {
	Nodes      NodeDiff         `json:"nodes"`
	Edges      EdgeDiff         `json:"edges"`
	Jobs       []JobMove        `json:"jobs"`
	Budget     BudgetDelta      `json:"budget"`
	Guarantees []GuaranteeDelta `json:"guarantees"`
	Warnings   []string         `json:"warnings"`
	Gated      string           `json:"gated,omitempty"`
}

// NodeDiff lists node id changes.
type NodeDiff struct {
	Added   []string `json:"added"`
	Removed []string `json:"removed"`
	Resized []string `json:"resized"`
}

// EdgeDiff lists connection changes as "src->dst" or "src->dst (job)".
type EdgeDiff struct {
	Added   []string `json:"added"`
	Removed []string `json:"removed"`
}

// JobMove is one job whose binding or engine changed.
type JobMove struct {
	Job  string `json:"job"`
	From string `json:"from"`
	To   string `json:"to"`
	Fit  string `json:"fit"`
}

// BudgetDelta is proposed minus base resource envelope.
type BudgetDelta struct {
	Pods     int64 `json:"pods"`
	CPUMilli int64 `json:"cpuMilli"`
	MemMi    int64 `json:"memMi"`
	RoomFits bool  `json:"roomFits"`
}

// GuaranteeDelta describes a guarantee card entry change.
type GuaranteeDelta struct {
	Change string `json:"change"`
	Chaos  string `json:"chaos,omitempty"`
}

// Build compares base and proposed designs.
func Build(in Input) (Plan, error) {
	if in.Template == nil {
		return Plan{}, fmt.Errorf("designplan: template is required")
	}
	if in.Catalog == nil {
		return Plan{}, fmt.Errorf("designplan: catalog is required")
	}
	tier := strings.TrimSpace(in.QuotaTier)
	if tier == "" {
		tier = "explorer"
	}

	base := in.Base
	proposed := in.Proposed
	deployer.NormalizeDesign(&base)
	deployer.NormalizeDesign(&proposed)
	if len(base.Nodes) == 0 {
		preset := in.Template.Preset
		if preset == nil {
			return Plan{}, fmt.Errorf("designplan: base design empty and template has no preset")
		}
		base = designFromPreset(in.Template.ID, preset)
	}
	if strings.TrimSpace(proposed.Template) == "" {
		proposed.Template = in.Template.ID
	}

	plan := Plan{
		Nodes: diffNodes(base, proposed),
		Edges: diffEdges(base, proposed),
		Jobs:  diffJobs(base, proposed, in.Template, in.Catalog),
	}
	plan.Budget = diffBudget(base, proposed, tier, in.Room)
	plan.Guarantees = diffGuarantees(base, proposed, in.Template, in.Catalog)
	plan.Warnings = collectWarnings(proposed, in.Template, in.Catalog)
	return plan, nil
}

func diffNodes(base, proposed deployer.DeploymentDesign) NodeDiff {
	baseIDs := nodeIDs(base)
	propIDs := nodeIDs(proposed)
	var out NodeDiff
	for id := range propIDs {
		if _, ok := baseIDs[id]; !ok {
			out.Added = append(out.Added, id)
		}
	}
	for id := range baseIDs {
		if _, ok := propIDs[id]; !ok {
			out.Removed = append(out.Removed, id)
		}
	}
	baseCfg := instanceSnapshot(base)
	propCfg := instanceSnapshot(proposed)
	for id := range baseIDs {
		if _, ok := propIDs[id]; !ok {
			continue
		}
		if baseCfg[id] != propCfg[id] {
			out.Resized = append(out.Resized, id)
		}
	}
	sort.Strings(out.Added)
	sort.Strings(out.Removed)
	sort.Strings(out.Resized)
	return out
}

func nodeIDs(d deployer.DeploymentDesign) map[string]struct{} {
	out := make(map[string]struct{}, len(d.Nodes))
	for _, n := range d.Nodes {
		if n.ID != "" {
			out[n.ID] = struct{}{}
		}
	}
	return out
}

func instanceSnapshot(d deployer.DeploymentDesign) map[string]string {
	cfg := deployer.DesignConfigFromDesign(&d)
	out := make(map[string]string)
	for _, insts := range cfg.Roles {
		for _, inst := range insts {
			out[inst.ID] = instanceFingerprint(inst)
		}
	}
	return out
}

func instanceFingerprint(inst deployer.InstanceConfig) string {
	b, _ := json.Marshal(struct {
		Engine   string         `json:"engine"`
		Settings map[string]any `json:"settings"`
	}{inst.Engine, inst.Settings})
	return string(b)
}

func edgeKeys(d deployer.DeploymentDesign) map[string]struct{} {
	out := make(map[string]struct{}, len(d.Connections))
	for _, c := range d.Connections {
		if c.SourceID == "" || c.TargetID == "" {
			continue
		}
		out[fmt.Sprintf("%s->%s", c.SourceID, c.TargetID)] = struct{}{}
	}
	return out
}

func diffEdges(base, proposed deployer.DeploymentDesign) EdgeDiff {
	baseE := edgeKeys(base)
	propE := edgeKeys(proposed)
	var out EdgeDiff
	for k := range propE {
		if _, ok := baseE[k]; !ok {
			out.Added = append(out.Added, k)
		}
	}
	for k := range baseE {
		if _, ok := propE[k]; !ok {
			out.Removed = append(out.Removed, k)
		}
	}
	sort.Strings(out.Added)
	sort.Strings(out.Removed)
	return out
}

func bindingEndpoint(b deployer.Binding) string {
	if b.NodeID == "" {
		return ""
	}
	eng := b.Engine
	if eng == "" {
		eng = "unknown"
	}
	return fmt.Sprintf("%s/%s", b.NodeID, eng)
}

func diffJobs(base, proposed deployer.DeploymentDesign, tmpl *templates.TemplateSpec, cat *engines.Catalog) []JobMove {
	baseMap := bindingsByJob(deployer.ResolveJobBindings(base, tmpl))
	propMap := bindingsByJob(deployer.ResolveJobBindings(proposed, tmpl))
	jobs := unionKeys(baseMap, propMap)
	var out []JobMove
	for _, job := range jobs {
		bb, bok := baseMap[job]
		pb, pok := propMap[job]
		if !bok || !pok {
			continue
		}
		from := bindingEndpoint(bb)
		to := bindingEndpoint(pb)
		if from == to {
			continue
		}
		fit := ""
		if pb.Engine != "" {
			if pair, ok := cat.Pair(job, pb.Engine); ok {
				fit = string(pair.Fit)
			}
		}
		out = append(out, JobMove{Job: job, From: from, To: to, Fit: fit})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Job < out[j].Job })
	return out
}

func bindingsByJob(bindings []deployer.Binding) map[string]deployer.Binding {
	out := make(map[string]deployer.Binding, len(bindings))
	for _, b := range bindings {
		out[b.Job] = b
	}
	return out
}

func unionKeys(a, b map[string]deployer.Binding) []string {
	seen := make(map[string]struct{})
	for k := range a {
		seen[k] = struct{}{}
	}
	for k := range b {
		seen[k] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func diffBudget(base, proposed deployer.DeploymentDesign, tier string, room *admission.Config) BudgetDelta {
	baseW := admission.WeightFromBudget(deployer.QuotaResourceLimits(base, tier))
	propW := admission.WeightFromBudget(deployer.QuotaResourceLimits(proposed, tier))
	roomFits := true
	if room != nil {
		roomFits = !admission.ExceedsRoom(room, propW)
	}
	return BudgetDelta{
		Pods:     deployer.WorkloadPodCount(proposed) - deployer.WorkloadPodCount(base),
		CPUMilli: propW.CPUMillicores - baseW.CPUMillicores,
		MemMi:    propW.MemoryMi - baseW.MemoryMi,
		RoomFits: roomFits,
	}
}

func diffGuarantees(base, proposed deployer.DeploymentDesign, tmpl *templates.TemplateSpec, cat *engines.Catalog) []GuaranteeDelta {
	baseMap := bindingsByJob(deployer.ResolveJobBindings(base, tmpl))
	propMap := bindingsByJob(deployer.ResolveJobBindings(proposed, tmpl))
	jobs := unionKeys(baseMap, propMap)
	var out []GuaranteeDelta
	for _, job := range jobs {
		bb, bok := baseMap[job]
		pb, pok := propMap[job]
		if !bok || !pok || bb.Engine == pb.Engine {
			continue
		}
		baseEntries := guaranteeEntriesByID(cat, job, bb.Engine)
		propEntries := guaranteeEntriesByID(cat, job, pb.Engine)
		for id, e := range baseEntries {
			if _, still := propEntries[id]; still {
				continue
			}
			out = append(out, GuaranteeDelta{Change: e.Claim, Chaos: e.Chaos})
		}
		for id, e := range propEntries {
			if _, had := baseEntries[id]; had {
				continue
			}
			out = append(out, GuaranteeDelta{Change: e.Claim, Chaos: e.Chaos})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Change == out[j].Change {
			return out[i].Chaos < out[j].Chaos
		}
		return out[i].Change < out[j].Change
	})
	return out
}

func guaranteeEntriesByID(cat *engines.Catalog, job, engine string) map[string]engines.GuaranteeEntry {
	out := make(map[string]engines.GuaranteeEntry)
	for _, card := range cat.Guarantees {
		if card.Job != job || card.Engine != engine {
			continue
		}
		for _, e := range card.Entries {
			out[e.ID] = e
		}
		return out
	}
	return out
}

func collectWarnings(proposed deployer.DeploymentDesign, tmpl *templates.TemplateSpec, cat *engines.Catalog) []string {
	var out []string
	for _, w := range deployer.RuntimeContractWarnings(proposed) {
		out = append(out, w.Message)
	}
	if err := deployer.ValidateJobBindings(proposed, tmpl, cat); err != nil {
		out = append(out, err.Error())
	}
	return out
}

func designFromPreset(templateID string, preset *templates.TemplatePreset) deployer.DeploymentDesign {
	d := deployer.DeploymentDesign{
		Version:   "1.0",
		Template:  templateID,
		Archetype: "standard",
		Nodes:     make([]deployer.DeploymentNode, 0, len(preset.Nodes)),
	}
	for _, n := range preset.Nodes {
		cfg := map[string]interface{}{}
		for k, v := range n.Config {
			cfg[k] = v
		}
		pos := append([]float64(nil), n.Position[:]...)
		d.Nodes = append(d.Nodes, deployer.DeploymentNode{
			ID:       n.ID,
			Type:     canvasPresetTypeToBackend(n.Type),
			Name:     n.Label,
			Config:   cfg,
			Position: pos,
		})
	}
	for _, e := range preset.Edges {
		d.Connections = append(d.Connections, deployer.DeploymentConn{
			SourceID: e.SourceID,
			TargetID: e.TargetID,
			Protocol: e.Protocol,
		})
	}
	deployer.NormalizeDesign(&d)
	return d
}

func canvasPresetTypeToBackend(canvasType string) string {
	switch canvasType {
	case "compute":
		return "app-server"
	case "database":
		return "db"
	case "message_queue":
		return "mq"
	case "load_balancer":
		return "lb"
	default:
		return canvasType
	}
}
