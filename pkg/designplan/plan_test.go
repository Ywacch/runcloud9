package designplan

import (
	"testing"

	"github.com/ywacch/cloud9/pkg/admission"
	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/engines"
	"github.com/ywacch/cloud9/pkg/templates"
)

func TestBuild_noOp(t *testing.T) {
	t.Parallel()
	tmpl := templates.GetTemplate("url-shortener")
	if tmpl == nil {
		t.Fatal("template missing")
	}
	design := designFromPreset("url-shortener", tmpl.Preset)
	plan, err := Build(Input{
		Base:     design,
		Proposed: design,
		Template: tmpl,
		Catalog:  engines.Default(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Nodes.Added)+len(plan.Nodes.Removed)+len(plan.Nodes.Resized) != 0 {
		t.Fatalf("nodes: %+v", plan.Nodes)
	}
	if len(plan.Edges.Added)+len(plan.Edges.Removed) != 0 {
		t.Fatalf("edges: %+v", plan.Edges)
	}
	if len(plan.Jobs) != 0 {
		t.Fatalf("jobs: %+v", plan.Jobs)
	}
	if plan.Budget.Pods != 0 || plan.Budget.CPUMilli != 0 || plan.Budget.MemMi != 0 {
		t.Fatalf("budget: %+v", plan.Budget)
	}
}

func TestBuild_consolidation_timelineToDB(t *testing.T) {
	t.Parallel()
	tmpl := templates.GetTemplate("social-feed")
	if tmpl == nil || tmpl.Preset == nil {
		t.Fatal("social-feed preset missing")
	}

	proposed := designFromPreset("social-feed", tmpl.Preset)
	proposed.Nodes = filterNodes(proposed.Nodes, func(n deployer.DeploymentNode) bool {
		return n.ID != "cache-1"
	})
	proposed.Connections = filterConns(proposed.Connections, func(c deployer.DeploymentConn) bool {
		return c.TargetID != "cache-1" && c.SourceID != "cache-1"
	})
	proposed.JobBindings = map[string]string{"timeline": "db-1"}
	for i := range proposed.Connections {
		if proposed.Connections[i].SourceID == "worker-1" && proposed.Connections[i].TargetID == "db-1" {
			proposed.Connections[i].Protocol = "timeline"
		}
	}
	// worker should reach db for timeline fanout
	hasWorkerDB := false
	for _, c := range proposed.Connections {
		if c.SourceID == "worker-1" && c.TargetID == "db-1" {
			hasWorkerDB = true
		}
	}
	if !hasWorkerDB {
		proposed.Connections = append(proposed.Connections, deployer.DeploymentConn{
			SourceID: "worker-1",
			TargetID: "db-1",
			Protocol: "tcp",
		})
	}

	plan, err := Build(Input{
		Proposed:  proposed,
		Template:  tmpl,
		Catalog:   engines.Default(),
		QuotaTier: "explorer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Nodes.Removed) != 1 || plan.Nodes.Removed[0] != "cache-1" {
		t.Fatalf("removed nodes: %+v", plan.Nodes.Removed)
	}
	if plan.Budget.Pods >= 0 {
		t.Fatalf("expected fewer pods, got %+v", plan.Budget)
	}
	var timelineMove *JobMove
	for i := range plan.Jobs {
		if plan.Jobs[i].Job == "timeline" {
			timelineMove = &plan.Jobs[i]
			break
		}
	}
	if timelineMove == nil {
		t.Fatalf("jobs: %+v", plan.Jobs)
	}
	if timelineMove.From != "cache-1/redis" {
		t.Fatalf("from: %q", timelineMove.From)
	}
	if timelineMove.To != "db-1/postgresql" {
		t.Fatalf("to: %q", timelineMove.To)
	}
	if timelineMove.Fit != string(engines.FitWorkable) {
		t.Fatalf("fit: %q", timelineMove.Fit)
	}
}

func TestBuild_resizeOnly(t *testing.T) {
	t.Parallel()
	tmpl := templates.GetTemplate("url-shortener")
	base := designFromPreset("url-shortener", tmpl.Preset)
	proposed := designFromPreset("url-shortener", tmpl.Preset)
	for i := range proposed.Nodes {
		if proposed.Nodes[i].ID == "app-1" {
			proposed.Nodes[i].Config["instanceSize"] = "large"
		}
	}
	plan, err := Build(Input{
		Base:     base,
		Proposed: proposed,
		Template: tmpl,
		Catalog:  engines.Default(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Nodes.Resized) != 1 || plan.Nodes.Resized[0] != "app-1" {
		t.Fatalf("resized: %+v", plan.Nodes.Resized)
	}
	if len(plan.Nodes.Added)+len(plan.Nodes.Removed) != 0 {
		t.Fatalf("added/removed: %+v %+v", plan.Nodes.Added, plan.Nodes.Removed)
	}
}

func TestBuild_roomDoesNotFit(t *testing.T) {
	t.Parallel()
	tmpl := templates.GetTemplate("url-shortener")
	proposed := designFromPreset("url-shortener", tmpl.Preset)
	for i := range proposed.Nodes {
		if proposed.Nodes[i].Type == "app-server" {
			proposed.Nodes[i].Config["replicas"] = 3
			proposed.Nodes[i].Config["instanceSize"] = "large"
		}
	}
	room := &admission.Config{CPUMillicores: 1, MemoryMi: 1}
	plan, err := Build(Input{
		Proposed:  proposed,
		Template:  tmpl,
		Catalog:   engines.Default(),
		QuotaTier: "explorer",
		Room:      room,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Budget.RoomFits {
		t.Fatalf("budget: %+v", plan.Budget)
	}
}

func filterNodes(nodes []deployer.DeploymentNode, keep func(deployer.DeploymentNode) bool) []deployer.DeploymentNode {
	out := make([]deployer.DeploymentNode, 0, len(nodes))
	for _, n := range nodes {
		if keep(n) {
			out = append(out, n)
		}
	}
	return out
}

func filterConns(conns []deployer.DeploymentConn, keep func(deployer.DeploymentConn) bool) []deployer.DeploymentConn {
	out := make([]deployer.DeploymentConn, 0, len(conns))
	for _, c := range conns {
		if keep(c) {
			out = append(out, c)
		}
	}
	return out
}
