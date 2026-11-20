package placement

import "testing"

func TestTopology_NodeByID(t *testing.T) {
	t.Parallel()
	topo := Topology{
		Nodes: []Node{{NodeID: "a", NodeType: "cache"}, {NodeID: "b", NodeType: "db"}},
	}
	if _, ok := topo.NodeByID("missing"); ok {
		t.Fatal("expected missing")
	}
	n, ok := topo.NodeByID("b")
	if !ok || n.NodeType != "db" {
		t.Fatalf("got %+v ok=%v", n, ok)
	}
}

func TestTopology_JobByID(t *testing.T) {
	t.Parallel()
	topo := Topology{Jobs: []Job{{Job: "work_queue", NodeID: "mq-1"}}}
	j, ok := topo.JobByID("work_queue")
	if !ok || j.NodeID != "mq-1" {
		t.Fatalf("got %+v", j)
	}
}

func TestTopology_NodeForComponent(t *testing.T) {
	t.Parallel()
	topo := Topology{
		Nodes: []Node{{NodeID: "c1", NodeType: "cache"}},
		Jobs: []Job{
			{Job: "hot_cache", Component: "cache", NodeID: "c1"},
			{Job: "work_queue", Component: "mq", NodeID: "mq-1"},
		},
	}
	n, ok := topo.NodeForComponent("cache")
	if !ok || n.NodeID != "c1" {
		t.Fatalf("cache node: %+v ok=%v", n, ok)
	}
}

func TestTopology_ComponentsOnNode(t *testing.T) {
	t.Parallel()
	topo := Topology{
		Jobs: []Job{
			{Job: "timeline", Component: "cache", NodeID: "c1"},
			{Job: "hot_cache", Component: "cache", NodeID: "c1"},
		},
	}
	got := topo.ComponentsOnNode("c1")
	if len(got) != 1 || got[0] != "cache" {
		t.Fatalf("got %v", got)
	}
}

func TestTopology_PrimaryMember(t *testing.T) {
	t.Parallel()
	topo := Topology{
		Nodes: []Node{{
			NodeID: "db-1",
			Members: []Member{
				{Role: "replica", AppLabel: "postgres-replica", Index: 0},
				{Role: "primary", AppLabel: "postgres", Container: "postgres"},
			},
		}},
	}
	m, ok := topo.PrimaryMember("db-1")
	if !ok || m.Role != "primary" {
		t.Fatalf("got %+v ok=%v", m, ok)
	}
}
