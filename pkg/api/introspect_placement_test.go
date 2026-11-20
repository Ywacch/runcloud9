package api

import (
	"context"
	"testing"

	"github.com/ywacch/cloud9/pkg/introspect"
	"github.com/ywacch/cloud9/pkg/placement"
)

func TestResolveIntrospectHost_PlacementWorkQueue(t *testing.T) {
	t.Parallel()
	topo := placement.Topology{
		Nodes: []placement.Node{{
			NodeID: "wq",
			Engine: "redis",
			Members: []placement.Member{{
				Role: "primary", AppLabel: "work-queue", Service: "work-queue", Port: "6379",
			}},
		}},
		Jobs: []placement.Job{{Job: "work_queue", Component: "mq", NodeID: "wq", Engine: "redis"}},
	}
	host, port, err := resolveIntrospectHost(context.Background(), "redis", "mq", "ns1", "primary", "", nil, nil, &topo)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if host != "work-queue.ns1.svc.cluster.local" || port != 6379 {
		t.Fatalf("host=%q port=%d", host, port)
	}
}

func TestIntrospectEngineForComponent_mqFromPlacement(t *testing.T) {
	t.Parallel()
	topo := placement.Topology{
		Nodes: []placement.Node{{NodeID: "wq", Engine: "redis"}},
		Jobs:  []placement.Job{{Job: "work_queue", Component: "mq", NodeID: "wq", Engine: "redis"}},
	}
	eng := introspectEngineForComponent("mq", &topo, introspect.ComponentSpec{Queues: []string{"fanout"}}, nil)
	if eng != "redis" {
		t.Fatalf("engine=%q", eng)
	}
}
