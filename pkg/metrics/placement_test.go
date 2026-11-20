package metrics

import (
	"testing"

	"github.com/ywacch/cloud9/pkg/placement"
)

func TestMetricsCollector_SetPlacement_Cleanup(t *testing.T) {
	t.Parallel()
	c := NewMetricsCollector("", nil, nil, nil, nil)
	topo := placement.Topology{Jobs: []placement.Job{{Job: "main_db", Component: "db", NodeID: "db-1", Engine: "postgresql"}}}
	c.SetPlacement("ns-a", topo)
	if p := c.placementForNamespace("ns-a"); p == nil || len(p.Jobs) != 1 {
		t.Fatal("expected placement stored")
	}
	c.Cleanup("ns-a")
	if p := c.placementForNamespace("ns-a"); p != nil {
		t.Fatal("expected placement removed after Cleanup")
	}
}
