package metrics

import (
	"testing"

	"github.com/ywacch/cloud9/pkg/placement"
)

func TestWorkQueueEngineLabel_FromPlacement(t *testing.T) {
	t.Parallel()
	dm := &DeploymentMetrics{
		Placement: &placement.Topology{
			Jobs: []placement.Job{{
				Job: "work_queue", Engine: "redis", Component: "mq", NodeID: "c1",
			}},
		},
	}
	if got := WorkQueueEngineLabel(dm); got != "Redis" {
		t.Fatalf("label=%q", got)
	}
}

func TestWorkQueueEngineLabel_Fallback(t *testing.T) {
	t.Parallel()
	if got := WorkQueueEngineLabel(nil); got != "RabbitMQ" {
		t.Fatalf("label=%q", got)
	}
}
