package chaos

import (
	"strings"
	"testing"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/placement"
	"k8s.io/client-go/kubernetes/fake"
)

func TestResolve_pauseConsumers_pairGate(t *testing.T) {
	t.Parallel()
	client := fake.NewClientset()
	design := deployer.DeploymentDesign{
		ResolvedPlacement: &placement.Topology{
			Jobs: []placement.Job{{
				Job: "work_queue", Component: "mq", NodeID: "mq-1", Engine: "redis",
			}},
			Nodes: []placement.Node{{
				NodeID: "mq-1",
				Engine: "redis",
				Jobs:   []string{"work_queue"},
			}},
		},
	}
	if _, err := Resolve(scenarioPauseConsumers, "mq", design, client); err != nil {
		t.Fatalf("Resolve pause_consumers: %v", err)
	}

	design.ResolvedPlacement.Jobs[0].Engine = "rabbitmq"
	design.ResolvedPlacement.Nodes[0].Engine = "rabbitmq"
	if _, err := Resolve(scenarioPauseConsumers, "mq", design, client); err != nil {
		t.Fatalf("Resolve pause_consumers rabbitmq: %v", err)
	}
}

func TestPairAllowsScenario_deniesPromoteOnMQ(t *testing.T) {
	t.Parallel()
	design := deployer.DeploymentDesign{
		ResolvedPlacement: &placement.Topology{
			Jobs: []placement.Job{{
				Job: "work_queue", Component: "mq", NodeID: "mq-1", Engine: "redis",
			}},
			Nodes: []placement.Node{{
				NodeID: "mq-1",
				Engine: "redis",
				Jobs:   []string{"work_queue"},
			}},
		},
	}
	err := pairAllowsScenario("promote_replica", "mq", design)
	if err == nil || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("promote_replica on mq should be denied, got %v", err)
	}
}
