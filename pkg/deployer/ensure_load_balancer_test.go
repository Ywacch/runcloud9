package deployer

import (
	"testing"

	"github.com/ywacch/cloud9/pkg/templates"
)

func TestEnsureLoadBalancerForReplicas_noOpSingleReplica(t *testing.T) {
	t.Parallel()

	d := urlShortenerLiteDesign()
	before := len(d.Nodes)
	EnsureLoadBalancerForReplicas(&d)
	if len(d.Nodes) != before {
		t.Fatalf("nodes: got %d want %d", len(d.Nodes), before)
	}
}

func TestEnsureLoadBalancerForReplicas_noOpLBPresent(t *testing.T) {
	t.Parallel()

	d := urlShortenerDefaultDesign()
	d.Nodes[1].Config["replicas"] = 3
	before := len(d.Nodes)
	EnsureLoadBalancerForReplicas(&d)
	if len(d.Nodes) != before {
		t.Fatalf("nodes: got %d want %d", len(d.Nodes), before)
	}
}

func TestEnsureLoadBalancerForReplicas_injectsLBAndEdges(t *testing.T) {
	t.Parallel()

	d := urlShortenerLiteDesign()
	d.Nodes[0].Config["replicas"] = 2
	d.Connections = []DeploymentConn{
		{SourceID: "app-1", TargetID: "cache-1"},
		{SourceID: "app-1", TargetID: "db-1"},
	}
	EnsureLoadBalancerForReplicas(&d)

	if !designHasLBNode(d) {
		t.Fatal("expected lb node")
	}
	lb := findNodeByID(d, autoLoadBalancerNodeID)
	if lb == nil {
		t.Fatalf("missing node %q", autoLoadBalancerNodeID)
	}
	if configString(lb.Config, "lbAlgorithm", "") != "round-robin" {
		t.Fatalf("lbAlgorithm: got %q", configString(lb.Config, "lbAlgorithm", ""))
	}
	if !configBool(lb.Config, "autoAdded") {
		t.Fatal("expected autoAdded=true on injected lb")
	}
	if !hasConnection(d, autoLoadBalancerNodeID, "app-1") {
		t.Fatal("expected lb -> app-1 connection")
	}

	tmpl := templates.GetTemplate(d.Template)
	if err := ValidateRuntimeContract(d, tmpl); err != nil {
		t.Fatalf("ValidateRuntimeContract: %v", err)
	}
}

func TestEnsureLoadBalancerForReplicas_idempotent(t *testing.T) {
	t.Parallel()

	d := urlShortenerLiteDesign()
	d.Nodes[0].Config["replicas"] = 3
	EnsureLoadBalancerForReplicas(&d)
	nodesAfterFirst := len(d.Nodes)
	connsAfterFirst := len(d.Connections)
	EnsureLoadBalancerForReplicas(&d)
	if len(d.Nodes) != nodesAfterFirst || len(d.Connections) != connsAfterFirst {
		t.Fatalf("second call mutated design: nodes %d->%d conns %d->%d",
			nodesAfterFirst, len(d.Nodes), connsAfterFirst, len(d.Connections))
	}
}

func TestEnsureLoadBalancerForReplicas_engineerTierTopology(t *testing.T) {
	t.Parallel()

	d := urlShortenerLiteDesign()
	d.Nodes[0].Config["replicas"] = 2
	EnsureLoadBalancerForReplicas(&d)

	limits := TierTopologyLimits{
		MaxAppServerReplicas: 3,
		AllowLoadBalancer:    true,
		AllowMQWorker:        true,
		AllowProxyCache:      true,
	}
	if err := ValidateTierTopology(d, limits); err != nil {
		t.Fatalf("ValidateTierTopology: %v", err)
	}
}

func findNodeByID(d DeploymentDesign, id string) *DeploymentNode {
	for i := range d.Nodes {
		if d.Nodes[i].ID == id {
			return &d.Nodes[i]
		}
	}
	return nil
}

func configBool(config map[string]interface{}, key string) bool {
	if config == nil {
		return false
	}
	v, ok := config[key]
	if !ok {
		return false
	}
	b, ok := v.(bool)
	return ok && b
}
