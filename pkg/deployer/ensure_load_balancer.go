package deployer

const autoLoadBalancerNodeID = "auto-load-balancer"

// EnsureLoadBalancerForReplicas injects a load balancer node and lb -> app-server
// edges when any app-server can run more than one pod and the design has no lb node.
// That includes a fixed replica count above 1 and CPU autoscale whose max is above 1.
// Call after NormalizeDesign so node types and config keys are canonical.
func EnsureLoadBalancerForReplicas(d *DeploymentDesign) {
	if d == nil {
		return
	}
	targets := appServerIDsNeedingLoadBalancer(*d)
	if len(targets) == 0 || designHasLBNode(*d) {
		return
	}

	d.Nodes = append(d.Nodes, DeploymentNode{
		ID:   autoLoadBalancerNodeID,
		Type: "lb",
		Name: "Load Balancer",
		Config: map[string]interface{}{
			"lbAlgorithm": "round-robin",
			"autoAdded":   true,
		},
	})

	for _, targetID := range targets {
		if hasConnection(*d, autoLoadBalancerNodeID, targetID) {
			continue
		}
		d.Connections = append(d.Connections, DeploymentConn{
			SourceID: autoLoadBalancerNodeID,
			TargetID: targetID,
			Protocol: "http",
		})
	}
}

func designHasLBNode(d DeploymentDesign) bool {
	for _, n := range d.Nodes {
		if n.Type == "lb" {
			return true
		}
	}
	return false
}

func appServerIDsNeedingLoadBalancer(d DeploymentDesign) []string {
	var ids []string
	for _, n := range d.Nodes {
		if n.Type != "app-server" {
			continue
		}
		if appServerNeedsLoadBalancer(n.Config) {
			ids = append(ids, n.ID)
		}
	}
	return ids
}

func appServerNeedsLoadBalancer(config map[string]any) bool {
	if parseReplicaCount(config, 1) > 1 {
		return true
	}
	ac, err := parseAutoscaleConfig(config)
	if err != nil || ac == nil {
		return false
	}
	return ac.Max > 1
}

func hasConnection(d DeploymentDesign, sourceID, targetID string) bool {
	for _, c := range d.Connections {
		if c.SourceID == sourceID && c.TargetID == targetID {
			return true
		}
	}
	return false
}
