package deployer

// NormalizeDesign mutates d in place so stored designs and deployers use canonical
// node types and config keys (e.g. canvas "load_balancer" → "lb", "algorithm" → "lbAlgorithm").
func NormalizeDesign(d *DeploymentDesign) {
	if d == nil {
		return
	}
	typeAliases := map[string]string{
		"load_balancer": "lb",
		"compute":       "app-server",
	}
	for i := range d.Nodes {
		if canon, ok := typeAliases[d.Nodes[i].Type]; ok {
			d.Nodes[i].Type = canon
		}
		normalizeNodeConfig(&d.Nodes[i])
	}
}

func normalizeNodeConfig(n *DeploymentNode) {
	if n == nil || n.Config == nil {
		return
	}

	// Map canvas instanceSize to deployer resourceProfile. Makes sure app server cpu and memory requests/limits are set.
	if _, has := n.Config["resourceProfile"]; !has {
		if size, ok := n.Config["instanceSize"]; ok && size != nil {
			n.Config["resourceProfile"] = size
		}
	}

	if n.Type != "lb" {
		return
	}

	if algo, ok := n.Config["algorithm"]; ok && algo != nil {
		if _, has := n.Config["lbAlgorithm"]; !has {
			n.Config["lbAlgorithm"] = algo
		}
		delete(n.Config, "algorithm")
	}
}
