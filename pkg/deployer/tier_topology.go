package deployer

import "fmt"

// TierTopologyLimits constrains which node types a subscription tier may deploy.
type TierTopologyLimits struct {
	MaxAppServerReplicas int32
	AllowLoadBalancer    bool
	AllowMQWorker        bool
	AllowProxyCache      bool
	AllowAutoscale       bool
}

// ValidateTierTopology rejects designs that exceed subscription topology caps.
func ValidateTierTopology(design DeploymentDesign, limits TierTopologyLimits) error {
	for _, node := range design.Nodes {
		switch node.Type {
		case "lb":
			if !limits.AllowLoadBalancer {
				return fmt.Errorf("load balancers require an active subscription — remove the load balancer or upgrade")
			}
		case "mq", "worker":
			if !limits.AllowMQWorker {
				return fmt.Errorf("message queues and workers require an active subscription — remove async components or upgrade")
			}
			if node.Type == "worker" {
				if err := rejectAutoscaleUnlessAllowed(node, limits); err != nil {
					return err
				}
			}
		case "app-server":
			if err := rejectAutoscaleUnlessAllowed(node, limits); err != nil {
				return err
			}
			replicas, err := effectiveAppServerReplicasForTier(node)
			if err != nil {
				return fmt.Errorf("app server %q: %w", node.Name, err)
			}
			if limits.MaxAppServerReplicas > 0 && replicas > limits.MaxAppServerReplicas {
				return fmt.Errorf(
					"app server %q: replicas %d exceed your plan limit of %d — reduce replicas or upgrade",
					node.Name, replicas, limits.MaxAppServerReplicas,
				)
			}
		case "cache":
			if !limits.AllowProxyCache && RequiresProxy(node) {
				return fmt.Errorf(
					"proxy cache strategies require an active subscription — use cache-aside or timeline-store, or upgrade",
				)
			}
		}
	}
	return nil
}

func rejectAutoscaleUnlessAllowed(node DeploymentNode, limits TierTopologyLimits) error {
	if limits.AllowAutoscale || !nodeRequestsAutoscale(node.Config) {
		return nil
	}
	kind := "app server"
	if node.Type == "worker" {
		kind = "worker"
	}
	return fmt.Errorf("%s %q: cpu autoscale is limited to internal accounts", kind, node.Name)
}

func nodeRequestsAutoscale(config map[string]interface{}) bool {
	if config == nil {
		return false
	}
	raw, ok := config["autoscale"]
	return ok && raw != nil
}
