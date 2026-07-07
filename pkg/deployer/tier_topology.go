package deployer

import "fmt"

// TierTopologyLimits constrains which node types a subscription tier may deploy.
type TierTopologyLimits struct {
	MaxAppServerReplicas int32
	AllowLoadBalancer    bool
	AllowMQWorker        bool
	AllowProxyCache      bool
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
		case "app-server":
			replicas, err := replicaCountFromConfig(
				node.Config,
				1,
				minAppServerReplicas,
				maxAppServerReplicas,
			)
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
