package deployer

import (
	"context"
	"fmt"
	"strconv"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// AutoscaleConfig holds CPU horizontal scaling bounds for app-server and worker nodes.
type AutoscaleConfig struct {
	Min              int32
	Max              int32
	TargetCPUPercent int32
}

// parseAutoscaleConfig reads node.config.autoscale. Absent or null returns (nil, nil).
func parseAutoscaleConfig(config map[string]any) (*AutoscaleConfig, error) {
	if config == nil {
		return nil, nil
	}
	raw, present := config["autoscale"]
	if !present || raw == nil {
		return nil, nil
	}
	sub, ok := asConfigMap(raw)
	if !ok {
		return nil, fmt.Errorf("autoscale must be an object")
	}
	min, err := intFieldInRange(sub, "min", 1, maxAppServerReplicas)
	if err != nil {
		return nil, fmt.Errorf("autoscale.min: %w", err)
	}
	max, err := intFieldInRange(sub, "max", 1, maxAppServerReplicas)
	if err != nil {
		return nil, fmt.Errorf("autoscale.max: %w", err)
	}
	if max < min {
		return nil, fmt.Errorf("autoscale.max must be at least autoscale.min (min=%d max=%d)", min, max)
	}
	target, err := intFieldInRange(sub, "targetCPUPercent", 1, 100)
	if err != nil {
		return nil, fmt.Errorf("autoscale.targetCPUPercent: %w", err)
	}
	return &AutoscaleConfig{
		Min:              int32(min),
		Max:              int32(max),
		TargetCPUPercent: int32(target),
	}, nil
}

func configSubMap(cfg map[string]any, key string) (map[string]any, bool) {
	if cfg == nil {
		return nil, false
	}
	raw, ok := cfg[key]
	if !ok || raw == nil {
		return nil, false
	}
	return asConfigMap(raw)
}

func asConfigMap(raw any) (map[string]any, bool) {
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, false
	}
	return m, true
}

func intFieldInRange(sub map[string]any, key string, min, max int) (int, error) {
	raw := configString(sub, key, "")
	if raw == "" {
		return 0, fmt.Errorf("%s is required", key)
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("must be an integer (got %q)", raw)
	}
	if n < min || n > max {
		return 0, fmt.Errorf("must be between %d and %d (got %d)", min, max, n)
	}
	return n, nil
}

func validateAutoscaleUniformSizes(node DeploymentNode, ac *AutoscaleConfig) error {
	if ac == nil {
		return nil
	}
	sizes := instanceSizesFromConfig(node.Config, int(ac.Max))
	if len(sizeGroups(sizes)) > 1 {
		return fmt.Errorf("autoscale requires the same instance size on every replica")
	}
	return nil
}

// deployReplicaCount is the initial Deployment size: autoscale.min when set, else config.replicas.
func deployReplicaCount(config map[string]any, defaultVal int) (int32, error) {
	ac, err := parseAutoscaleConfig(config)
	if err != nil {
		return 0, err
	}
	if ac != nil {
		return ac.Min, nil
	}
	return parseReplicaCount(config, defaultVal), nil
}

func effectiveAppServerReplicasForTier(node DeploymentNode) (int32, error) {
	ac, err := parseAutoscaleConfig(node.Config)
	if err != nil {
		return 0, err
	}
	if ac != nil {
		return ac.Max, nil
	}
	return replicaCountFromConfig(node.Config, 1, minAppServerReplicas, maxAppServerReplicas)
}

func cpuUtilizationHPA(namespace, deploymentName string, ac AutoscaleConfig) *autoscalingv2.HorizontalPodAutoscaler {
	target := ac.TargetCPUPercent
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentName,
			Namespace: namespace,
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       deploymentName,
			},
			MinReplicas: ptr(ac.Min),
			MaxReplicas: ac.Max,
			Metrics: []autoscalingv2.MetricSpec{
				{
					Type: autoscalingv2.ResourceMetricSourceType,
					Resource: &autoscalingv2.ResourceMetricSource{
						Name: "cpu",
						Target: autoscalingv2.MetricTarget{
							Type:               autoscalingv2.UtilizationMetricType,
							AverageUtilization: ptr(target),
						},
					},
				},
			},
		},
	}
}

func ensureCPUHorizontalPodAutoscaler(ctx context.Context, clientset kubernetes.Interface, namespace, deploymentName string, ac AutoscaleConfig) error {
	hpa := cpuUtilizationHPA(namespace, deploymentName, ac)
	_, err := clientset.AutoscalingV2().HorizontalPodAutoscalers(namespace).Create(ctx, hpa, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create horizontal pod autoscaler %s: %w", deploymentName, err)
	}
	return nil
}
