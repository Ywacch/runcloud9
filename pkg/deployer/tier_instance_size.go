package deployer

import "fmt"

func instanceSizeRank(size string) int {
	switch normalizeInstanceSize(size) {
	case "medium":
		return 1
	case "large":
		return 2
	default:
		return 0
	}
}

func instanceSizeExceedsMax(actual, max string) bool {
	maxRank := instanceSizeRank(max)
	if maxRank == 0 {
		return false
	}
	return instanceSizeRank(actual) > maxRank
}

func collectConfigSizes(config map[string]interface{}) []string {
	if config == nil {
		return nil
	}
	var sizes []string
	if v := configString(config, "instanceSize", ""); v != "" {
		sizes = append(sizes, v)
	}
	if v := configString(config, "resourceProfile", ""); v != "" {
		sizes = append(sizes, v)
	}
	if v := configString(config, "proxyInstanceSize", ""); v != "" {
		sizes = append(sizes, v)
	}
	if v := configString(config, "proxyResourceProfile", ""); v != "" {
		sizes = append(sizes, v)
	}
	raw, ok := config["instanceSizes"]
	if !ok || raw == nil {
		return sizes
	}
	switch arr := raw.(type) {
	case []any:
		for _, item := range arr {
			sizes = append(sizes, fmt.Sprint(item))
		}
	case []string:
		sizes = append(sizes, arr...)
	}
	return sizes
}

// ValidateInstanceSizeForTier rejects designs with sizing above the tier ceiling.
func ValidateInstanceSizeForTier(design DeploymentDesign, maxSize string) error {
	maxSize = normalizeInstanceSize(maxSize)
	if maxSize == "" || maxSize == "large" {
		return nil
	}
	for _, node := range design.Nodes {
		if !isDeployableNodeType(node.Type) {
			continue
		}
		for _, raw := range collectConfigSizes(node.Config) {
			actual := normalizeInstanceSize(raw)
			if instanceSizeExceedsMax(actual, maxSize) {
				return fmt.Errorf(
					"instance size %s exceeds your plan limit of %s",
					actual,
					maxSize,
				)
			}
		}
	}
	return nil
}
