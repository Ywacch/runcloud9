package deployer

import (
	"fmt"
	"strconv"
)

// parseReplicaCount reads config.replicas, returning defaultVal when unset or invalid.
func parseReplicaCount(config map[string]any, defaultVal int) int32 {
	raw := configString(config, "replicas", strconv.Itoa(defaultVal))
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return int32(defaultVal)
	}
	return int32(n)
}

// replicaCountFromConfig parses config.replicas and enforces [min, max].
func replicaCountFromConfig(config map[string]any, defaultVal, min, max int) (int32, error) {
	raw := configString(config, "replicas", strconv.Itoa(defaultVal))
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("replicas must be an integer (got %q)", raw)
	}
	if n < min || n > max {
		return 0, fmt.Errorf("replicas must be between %d and %d (got %q)", min, max, raw)
	}
	return int32(n), nil
}
