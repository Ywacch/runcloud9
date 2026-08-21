package deployer

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	minReadReplicas = 0
	maxReadReplicas = 2
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

// ParseReadReplicaCount reads config.readReplicas, defaulting to 0 when unset or invalid.
func ParseReadReplicaCount(config map[string]any) int32 {
	return parseReadReplicaCount(config)
}

// parseReadReplicaCount reads config.readReplicas, defaulting to 0 when unset or invalid.
func parseReadReplicaCount(config map[string]any) int32 {
	raw := configString(config, "readReplicas", "0")
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0
	}
	return int32(n)
}

// readReplicaCountFromConfig parses config.readReplicas and enforces [min, max].
func readReplicaCountFromConfig(config map[string]any, min, max int) (int32, error) {
	raw := configString(config, "readReplicas", "0")
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("readReplicas must be an integer (got %q)", raw)
	}
	if n < min || n > max {
		return 0, fmt.Errorf("readReplicas must be between %d and %d (got %q)", min, max, raw)
	}
	return int32(n), nil
}

// parseReplicationMode normalizes config.replicationMode to async or sync.
// Empty, none, and legacy read-replica map to async. Unknown values return an error.
func parseReplicationMode(config map[string]any) (string, error) {
	raw := strings.TrimSpace(strings.ToLower(configString(config, "replicationMode", "")))
	switch raw {
	case "", "none", "async", "asynchronous", "read-replica":
		return "async", nil
	case "sync", "synchronous":
		return "sync", nil
	default:
		return "", fmt.Errorf("replicationMode must be async or sync (got %q)", raw)
	}
}

// dbEngineSupportsStreamingReplicas reports whether the db engine can use physical streaming replicas.
func dbEngineSupportsStreamingReplicas(engine string) bool {
	switch strings.ToLower(strings.TrimSpace(engine)) {
	case "", "postgresql", "postgres":
		return true
	default:
		return false
	}
}

// dbEngineFromConfig returns the normalized db engine name from node config.
func dbEngineFromConfig(config map[string]any) string {
	return strings.ToLower(strings.TrimSpace(configString(config, "engine", "postgresql")))
}
