package deployer

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	minReadReplicas = 0
	maxReadReplicas = 2

	readConsistencyLeaderOnly   = "leader-only"
	readConsistencyReplica      = "replica"
	readConsistencySticky       = "sticky"
	readConsistencyRYW          = "ryw"
	readConsistencyMonotonic    = "monotonic"
	readConsistencyRYWMonotonic = "ryw+monotonic"

	replicationModeAsync    = "async"
	replicationModeSemiSync = "semi-sync"
	replicationModeSync     = "sync"

	syncCommitRemoteWrite = "remote_write"
	syncCommitOn          = "on"
	syncCommitRemoteApply = "remote_apply"
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

// ParseReadConsistency reads config.readConsistency, defaulting to leader-only when unset.
func ParseReadConsistency(config map[string]any) string {
	return parseReadConsistency(config)
}

// parseReadConsistency reads config.readConsistency, defaulting to leader-only when unset.
func parseReadConsistency(config map[string]any) string {
	raw := strings.TrimSpace(strings.ToLower(configString(config, "readConsistency", readConsistencyLeaderOnly)))
	if raw == "" {
		return readConsistencyLeaderOnly
	}
	return raw
}

func readConsistencyEnumList() string {
	return "leader-only, replica, sticky, ryw, monotonic, ryw+monotonic"
}

func isValidReadConsistency(policy string) bool {
	switch policy {
	case readConsistencyLeaderOnly, readConsistencyReplica, readConsistencySticky,
		readConsistencyRYW, readConsistencyMonotonic, readConsistencyRYWMonotonic:
		return true
	default:
		return false
	}
}

// readConsistencyFromConfig parses config.readConsistency and validates the enum.
func readConsistencyFromConfig(config map[string]any) (string, error) {
	policy := parseReadConsistency(config)
	if !isValidReadConsistency(policy) {
		return "", fmt.Errorf("readConsistency must be one of %s (got %q)", readConsistencyEnumList(), policy)
	}
	return policy, nil
}

func requiresMultiReplicaReadConsistency(policy string) bool {
	return policy == readConsistencyMonotonic || policy == readConsistencyRYWMonotonic
}

// effectiveReadConsistency returns the deploy-time policy after bounds checks.
// monotonic and ryw+monotonic clamp to ryw when readReplicas < 2.
func effectiveReadConsistency(config map[string]any, readReplicas int32) string {
	policy, err := readConsistencyFromConfig(config)
	if err != nil {
		return readConsistencyLeaderOnly
	}
	if readReplicas < 2 && requiresMultiReplicaReadConsistency(policy) {
		return readConsistencyRYW
	}
	return policy
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

// parseReplicationMode normalizes config.replicationMode to async, semi-sync, or sync.
// Empty, none, and legacy read-replica map to async. Unknown values return an error.
func parseReplicationMode(config map[string]any) (string, error) {
	raw := strings.TrimSpace(strings.ToLower(configString(config, "replicationMode", "")))
	switch raw {
	case "", "none", "async", "asynchronous", "read-replica":
		return replicationModeAsync, nil
	case "semi-sync", "semi_sync", "semisync":
		return replicationModeSemiSync, nil
	case "sync", "synchronous":
		return replicationModeSync, nil
	default:
		return "", fmt.Errorf("replicationMode must be async, semi-sync, or sync (got %q)", raw)
	}
}

func syncCommitLevelEnumList() string {
	return "remote_write, on, remote_apply"
}

func isValidSyncCommitLevel(level string) bool {
	switch level {
	case syncCommitRemoteWrite, syncCommitOn, syncCommitRemoteApply:
		return true
	default:
		return false
	}
}

// parseSyncCommitLevel reads config.syncCommitLevel, defaulting to on when unset.
func parseSyncCommitLevel(config map[string]any) string {
	level, err := syncCommitLevelFromConfig(config)
	if err != nil {
		return syncCommitOn
	}
	return level
}

// syncCommitLevelFromConfig parses and validates config.syncCommitLevel.
func syncCommitLevelFromConfig(config map[string]any) (string, error) {
	raw := strings.TrimSpace(strings.ToLower(configString(config, "syncCommitLevel", "")))
	if raw == "" {
		return syncCommitOn, nil
	}
	if !isValidSyncCommitLevel(raw) {
		return "", fmt.Errorf("syncCommitLevel must be one of %s (got %q)", syncCommitLevelEnumList(), raw)
	}
	return raw, nil
}

// requiresSynchronousReplication reports whether mode waits on standbys at commit time.
func requiresSynchronousReplication(mode string) bool {
	return mode == replicationModeSemiSync || mode == replicationModeSync
}

// seedReplicaReadyBeforeRestore returns how many read-replica pods must be Ready
// before pg_restore may run: 0 = seed immediately after primary Ready (async),
// 1 = semi-sync (ANY 1), replicaCount = sync (all standbys).
func seedReplicaReadyBeforeRestore(mode string, replicaCount int32) int32 {
	if replicaCount <= 0 {
		return 0
	}
	switch mode {
	case "async":
		return 0
	case "semi-sync":
		return 1
	case "sync":
		return replicaCount
	default:
		return replicaCount
	}
}

// DBEngineSupportsStreamingReplicas reports whether the db engine can use physical streaming replicas.
func DBEngineSupportsStreamingReplicas(engine string) bool {
	return dbEngineSupportsStreamingReplicas(engine)
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
