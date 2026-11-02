package deployer

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	maxReplicaApplyDelayMs = 5000
	// WarningCodeReplicaEqualApplyDelay is emitted when every standby gets the same apply delay.
	WarningCodeReplicaEqualApplyDelay = "replica_equal_apply_delay"
)

// ParseReplicaApplyDelayMs reads replicaApplyDelayMs for a db node config.
// Missing or empty means every standby is 0 ms. A scalar applies to each standby.
// An array must have length readReplicas when readReplicas > 0.
func ParseReplicaApplyDelayMs(config map[string]any, readReplicas int) ([]int, error) {
	if readReplicas < 0 {
		return nil, fmt.Errorf("replicaApplyDelayMs: readReplicas must be >= 0")
	}
	if readReplicas == 0 {
		return nil, nil
	}
	raw, ok := config["replicaApplyDelayMs"]
	if !ok || raw == nil {
		return zeroDelays(readReplicas), nil
	}
	values, err := parseReplicaApplyDelayRaw(raw)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return zeroDelays(readReplicas), nil
	}
	// A JSON number or a single comma-segment string applies to every standby.
	// An array is per-standby and must match readReplicas, including length 1.
	if len(values) == 1 && !isReplicaApplyDelayArray(raw) {
		return expandDelay(values[0], readReplicas), nil
	}
	if len(values) != readReplicas {
		return nil, fmt.Errorf(
			"replicaApplyDelayMs length must equal readReplicas (%d, got %d)",
			readReplicas, len(values),
		)
	}
	return values, nil
}

// ValidateReplicaApplyDelay rejects delays the db node's replication settings cannot honour.
// Deploy validation and the live replica-lag update share it.
func ValidateReplicaApplyDelay(config map[string]any, delaysMs []int) error {
	if !HasPositiveReplicaApplyDelay(delaysMs) {
		return nil
	}
	delayer, err := LookupReplicaDelayer(dbEngineFromConfig(config))
	if err != nil {
		return err
	}
	return delayer.Validate(config, delaysMs)
}

// ReplicaApplyDelayUniform reports whether every standby has the same delay.
func ReplicaApplyDelayUniform(delays []int) bool {
	if len(delays) <= 1 {
		return true
	}
	first := delays[0]
	for _, ms := range delays[1:] {
		if ms != first {
			return false
		}
	}
	return true
}

// HasPositiveReplicaApplyDelay reports whether any standby delay is above zero.
func HasPositiveReplicaApplyDelay(delays []int) bool {
	for _, ms := range delays {
		if ms > 0 {
			return true
		}
	}
	return false
}

func isReplicaApplyDelayArray(raw any) bool {
	switch raw.(type) {
	case []any, []int:
		return true
	default:
		return false
	}
}

func zeroDelays(n int) []int {
	return make([]int, n)
}

func expandDelay(ms int, n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = ms
	}
	return out
}

func parseReplicaApplyDelayRaw(raw any) ([]int, error) {
	switch v := raw.(type) {
	case float64:
		ms, err := delayFromFloat(v)
		if err != nil {
			return nil, err
		}
		return []int{ms}, nil
	case int:
		ms, err := validateDelayMs(v)
		if err != nil {
			return nil, err
		}
		return []int{ms}, nil
	case int32:
		ms, err := validateDelayMs(int(v))
		if err != nil {
			return nil, err
		}
		return []int{ms}, nil
	case int64:
		ms, err := validateDelayMs(int(v))
		if err != nil {
			return nil, err
		}
		return []int{ms}, nil
	case string:
		return parseReplicaApplyDelayString(v)
	case []any:
		out := make([]int, 0, len(v))
		for i, item := range v {
			ms, err := delayFromJSONNumber(item)
			if err != nil {
				return nil, fmt.Errorf("replicaApplyDelayMs[%d]: %w", i, err)
			}
			out = append(out, ms)
		}
		return out, nil
	case []int:
		out := make([]int, len(v))
		for i, ms := range v {
			valid, err := validateDelayMs(ms)
			if err != nil {
				return nil, fmt.Errorf("replicaApplyDelayMs[%d]: %w", i, err)
			}
			out[i] = valid
		}
		return out, nil
	default:
		return nil, fmt.Errorf("replicaApplyDelayMs must be a number, string, or array (got %T)", raw)
	}
}

func parseReplicaApplyDelayString(s string) ([]int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	parts := strings.Split(s, ",")
	out := make([]int, 0, len(parts))
	for i, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("replicaApplyDelayMs: empty segment at index %d", i)
		}
		n, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("replicaApplyDelayMs: segment %q must be an integer", part)
		}
		ms, err := validateDelayMs(n)
		if err != nil {
			return nil, err
		}
		out = append(out, ms)
	}
	return out, nil
}

func delayFromJSONNumber(v any) (int, error) {
	switch n := v.(type) {
	case float64:
		return delayFromFloat(n)
	case int:
		return validateDelayMs(n)
	case int32:
		return validateDelayMs(int(n))
	case int64:
		return validateDelayMs(int(n))
	default:
		return 0, fmt.Errorf("must be an integer (got %T)", v)
	}
}

func delayFromFloat(f float64) (int, error) {
	if math.Trunc(f) != f {
		return 0, fmt.Errorf("must be a whole number of milliseconds (got %v)", f)
	}
	return validateDelayMs(int(f))
}

func validateDelayMs(ms int) (int, error) {
	if ms < 0 || ms > maxReplicaApplyDelayMs {
		return 0, fmt.Errorf("must be between 0 and %d (got %d)", maxReplicaApplyDelayMs, ms)
	}
	return ms, nil
}
