package readrouter

import "strings"

// PolicyFromEnv maps READ_CONSISTENCY env values to a routing policy.
func PolicyFromEnv(raw string) RoutingPolicy {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case string(PolicyReplica):
		return PolicyReplica
	case string(PolicySticky):
		return PolicySticky
	case string(PolicyRYW):
		return PolicyRYW
	case string(PolicyMonotonic):
		return PolicyMonotonic
	case string(PolicyRYWMonotonic):
		return PolicyRYWMonotonic
	default:
		return PolicyLeaderOnly
	}
}
