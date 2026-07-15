package deployer

import (
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// redisEvictionPolicies are Redis maxmemory-policy values we accept from the canvas.
var redisEvictionPolicies = map[string]struct{}{
	"noeviction":      {},
	"allkeys-lru":     {},
	"volatile-lru":    {},
	"allkeys-lfu":     {},
	"volatile-lfu":    {},
	"allkeys-random":  {},
	"volatile-random": {},
	"volatile-ttl":    {},
}

// normalizeRedisEvictionPolicy returns a known Redis maxmemory-policy, defaulting to allkeys-lru.
func normalizeRedisEvictionPolicy(policy string) string {
	p := strings.ToLower(strings.TrimSpace(policy))
	if _, ok := redisEvictionPolicies[p]; ok {
		return p
	}
	return "allkeys-lru"
}

// redisServerArgs builds redis-server flags from canvas evictionPolicy and memory limits.
// maxmemory is 90% of the container memory limit so Redis has headroom under the cgroup.
// Args start with "--" so the official redis image entrypoint prepends redis-server.
func redisServerArgs(config map[string]interface{}, resources corev1.ResourceRequirements) []string {
	policy := normalizeRedisEvictionPolicy(configString(config, "evictionPolicy", "allkeys-lru"))
	args := []string{"--maxmemory-policy", policy}

	mem := resources.Limits.Memory()
	if mem == nil || mem.IsZero() {
		return args
	}
	bytes := mem.Value() * 90 / 100
	if bytes <= 0 {
		return args
	}
	return append([]string{"--maxmemory", strconv.FormatInt(bytes, 10)}, args...)
}
