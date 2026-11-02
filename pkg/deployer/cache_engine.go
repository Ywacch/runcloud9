package deployer

import "strings"

// CacheEngineFromNode returns the cache node engine id (default redis).
func CacheEngineFromNode(node DeploymentNode) string {
	return cacheEngineFromConfig(node.Config)
}

// cacheEngineFromConfig returns the normalized cache node engine id (default redis).
func cacheEngineFromConfig(config map[string]interface{}) string {
	raw := strings.ToLower(strings.TrimSpace(configString(config, "engine", "redis")))
	if raw == "" {
		return "redis"
	}
	return raw
}
