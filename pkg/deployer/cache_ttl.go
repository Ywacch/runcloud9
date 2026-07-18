package deployer

import (
	"strconv"
	"strings"
)

// maxCacheTTLSeconds caps timed cache expiry at 24 hours. The frontend
// inspector enforces the same bound; values outside it are treated as unset
// so the image's template default applies instead of a surprising clamp.
const maxCacheTTLSeconds = 86400

// normalizeCacheTTLSeconds validates a design-config cacheTtlSeconds value.
// The CACHE_TTL contract shared with every image is: "0" = no expiry,
// "1".."86400" = timed expiry in seconds. Negative, oversized, or malformed
// values report ok=false so the caller omits CACHE_TTL entirely and the image
// falls back to its template default.
func normalizeCacheTTLSeconds(config map[string]any) (string, bool) {
	raw := strings.TrimSpace(configString(config, "cacheTtlSeconds", ""))
	if raw == "" {
		return "", false
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 || n > maxCacheTTLSeconds {
		return "", false
	}
	return strconv.Itoa(n), true
}

// cacheAsideTTL resolves the effective CACHE_TTL for an app-server that talks
// directly to Redis. The cache node is the single design owner of
// cacheTtlSeconds; the app node is consulted only as a legacy fallback for
// designs saved before ownership moved to the cache node.
func cacheAsideTTL(appNode DeploymentNode, design DeploymentDesign, nodeMap map[string]DeploymentNode) (string, bool) {
	if cacheNode, ok := firstNonProxyCacheFromApp(appNode.ID, design, nodeMap); ok {
		if ttl, valid := normalizeCacheTTLSeconds(cacheNode.Config); valid {
			return ttl, true
		}
	}
	return normalizeCacheTTLSeconds(appNode.Config)
}

// firstProxyCacheFromApp returns the first cache node reachable from
// appServerID that uses a proxy strategy (write-through / write-behind).
// Counterpart of firstNonProxyCacheFromApp.
func firstProxyCacheFromApp(appServerID string, design DeploymentDesign, nodeMap map[string]DeploymentNode) (DeploymentNode, bool) {
	for _, conn := range design.Connections {
		if conn.SourceID != appServerID {
			continue
		}
		target, ok := nodeMap[conn.TargetID]
		if !ok || target.Type != "cache" {
			continue
		}
		if requiresProxy(target) {
			return target, true
		}
	}
	return DeploymentNode{}, false
}
