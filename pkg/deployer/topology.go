package deployer

import (
	"fmt"
	"strconv"
)

func requiresProxy(cacheNode DeploymentNode) bool {
	strategy := configString(cacheNode.Config, "strategy", "cache-aside")
	switch strategy {
	case "write-through", "write-behind", "read-through":
		return true
	default:
		return false
	}
}

// firstNonProxyCacheFromApp returns the first cache node reachable from appServerID
// that is not a proxy strategy (direct Redis / cache-aside path).
func firstNonProxyCacheFromApp(appServerID string, design DeploymentDesign, nodeMap map[string]DeploymentNode) (DeploymentNode, bool) {
	for _, conn := range design.Connections {
		if conn.SourceID != appServerID {
			continue
		}
		target, ok := nodeMap[conn.TargetID]
		if !ok || target.Type != "cache" {
			continue
		}
		if !requiresProxy(target) {
			return target, true
		}
	}
	return DeploymentNode{}, false
}

// ValidateTopology checks that the canvas graph matches the expectations
// of the chosen caching strategy.
func ValidateTopology(design DeploymentDesign) error {
	nodeMap := make(map[string]DeploymentNode, len(design.Nodes))
	for _, n := range design.Nodes {
		nodeMap[n.ID] = n
	}

	for _, cacheNode := range design.Nodes {
		if cacheNode.Type != "cache" {
			continue
		}
		if !requiresProxy(cacheNode) {
			continue
		}

		// Proxy strategies require cache -> db on the canvas.
		hasDBConn := false
		for _, conn := range design.Connections {
			if conn.SourceID != cacheNode.ID {
				continue
			}
			target, ok := nodeMap[conn.TargetID]
			if !ok {
				continue
			}
			if target.Type == "db" {
				hasDBConn = true
				break
			}
		}

		if !hasDBConn {
			strategy := configString(cacheNode.Config, "strategy", "cache-aside")
			return fmt.Errorf(
				"cache node %q has strategy %q but is not connected to a db node",
				cacheNode.Name,
				strategy,
			)
		}
	}

	// Proxy topologies: app-server must not also connect directly to db (ambiguous / wrong model).
	for _, appNode := range design.Nodes {
		if appNode.Type != "app-server" {
			continue
		}
		var hasProxyCache, hasDirectDB bool
		for _, conn := range design.Connections {
			if conn.SourceID != appNode.ID {
				continue
			}
			target, ok := nodeMap[conn.TargetID]
			if !ok {
				continue
			}
			switch target.Type {
			case "db":
				hasDirectDB = true
			case "cache":
				if requiresProxy(target) {
					hasProxyCache = true
				}
			}
		}
		if hasProxyCache && hasDirectDB {
			return fmt.Errorf(
				"app-server %q must not connect directly to a db node when using a proxy cache strategy; remove the app-server -> db connection",
				appNode.Name,
			)
		}
	}

	return nil
}

// DeriveEnvVars derives environment variables for a node based on the full deployment design.
// env vars are how the user's topology becomes runtime behaviour.
// The deployer translates connections to env vars; the image contains the conditional logic.
// Adding a new component type = add one case here. Nothing else changes.
func DeriveEnvVars(node DeploymentNode, design DeploymentDesign) map[string]string {
	env := map[string]string{}

	nodeMap := make(map[string]DeploymentNode, len(design.Nodes))
	for _, n := range design.Nodes {
		nodeMap[n.ID] = n
	}

	isAppServer := node.Type == "app-server"
	isCache := node.Type == "cache"

	// Proxy routing detection for the app-server:
	// if the app-server connects to a cache node configured for proxy strategies,
	// the app-server must be DB-free (no DB_HOST/creds).
	hasCacheConn := false
	hasProxyCacheConn := false
	if isAppServer {
		for _, conn := range design.Connections {
			if conn.SourceID != node.ID {
				continue
			}
			target, ok := nodeMap[conn.TargetID]
			if !ok {
				continue
			}
			if target.Type == "cache" {
				hasCacheConn = true
				if requiresProxy(target) {
					hasProxyCacheConn = true
				}
			}
		}
	}

	cacheProxy := isCache && requiresProxy(node)

	// Step 2 — walk outbound connections from this node.
	for _, conn := range design.Connections {
		if conn.SourceID != node.ID {
			continue
		}

		target, ok := nodeMap[conn.TargetID]
		if !ok {
			continue
		}

		switch target.Type {
		case "db":
			// In proxy topologies, the app-server must not receive DB credentials.
			if isAppServer && hasProxyCacheConn {
				continue
			}
			// For cache nodes, only proxy strategies need DB credentials
			// (keeps cache-aside cache nodes "dumb").
			if isCache && !cacheProxy {
				continue
			}

			env["DB_HOST"] = "db"
			env["DB_PORT"] = "5432"
			env["DB_NAME"] = "syscrack"
			env["DB_USER"] = "syscrack"
			env["DB_PASSWORD"] = "syscrack"
		case "cache":
			// We only derive cache routing env vars for the app-server.
			// Cache-aside or proxy strategies determine whether the app-server
			// talks directly to Redis vs via caching-proxy HTTP.
			if !isAppServer {
				continue
			}

			env["CACHE_HOST"] = "cache"
			if hasProxyCacheConn {
				// Proxy topology: app-server -> caching-proxy
				env["CACHE_MODE"] = "proxy"
				// Intentionally do not set CACHE_PORT; the url-shortener in proxy
				// mode uses the proxy's fixed HTTP port (8081).
			} else {
				// Cache-aside topology: app-server -> Redis
				env["CACHE_PORT"] = "6379"
			}
		case "mq":
			env["MQ_HOST"] = "mq"
			env["MQ_PORT"] = "5672"
		}
	}

	// Step 3 — node-local behaviour flags from config.
	switch node.Type {
	case "app-server":
		if hasCacheConn && !hasProxyCacheConn {
			// CACHE_STRATEGY for redis mode comes from the connected cache-aside cache node, not the app-server.
			if cacheNode, ok := firstNonProxyCacheFromApp(node.ID, design, nodeMap); ok {
				env["CACHE_STRATEGY"] = configString(cacheNode.Config, "cacheStrategy", "cache-aside")
			} else {
				env["CACHE_STRATEGY"] = "cache-aside"
			}
			env["CACHE_TTL"] = configString(node.Config, "cacheTtlSeconds", "300")
			// WRITE_BEHIND_INTERVAL is only for caching-proxy (canonical write-behind), not url-shortener.
			env["STAMPEDE_PROTECTION"] = configString(node.Config, "stampedeProtection", "false")
		}
	case "db":
		env["DB_ENGINE"] = configString(node.Config, "engine", "postgresql")
		env["DB_REPLICATION"] = configString(node.Config, "replicationMode", "none")
	case "cache":
		if cacheProxy {
			env["STRATEGY"] = configString(node.Config, "strategy", "write-through")
			env["CACHE_TTL"] = configString(node.Config, "cacheTtlSeconds", "300")
			env["WRITE_BEHIND_INTERVAL"] = configString(node.Config, "writeBehindIntervalSeconds", "30")
		}
	}

	return env
}

func configString(config map[string]any, key, defaultVal string) string {
	if config == nil {
		return defaultVal
	}

	val, ok := config[key]
	if !ok || val == nil {
		return defaultVal
	}

	switch v := val.(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	default:
		return defaultVal
	}
}
