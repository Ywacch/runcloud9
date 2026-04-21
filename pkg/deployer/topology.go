package deployer

import (
	"fmt"
	"sort"
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

// DisplayCacheStrategyForApp returns the cache strategy label for UI: the first app->cache
// edge's target config (proxy strategies use the cache node's "strategy"; Redis path uses
// cacheRedisStrategyForApp). Empty string if the app has no outbound cache connection.
func DisplayCacheStrategyForApp(appServerID string, design DeploymentDesign) string {
	nodeMap := make(map[string]DeploymentNode, len(design.Nodes))
	for _, n := range design.Nodes {
		nodeMap[n.ID] = n
	}
	for _, conn := range design.Connections {
		if conn.SourceID != appServerID {
			continue
		}
		target, ok := nodeMap[conn.TargetID]
		if !ok || target.Type != "cache" {
			continue
		}
		if requiresProxy(target) {
			return configString(target.Config, "strategy", "write-through")
		}
		return cacheRedisStrategyForApp(target)
	}
	return ""
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

	for _, workerNode := range design.Nodes {
		if workerNode.Type != "worker" {
			continue
		}
		hasDBDep := false
		hasMQDep := false
		hasInboundMQ := false
		for _, conn := range design.Connections {
			if conn.SourceID == workerNode.ID {
				target, ok := nodeMap[conn.TargetID]
				if !ok {
					continue
				}
				if target.Type == "db" {
					hasDBDep = true
				}
				if target.Type == "mq" {
					hasMQDep = true
				}
			}
			if conn.TargetID == workerNode.ID {
				src, ok := nodeMap[conn.SourceID]
				if ok && src.Type == "mq" {
					hasInboundMQ = true
				}
			}
		}
		if !hasDBDep {
			return fmt.Errorf(`worker %q must connect to a db node (add worker -> db edge)`, workerNode.Name)
		}
		if !hasMQDep {
			if hasInboundMQ {
				return fmt.Errorf(`invalid worker/mq edge direction for worker %q: found mq -> worker; use worker -> mq`, workerNode.Name)
			}
			return fmt.Errorf(`worker %q must connect to an mq node (add worker -> mq edge)`, workerNode.Name)
		}
	}

	// UNDERSTAND: DeploymentConn uses source -> target where the source DEPENDS ON the target.
	// Kahn's algorithm yields dependency-graph sources first, so we reverse the result to ensure
	// infrastructure (targets) deploys before application (sources). DeploymentOrder also performs
	// cycle detection for us.
	if _, err := DeploymentOrder(design); err != nil {
		return err
	}

	return nil
}

// DeploymentOrder returns nodes in dependency order (infra before app).
// Uses Kahn's algorithm with stable tie-breaking; reverses the final result.
// Returns an error if a cycle exists.
func DeploymentOrder(design DeploymentDesign) ([]DeploymentNode, error) {
	inDeg := make(map[string]int, len(design.Nodes))
	adj := make(map[string][]string, len(design.Nodes))
	nodeMap := make(map[string]DeploymentNode, len(design.Nodes))

	for _, n := range design.Nodes {
		inDeg[n.ID] = 0
		nodeMap[n.ID] = n
	}

	for _, c := range design.Connections {
		if _, ok := nodeMap[c.SourceID]; !ok {
			return nil, fmt.Errorf("unknown node id in connections sourceId=%q", c.SourceID)
		}
		if _, ok := nodeMap[c.TargetID]; !ok {
			return nil, fmt.Errorf("unknown node id in connections targetId=%q", c.TargetID)
		}

		adj[c.SourceID] = append(adj[c.SourceID], c.TargetID)
		// Edge source -> target means "source depends on target".
		// Kahn's algorithm counts incoming edges, so dependency nodes accumulate inDeg.
		inDeg[c.TargetID]++
	}

	// Stable traversal: sort neighbor lists up front.
	for k := range adj {
		sort.Strings(adj[k])
	}

	// Queue initial nodes with in-degree 0 (no dependencies).
	queue := make([]string, 0, len(inDeg))
	for id, deg := range inDeg {
		if deg == 0 {
			queue = append(queue, id)
		}
	}
	sort.Strings(queue)

	ordered := make([]DeploymentNode, 0, len(design.Nodes))
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]

		ordered = append(ordered, nodeMap[id])

		for _, nid := range adj[id] {
			inDeg[nid]--
			if inDeg[nid] == 0 {
				queue = append(queue, nid)
				sort.Strings(queue)
			}
		}
	}

	if len(ordered) != len(design.Nodes) {
		return nil, fmt.Errorf("topology has a cycle — cannot determine deployment order")
	}

	// Reverse: Kahn's gives sources-first; we need infra-first.
	for i, j := 0, len(ordered)-1; i < j; i, j = i+1, j-1 {
		ordered[i], ordered[j] = ordered[j], ordered[i]
	}

	return ordered, nil
}

// DeploymentLayers returns nodes grouped by dependency depth for parallel deploy.
// Nodes in the same layer have no dependencies on each other; layers run sequentially,
// and nodes within a layer may run concurrently. Layer order matches DeploymentOrder
// (infrastructure before dependents). Uses the same edge semantics as DeploymentOrder.
func DeploymentLayers(design DeploymentDesign) ([][]DeploymentNode, error) {
	inDeg := make(map[string]int, len(design.Nodes))
	adj := make(map[string][]string, len(design.Nodes))
	nodeMap := make(map[string]DeploymentNode, len(design.Nodes))

	for _, n := range design.Nodes {
		inDeg[n.ID] = 0
		nodeMap[n.ID] = n
	}

	for _, c := range design.Connections {
		if _, ok := nodeMap[c.SourceID]; !ok {
			return nil, fmt.Errorf("unknown node id in connections sourceId=%q", c.SourceID)
		}
		if _, ok := nodeMap[c.TargetID]; !ok {
			return nil, fmt.Errorf("unknown node id in connections targetId=%q", c.TargetID)
		}

		adj[c.SourceID] = append(adj[c.SourceID], c.TargetID)
		inDeg[c.TargetID]++
	}

	for k := range adj {
		sort.Strings(adj[k])
	}

	queue := make([]string, 0, len(inDeg))
	for id, deg := range inDeg {
		if deg == 0 {
			queue = append(queue, id)
		}
	}
	sort.Strings(queue)

	var layers [][]DeploymentNode
	for len(queue) > 0 {
		layerIDs := queue
		queue = nil

		layer := make([]DeploymentNode, 0, len(layerIDs))
		for _, id := range layerIDs {
			layer = append(layer, nodeMap[id])
			for _, nid := range adj[id] {
				inDeg[nid]--
				if inDeg[nid] == 0 {
					queue = append(queue, nid)
				}
			}
		}
		sort.Strings(queue)
		layers = append(layers, layer)
	}

	var total int
	for _, L := range layers {
		total += len(L)
	}
	if total != len(design.Nodes) {
		return nil, fmt.Errorf("topology has a cycle — cannot determine deployment order")
	}

	for i, j := 0, len(layers)-1; i < j; i, j = i+1, j-1 {
		layers[i], layers[j] = layers[j], layers[i]
	}

	ordered, oerr := DeploymentOrder(design)
	if oerr != nil {
		return nil, oerr
	}
	rank := make(map[string]int, len(ordered))
	for i, n := range ordered {
		rank[n.ID] = i
	}
	for i := range layers {
		sort.Slice(layers[i], func(a, b int) bool {
			return rank[layers[i][a].ID] < rank[layers[i][b].ID]
		})
	}

	return layers, nil
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

// cacheRedisStrategyForApp is the label shown in UI for app -> Redis (non-proxy) paths.
// Prefer canvas key "strategy", then legacy "cacheStrategy", else cache-aside.
// (DeriveEnvVars sets CACHE_STRATEGY from cacheStrategy only — see tests.)
func cacheRedisStrategyForApp(cacheNode DeploymentNode) string {
	if s := configString(cacheNode.Config, "strategy", ""); s != "" {
		return s
	}
	if s := configString(cacheNode.Config, "cacheStrategy", ""); s != "" {
		return s
	}
	return "cache-aside"
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
