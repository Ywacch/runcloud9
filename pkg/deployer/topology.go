package deployer

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

func requiresProxy(cacheNode DeploymentNode) bool {
	return RequiresProxy(cacheNode)
}

// CanonicalCacheStrategy folds legacy selectable names into teaching-facing strategies.
// "read-through" is not a distinct write strategy: proxy GET miss-fill (DB → Redis) runs for
// all proxy modes. Legacy canvases that still store strategy=read-through deploy as write-through.
func CanonicalCacheStrategy(strategy string) string {
	s := strings.TrimSpace(strategy)
	if strings.EqualFold(s, "read-through") {
		return "write-through"
	}
	return s
}

// cacheNodeStrategy returns the effective cache strategy for a node (canonicalized).
func cacheNodeStrategy(cacheNode DeploymentNode) string {
	s := configString(cacheNode.Config, "strategy", "")
	if s == "" {
		s = configString(cacheNode.Config, "cacheStrategy", "")
	}
	if s == "" {
		return "cache-aside"
	}
	return CanonicalCacheStrategy(s)
}

// RequiresProxy reports whether the cache node uses a proxy strategy (write-through, write-behind).
// Legacy strategy "read-through" is treated as write-through (still proxy).
func RequiresProxy(cacheNode DeploymentNode) bool {
	switch cacheNodeStrategy(cacheNode) {
	case "write-through", "write-behind":
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
			return cacheNodeStrategy(target)
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
	if err := validatePlatformSingletons(design); err != nil {
		return err
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
			strategy := cacheNodeStrategy(cacheNode)
			return fmt.Errorf(
				"cache node %q has strategy %q but is not connected to a db node",
				cacheNode.Name,
				strategy,
			)
		}
	}

	// Non-proxy caches (e.g. cache-aside) must not connect to db — only proxy strategies use cache -> db.
	for _, cacheNode := range design.Nodes {
		if cacheNode.Type != "cache" || requiresProxy(cacheNode) {
			continue
		}
		for _, conn := range design.Connections {
			if conn.SourceID != cacheNode.ID {
				continue
			}
			target, ok := nodeMap[conn.TargetID]
			if !ok {
				continue
			}
			if target.Type == "db" {
				strategy := cacheNodeStrategy(cacheNode)
				return fmt.Errorf(
					"cache node %q uses non-proxy strategy %q but is connected to db %q — "+
						"Non-proxy cache stores do not connect to the database directly; "+
						"remove the cache -> db edge, or change strategy to write-through/write-behind",
					cacheNode.Name, strategy, target.Name,
				)
			}
		}
	}

	// If the app reaches a cache that also reaches db, the app must not also connect directly to db.
	for _, appNode := range design.Nodes {
		if appNode.Type != "app-server" {
			continue
		}
		var hasDirectDB bool
		for _, conn := range design.Connections {
			if conn.SourceID != appNode.ID {
				continue
			}
			target, ok := nodeMap[conn.TargetID]
			if !ok {
				continue
			}
			if target.Type == "db" {
				hasDirectDB = true
				break
			}
		}
		if !hasDirectDB {
			continue
		}
		for _, conn := range design.Connections {
			if conn.SourceID != appNode.ID {
				continue
			}
			cacheTarget, ok := nodeMap[conn.TargetID]
			if !ok || cacheTarget.Type != "cache" {
				continue
			}
			for _, cc := range design.Connections {
				if cc.SourceID != cacheTarget.ID {
					continue
				}
				db, ok := nodeMap[cc.TargetID]
				if ok && db.Type == "db" {
					return fmt.Errorf(
						"ambiguous data path: app-server %q connects to both cache %q and db directly, "+
							"but the cache also connects to the db — pick one path",
						appNode.Name, cacheTarget.Name,
					)
				}
			}
		}
	}

	// If an MQ node exists, a worker must exist to consume messages.
	for _, mqNode := range design.Nodes {
		if mqNode.Type != "mq" {
			continue
		}
		hasWorker := false
		for _, n := range design.Nodes {
			if n.Type == "worker" {
				hasWorker = true
				break
			}
		}
		if !hasWorker {
			return fmt.Errorf("mq node %q requires a worker node to consume messages", mqNode.Name)
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
			return fmt.Errorf("%s", workerMissingDBMessage(workerNode.Name))
		}
		if !hasMQDep {
			if hasInboundMQ {
				return fmt.Errorf("%s", workerWrongMQDirectionMessage(workerNode.Name))
			}
			return fmt.Errorf("%s", workerMissingMQMessage(workerNode.Name))
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
			injectReadReplicaEnv(env, target, node)
		case "cache":
			// We only derive cache routing env vars for the app-server.
			// Cache-aside or proxy strategies determine whether the app-server
			// talks directly to Redis vs via caching-proxy HTTP.
			if !isAppServer {
				if node.Type == "worker" {
					env["CACHE_HOST"] = "cache"
					env["CACHE_PORT"] = "6379"
					// Workers write timeline entries, so they honor the same
					// cache-node-owned TTL as every other cache writer.
					if ttl, ok := normalizeCacheTTLSeconds(target.Config); ok {
						env["CACHE_TTL"] = ttl
					}
				}
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
			// CACHE_STRATEGY for redis mode comes from the connected cache-aside cache node
			// (strategy / cacheStrategy kept in sync by NormalizeDesign; prefer strategy).
			if cacheNode, ok := firstNonProxyCacheFromApp(node.ID, design, nodeMap); ok {
				env["CACHE_STRATEGY"] = cacheNodeStrategy(cacheNode)
			} else {
				env["CACHE_STRATEGY"] = "cache-aside"
			}
			// cacheTtlSeconds is owned by the cache node (app node is a legacy
			// fallback). Unset/invalid leaves CACHE_TTL out so the image's
			// template default applies.
			if ttl, ok := cacheAsideTTL(node, design, nodeMap); ok {
				env["CACHE_TTL"] = ttl
			}
			// WRITE_BEHIND_INTERVAL is only for caching-proxy (canonical write-behind), not url-shortener.
			env["STAMPEDE_PROTECTION"] = configString(node.Config, "stampedeProtection", "false")
		}
		if mode := configString(node.Config, "inventoryMode", ""); mode != "" {
			env["INVENTORY_MODE"] = mode
		}
		if mode := configString(node.Config, "purchaseMode", ""); mode != "" {
			env["PURCHASE_MODE"] = mode
		}
		if name := strings.TrimSpace(configString(node.Config, "seedEventName", "")); name != "" {
			env["SEED_EVENT_NAME"] = name
		}
		if cap, ok := positiveIntConfig(node.Config, "seedEventCapacity"); ok {
			env["SEED_EVENT_CAPACITY"] = cap
		}
		if hasProxyCacheConn {
			// App-driven proxy writes carry a TTL in the request body; keep it
			// on the same cache-node value as proxy miss-fill so the two paths
			// can never diverge.
			if cacheNode, ok := firstProxyCacheFromApp(node.ID, design, nodeMap); ok {
				if ttl, valid := normalizeCacheTTLSeconds(cacheNode.Config); valid {
					env["CACHE_TTL"] = ttl
				}
			}
		}
	case "db":
		env["DB_ENGINE"] = configString(node.Config, "engine", "postgresql")
		if mode, err := parseReplicationMode(node.Config); err == nil {
			env["DB_REPLICATION"] = mode
		} else {
			env["DB_REPLICATION"] = "async"
		}
		if level, err := syncCommitLevelFromConfig(node.Config); err == nil {
			env["DB_SYNC_COMMIT_LEVEL"] = level
		}
	case "cache":
		if cacheProxy {
			env["STRATEGY"] = cacheNodeStrategy(node)
			if ttl, ok := normalizeCacheTTLSeconds(node.Config); ok {
				env["CACHE_TTL"] = ttl
			}
			env["WRITE_BEHIND_INTERVAL"] = normalizeWriteBehindIntervalSeconds(node.Config)
		}
	case "worker":
		if prefetch, ok := positiveIntConfig(node.Config, "workerPrefetch"); ok {
			env["WORKER_PREFETCH"] = prefetch
		}
		if concurrency, ok := positiveIntConfig(node.Config, "workerConcurrency"); ok {
			env["WORKER_CONCURRENCY"] = concurrency
		}
	}

	mergeJobEnvIntoDerive(node, design, env)

	return env
}

// injectReadReplicaEnv adds READ_REPLICA_* env vars for app-server and proxy cache nodes
// when the connected db node has read replicas and a non-leader-only consistency policy.
func injectReadReplicaEnv(env map[string]string, dbNode, consumer DeploymentNode) {
	if consumer.Type != "app-server" && !(consumer.Type == "cache" && requiresProxy(consumer)) {
		return
	}
	readReplicas := parseReadReplicaCount(dbNode.Config)
	if readReplicas <= 0 {
		return
	}
	policy := effectiveReadConsistency(dbNode.Config, readReplicas)
	if policy == readConsistencyLeaderOnly {
		return
	}
	hosts := dbReplicaHostsFromConfig(dbNode.Config)
	if hosts == "" {
		return
	}
	firstHost := hosts
	if idx := strings.Index(hosts, ","); idx >= 0 {
		firstHost = hosts[:idx]
	}
	env["READ_REPLICA_HOSTS"] = hosts
	env["READ_REPLICA_HOST"] = firstHost
	env["READ_REPLICA_PORT"] = "5432"
	env["READ_CONSISTENCY"] = policy
}

// cacheRedisStrategyForApp is the label shown in UI for app -> Redis (non-proxy) paths.
// Same resolution as cacheNodeStrategy (NormalizeDesign keeps strategy and cacheStrategy identical).
func cacheRedisStrategyForApp(cacheNode DeploymentNode) string {
	return cacheNodeStrategy(cacheNode)
}

func configBool(config map[string]any, key string) bool {
	if config == nil {
		return false
	}
	v, ok := config[key]
	if !ok {
		return false
	}
	b, ok := v.(bool)
	return ok && b
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
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	default:
		return defaultVal
	}
}

// positiveIntConfig returns a decimal string when config[key] parses as an integer > 0.
func positiveIntConfig(config map[string]any, key string) (string, bool) {
	raw := configString(config, key, "")
	if raw == "" {
		return "", false
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return "", false
	}
	return strconv.Itoa(n), true
}

// rateLimitPerIP parses lb config rateLimitPerIp. Missing, zero, invalid, or negative → 0 (off).
// Values above 100 clamp to 100.
func rateLimitPerIP(config map[string]any) int {
	raw := configString(config, "rateLimitPerIp", "")
	if raw == "" {
		return 0
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return 0
	}
	if n > 100 {
		return 100
	}
	return n
}
