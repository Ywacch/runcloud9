package deployer

import (
	"strconv"
)

// DesignConfig is a per-instance projection of the saved deployment design for
// brain context and rule precondition checks.
type DesignConfig struct {
	Template string
	Roles    map[string][]InstanceConfig // "db" -> [{ID: "db-1", ...}]
}

// InstanceConfig summarizes one canvas node under a role.
type InstanceConfig struct {
	ID       string
	Type     string
	Engine   string
	Settings map[string]any // only keys that exist, normalized
}

// DesignConfigFromDesign normalizes the design and projects decision-table
// settings per semantic component instance.
func DesignConfigFromDesign(d *DeploymentDesign) DesignConfig {
	if d == nil {
		return DesignConfig{Roles: map[string][]InstanceConfig{}}
	}
	design := *d
	NormalizeDesign(&design)

	roles := make(map[string][]InstanceConfig)
	for _, n := range design.Nodes {
		if !isSemanticComponentType(n.Type) {
			continue
		}
		roles[n.Type] = append(roles[n.Type], instanceConfigFromNode(n))
	}
	return DesignConfig{
		Template: design.Template,
		Roles:    roles,
	}
}

func instanceConfigFromNode(n DeploymentNode) InstanceConfig {
	cfg := n.Config
	inst := InstanceConfig{
		ID:       n.ID,
		Type:     n.Type,
		Settings: map[string]any{},
	}
	switch n.Type {
	case "db":
		inst.Engine = dbEngineFromConfig(cfg)
		projectDBSettings(inst.Settings, cfg)
	case "cache":
		inst.Engine = "redis"
		projectCacheSettings(inst.Settings, n)
	case "lb":
		projectLBSettings(inst.Settings, cfg)
	case "app-server":
		projectAppServerSettings(inst.Settings, cfg)
	case "worker":
		projectWorkerSettings(inst.Settings, cfg)
	default:
		projectCommonSettings(inst.Settings, cfg)
	}
	return inst
}

func projectCommonSettings(settings map[string]any, cfg map[string]interface{}) {
	if configKeyPresent(cfg, "replicas") {
		if n, err := strconv.Atoi(configString(cfg, "replicas", "")); err == nil && n > 0 {
			settings["replicas"] = n
		}
	}
	if configKeyPresent(cfg, "instanceSize") {
		settings["instanceSize"] = configString(cfg, "instanceSize", "")
	}
	if configKeyPresent(cfg, "resourceProfile") {
		settings["resourceProfile"] = configString(cfg, "resourceProfile", "")
	}
}

func projectDBSettings(settings map[string]any, cfg map[string]interface{}) {
	projectCommonSettings(settings, cfg)
	if configKeyPresent(cfg, "readConsistency") {
		if policy, err := readConsistencyFromConfig(cfg); err == nil {
			settings["readConsistency"] = policy
		}
	}
	if configKeyPresent(cfg, "replicationMode") {
		if mode, err := parseReplicationMode(cfg); err == nil {
			settings["replicationMode"] = mode
		}
	}
	if configKeyPresent(cfg, "syncCommitLevel") {
		if level, err := syncCommitLevelFromConfig(cfg); err == nil {
			settings["syncCommitLevel"] = level
		}
	}
	if configKeyPresent(cfg, "readReplicas") {
		settings["readReplicas"] = int(ParseReadReplicaCount(cfg))
	}
	if configKeyPresent(cfg, "replicaApplyDelayMs") {
		rr := int(ParseReadReplicaCount(cfg))
		if delays, err := ParseReplicaApplyDelayMs(cfg, rr); err == nil && len(delays) > 0 {
			settings["replicaApplyDelayMs"] = delays
		}
	}
}

func projectCacheSettings(settings map[string]any, n DeploymentNode) {
	cfg := n.Config
	projectCommonSettings(settings, cfg)
	settings["cacheStrategy"] = cacheNodeStrategy(n)
	if configKeyPresent(cfg, "evictionPolicy") {
		settings["evictionPolicy"] = normalizeRedisEvictionPolicy(configString(cfg, "evictionPolicy", ""))
	}
	if ttl, ok := normalizeCacheTTLSeconds(cfg); ok {
		if n, err := strconv.Atoi(ttl); err == nil {
			settings["cacheTtlSeconds"] = n
		}
	}
	if configKeyPresent(cfg, "writeBehindIntervalSeconds") {
		if n, err := strconv.Atoi(normalizeWriteBehindIntervalSeconds(cfg)); err == nil {
			settings["writeBehindIntervalSeconds"] = n
		}
	}
}

func projectLBSettings(settings map[string]any, cfg map[string]interface{}) {
	projectCommonSettings(settings, cfg)
	settings["lbAlgorithm"] = normalizeLBAlgorithm(configString(cfg, "lbAlgorithm", lbAlgoRoundRobin))
	if limit := rateLimitPerIP(cfg); limit > 0 {
		settings["rateLimitPerIp"] = limit
	}
	if configKeyPresent(cfg, "weightByInstanceSize") {
		settings["weightByInstanceSize"] = configBool(cfg, "weightByInstanceSize")
	}
}

func projectAppServerSettings(settings map[string]any, cfg map[string]interface{}) {
	projectCommonSettings(settings, cfg)
	if configKeyPresent(cfg, "stampedeProtection") {
		settings["stampedeProtection"] = configString(cfg, "stampedeProtection", "")
	}
	if configKeyPresent(cfg, "inventoryMode") {
		settings["inventoryMode"] = configString(cfg, "inventoryMode", "")
	}
	if configKeyPresent(cfg, "purchaseMode") {
		settings["purchaseMode"] = configString(cfg, "purchaseMode", "")
	}
}

func projectWorkerSettings(settings map[string]any, cfg map[string]interface{}) {
	projectCommonSettings(settings, cfg)
	if prefetch, ok := positiveIntConfig(cfg, "workerPrefetch"); ok {
		if n, err := strconv.Atoi(prefetch); err == nil {
			settings["workerPrefetch"] = n
		}
	}
	if concurrency, ok := positiveIntConfig(cfg, "workerConcurrency"); ok {
		if n, err := strconv.Atoi(concurrency); err == nil {
			settings["workerConcurrency"] = n
		}
	}
}

func configKeyPresent(cfg map[string]interface{}, key string) bool {
	if cfg == nil {
		return false
	}
	_, ok := cfg[key]
	return ok
}
