package deployer

import (
	"sort"
	"strings"
)

// WiringLink is a credential-free view of one design connection and how traffic is balanced.
type WiringLink struct {
	SourceID          string
	TargetID          string
	TargetType        string
	Balancing         string
	ReadRoutingPolicy string
	Fields            map[string]string
}

// WiringLinksFromDesign projects deploy-time env wiring per connection without secrets.
func WiringLinksFromDesign(d *DeploymentDesign) []WiringLink {
	if d == nil || len(d.Connections) == 0 {
		return nil
	}
	design := *d
	NormalizeDesign(&design)

	nodeMap := make(map[string]DeploymentNode, len(design.Nodes))
	for _, n := range design.Nodes {
		nodeMap[n.ID] = n
	}

	var out []WiringLink
	for _, conn := range design.Connections {
		source, okS := nodeMap[conn.SourceID]
		target, okT := nodeMap[conn.TargetID]
		if !okS || !okT {
			continue
		}
		fields := wiringFieldsForConnection(source, target, design)
		balancing, policy := wiringBalancing(source, target, design)
		if len(fields) == 0 && balancing == "" && policy == "" {
			continue
		}
		out = append(out, WiringLink{
			SourceID:          conn.SourceID,
			TargetID:          conn.TargetID,
			TargetType:        target.Type,
			Balancing:         balancing,
			ReadRoutingPolicy: policy,
			Fields:            fields,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SourceID != out[j].SourceID {
			return out[i].SourceID < out[j].SourceID
		}
		return out[i].TargetID < out[j].TargetID
	})
	return out
}

func wiringFieldsForConnection(source, target DeploymentNode, design DeploymentDesign) map[string]string {
	env := DeriveEnvVars(source, design)
	keys := wiringFieldKeysForTarget(source, target, design)
	if len(keys) == 0 {
		return nil
	}
	out := make(map[string]string, len(keys))
	for _, key := range keys {
		if isCredentialEnvKey(key) {
			continue
		}
		if val, ok := env[key]; ok && strings.TrimSpace(val) != "" {
			out[key] = val
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func wiringFieldKeysForTarget(source, target DeploymentNode, design DeploymentDesign) []string {
	switch target.Type {
	case "db":
		if source.Type == "app-server" && appServerHasProxyCacheConn(source.ID, design) {
			return nil
		}
		if source.Type == "cache" && !requiresProxy(source) {
			return nil
		}
		keys := []string{
			"DB_HOST", "DB_PORT", "DB_NAME",
			"READ_REPLICA_HOST", "READ_REPLICA_HOSTS", "READ_REPLICA_PORT", "READ_CONSISTENCY",
			"DB_REPLICATION", "DB_SYNC_COMMIT_LEVEL",
		}
		if source.Type == "cache" && requiresProxy(source) {
			keys = append(keys, "STRATEGY", "WRITE_BEHIND_INTERVAL", "CACHE_TTL")
		}
		return keys
	case "cache":
		if source.Type != "app-server" && source.Type != "worker" {
			return nil
		}
		return []string{"CACHE_HOST", "CACHE_PORT", "CACHE_MODE", "CACHE_STRATEGY", "CACHE_TTL"}
	case "mq":
		keys := []string{"MQ_HOST", "MQ_PORT"}
		if source.Type == "worker" {
			keys = append(keys, "WORKER_PREFETCH", "WORKER_CONCURRENCY")
		}
		return keys
	default:
		return nil
	}
}

func wiringBalancing(source, target DeploymentNode, design DeploymentDesign) (balancing, readPolicy string) {
	switch target.Type {
	case "db":
		readReplicas := parseReadReplicaCount(target.Config)
		policy := effectiveReadConsistency(target.Config, readReplicas)
		if readReplicas > 0 && policy != readConsistencyLeaderOnly &&
			(source.Type == "app-server" || (source.Type == "cache" && requiresProxy(source))) {
			return "app_round_robin", policy
		}
		return "kube_service_per_connection", policy
	default:
		return "kube_service_per_connection", ""
	}
}

func appServerHasProxyCacheConn(appID string, design DeploymentDesign) bool {
	nodeMap := make(map[string]DeploymentNode, len(design.Nodes))
	for _, n := range design.Nodes {
		nodeMap[n.ID] = n
	}
	for _, conn := range design.Connections {
		if conn.SourceID != appID {
			continue
		}
		target, ok := nodeMap[conn.TargetID]
		if ok && target.Type == "cache" && requiresProxy(target) {
			return true
		}
	}
	return false
}

func isCredentialEnvKey(key string) bool {
	upper := strings.ToUpper(strings.TrimSpace(key))
	if strings.HasSuffix(upper, "_PASSWORD") || strings.HasSuffix(upper, "_USER") {
		return true
	}
	switch upper {
	case "DB_PASSWORD", "DB_USER", "REDIS_PASSWORD", "RABBITMQ_PASSWORD", "RABBITMQ_USER":
		return true
	default:
		return false
	}
}
