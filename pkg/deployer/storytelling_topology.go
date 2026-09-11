package deployer

import (
	"fmt"
)

// StorytellingTopology summarizes canvas node presence and primary cache strategy
// for metrics narratives and UI. Derived only from the saved deployment design.
type StorytellingTopology struct {
	HasLB            bool
	HasMQ            bool
	HasWorker        bool
	HasCache         bool
	CacheStrategy    string
	Template         string
	HasDB            bool
	DBEngine         string
	DBRole           string
	ReadReplicaCount int
	ReplicationMode  string
	SyncCommitLevel  string
	CacheEngine      string
	CacheRole        string
	HasProxyCache    bool
}

// StorytellingTopologyFromDesign walks design nodes (load_balancer, message_queue, worker, cache).
// template is used when design.Template is empty.
func StorytellingTopologyFromDesign(d *DeploymentDesign, template string) StorytellingTopology {
	t := StorytellingTopology{Template: template}
	if d == nil {
		return t
	}
	if t.Template == "" && d.Template != "" {
		t.Template = d.Template
	}
	for _, n := range d.Nodes {
		switch n.Type {
		case "load_balancer":
			t.HasLB = true
		case "message_queue":
			t.HasMQ = true
		case "worker":
			t.HasWorker = true
		case "db":
			t.HasDB = true
			if eng := configStringFromNodeConfig(n.Config, "engine"); eng != "" {
				t.DBEngine = eng
			}
			t.ReadReplicaCount = int(parseReadReplicaCount(n.Config))
			if mode, err := parseReplicationMode(n.Config); err == nil {
				t.ReplicationMode = mode
			} else {
				t.ReplicationMode = "async"
			}
			t.SyncCommitLevel = parseSyncCommitLevel(n.Config)
			t.DBRole = "primary"
		case "cache":
			t.HasCache = true
			t.CacheEngine = "redis"
			if RequiresProxy(n) {
				t.HasProxyCache = true
			}
			if t.CacheStrategy == "" {
				t.CacheStrategy = cacheNodeStrategy(n)
			}
		}
	}
	if t.DBEngine == "" && t.HasDB {
		t.DBEngine = "postgresql"
	}
	if t.DBRole == "" && t.HasDB {
		t.DBRole = "primary"
	}
	if t.HasCache {
		if t.CacheRole == "" {
			t.CacheRole = t.CacheStrategy
		}
		if t.CacheRole == "" {
			t.CacheRole = "cache-aside"
		}
		// No db node: treat Redis as primary persistence for metrics labeling.
		if !t.HasDB {
			t.CacheRole = "primary"
		}
	}
	return t
}

func configStringFromNodeConfig(cfg map[string]interface{}, key string) string {
	if cfg == nil {
		return ""
	}
	v, ok := cfg[key]
	if !ok {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	case int:
		return fmt.Sprintf("%d", x)
	case int64:
		return fmt.Sprintf("%d", x)
	case float64:
		if x == float64(int64(x)) {
			return fmt.Sprintf("%.0f", x)
		}
		return fmt.Sprintf("%g", x)
	default:
		return fmt.Sprintf("%v", x)
	}
}
