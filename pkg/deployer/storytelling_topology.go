package deployer

import (
	"fmt"

	"github.com/ywacch/cloud9/pkg/templates"
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
	// SyncStandbyNames is ANY 1 (*) or FIRST N (*) when semi-sync/sync is configured.
	SyncStandbyNames string
	CacheEngine      string
	CacheRole        string
	HasProxyCache    bool
}

// StorytellingTopologyFromDesign walks design nodes (lb, mq, worker, cache).
// template is used when design.Template is empty.
func StorytellingTopologyFromDesign(d *DeploymentDesign, template string) StorytellingTopology {
	t := StorytellingTopology{Template: template}
	if d == nil {
		return t
	}
	if t.Template == "" && d.Template != "" {
		t.Template = d.Template
	}
	tmpl := templates.GetTemplate(d.Template)
	if tmpl != nil {
		for _, b := range ResolveJobBindings(*d, tmpl) {
			if b.Job == "work_queue" {
				t.HasMQ = true
			}
		}
	}
	for _, n := range d.Nodes {
		switch n.Type {
		case "lb":
			t.HasLB = true
		case "mq":
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
			if gucs, ok := postgresSyncGUCsFromConfig(n.Config); ok {
				t.SyncStandbyNames = gucs.StandbyNames
			}
			t.DBRole = "primary"
		case "cache":
			t.HasCache = true
			if RequiresProxy(n) {
				t.HasProxyCache = true
			}
			if t.CacheStrategy == "" {
				t.CacheStrategy = cacheNodeStrategy(n)
			}
		}
	}
	if t.HasCache {
		t.CacheEngine = cacheEngineFromAliasOrNode(d, tmpl)
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

// cacheEngineFromAliasOrNode prefers the node bound to hot_cache, then timeline.
// It skips a cache that only hosts work_queue. Designs with no jobs use the first cache node.
func cacheEngineFromAliasOrNode(d *DeploymentDesign, tmpl *templates.TemplateSpec) string {
	if d == nil {
		return ""
	}
	queueOnly := map[string]bool{}
	if tmpl != nil {
		bindings := ResolveJobBindings(*d, tmpl)
		jobsOn := map[string][]string{}
		for _, b := range bindings {
			jobsOn[b.NodeID] = append(jobsOn[b.NodeID], b.Job)
		}
		for _, jobID := range []string{"hot_cache", "timeline"} {
			for _, b := range bindings {
				if b.Job != jobID || b.NodeType != "cache" {
					continue
				}
				if node := nodeByID(*d, b.NodeID); node != nil {
					return CacheEngineFromNode(*node)
				}
			}
		}
		for id, jobs := range jobsOn {
			onlyQueue := len(jobs) > 0
			for _, j := range jobs {
				if j != "work_queue" {
					onlyQueue = false
					break
				}
			}
			queueOnly[id] = onlyQueue
		}
	}
	var first *DeploymentNode
	for i := range d.Nodes {
		n := &d.Nodes[i]
		if n.Type != "cache" {
			continue
		}
		if first == nil {
			first = n
		}
		if queueOnly[n.ID] {
			continue
		}
		return CacheEngineFromNode(*n)
	}
	if first != nil {
		return CacheEngineFromNode(*first)
	}
	return ""
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
