package deployer

import (
	"testing"
)

func TestStorytellingTopologyFromDesign(t *testing.T) {
	t.Parallel()
	d := &DeploymentDesign{
		Template: "social-feed",
		Nodes: []DeploymentNode{
			{Type: "lb"},
			{Type: "cache", Config: map[string]interface{}{"strategy": "timeline-store"}},
			{Type: "mq"},
		},
	}
	got := StorytellingTopologyFromDesign(d, "")
	if !got.HasLB || !got.HasCache || !got.HasMQ {
		t.Fatalf("flags: %+v", got)
	}
	if got.CacheStrategy != "timeline-store" {
		t.Fatalf("CacheStrategy: want timeline-store, got %q", got.CacheStrategy)
	}
	if got.Template != "social-feed" {
		t.Fatalf("Template: %q", got.Template)
	}
	if StorytellingTopologyFromDesign(nil, "x").Template != "x" {
		t.Fatal("nil design should preserve template arg")
	}
}

func TestStorytellingTopologyFromDesign_Storage(t *testing.T) {
	t.Parallel()
	d := &DeploymentDesign{
		Nodes: []DeploymentNode{
			{Type: "db", Config: map[string]interface{}{"engine": "postgresql", "replicationMode": "read-replica"}},
			{Type: "cache", Config: map[string]interface{}{"strategy": "write-through"}},
		},
	}
	got := StorytellingTopologyFromDesign(d, "")
	if !got.HasDB || got.DBEngine != "postgresql" || got.DBRole != "primary" {
		t.Fatalf("db: %+v", got)
	}
	if got.ReplicationMode != "async" || got.ReadReplicaCount != 0 {
		t.Fatalf("replication: mode=%q count=%d", got.ReplicationMode, got.ReadReplicaCount)
	}
	if got.CacheEngine != "redis" || got.CacheRole != "write-through" {
		t.Fatalf("cache: engine=%q role=%q", got.CacheEngine, got.CacheRole)
	}
	if !got.HasProxyCache {
		t.Fatal("write-through cache should set HasProxyCache")
	}
}

func TestStorytellingTopologyFromDesign_SyncCommitLevel(t *testing.T) {
	t.Parallel()
	d := &DeploymentDesign{
		Nodes: []DeploymentNode{
			{Type: "db", Config: map[string]interface{}{
				"engine":          "postgresql",
				"replicationMode": "semi-sync",
				"readReplicas":    2,
				"syncCommitLevel": "remote_apply",
			}},
		},
	}
	got := StorytellingTopologyFromDesign(d, "")
	if got.ReplicationMode != "semi-sync" {
		t.Fatalf("ReplicationMode: want semi-sync, got %q", got.ReplicationMode)
	}
	if got.SyncCommitLevel != "remote_apply" {
		t.Fatalf("SyncCommitLevel: want remote_apply, got %q", got.SyncCommitLevel)
	}
	if got.SyncStandbyNames != "ANY 1 (*)" {
		t.Fatalf("SyncStandbyNames: want ANY 1 (*), got %q", got.SyncStandbyNames)
	}
}

func TestStorytellingTopologyFromDesign_ReadReplicas(t *testing.T) {
	t.Parallel()
	d := &DeploymentDesign{
		Nodes: []DeploymentNode{
			{Type: "db", Config: map[string]interface{}{
				"engine":          "postgresql",
				"replicationMode": "async",
				"readReplicas":    1,
			}},
		},
	}
	got := StorytellingTopologyFromDesign(d, "")
	if got.DBRole != "primary" {
		t.Fatalf("DBRole: want primary, got %q", got.DBRole)
	}
	if got.ReadReplicaCount != 1 {
		t.Fatalf("ReadReplicaCount: want 1, got %d", got.ReadReplicaCount)
	}
	if got.ReplicationMode != "async" {
		t.Fatalf("ReplicationMode: want async, got %q", got.ReplicationMode)
	}
}

func TestStorytellingTopologyFromDesign_CacheAsideNoProxy(t *testing.T) {
	t.Parallel()
	d := &DeploymentDesign{
		Nodes: []DeploymentNode{
			{Type: "cache", Config: map[string]interface{}{"strategy": "cache-aside"}},
		},
	}
	got := StorytellingTopologyFromDesign(d, "")
	if got.HasProxyCache {
		t.Fatal("cache-aside should not set HasProxyCache")
	}
}

func TestStorytellingTopologyFromDesign_ReadThroughFoldsToWriteThrough(t *testing.T) {
	t.Parallel()
	d := &DeploymentDesign{
		Nodes: []DeploymentNode{
			{Type: "db"},
			{Type: "cache", Config: map[string]interface{}{"strategy": "read-through"}},
		},
	}
	got := StorytellingTopologyFromDesign(d, "")
	if !got.HasProxyCache {
		t.Fatal("legacy read-through should still be a proxy topology")
	}
	if got.CacheStrategy != "write-through" {
		t.Fatalf("CacheStrategy: want write-through, got %q", got.CacheStrategy)
	}
	if got.CacheRole != "write-through" {
		t.Fatalf("CacheRole: want write-through, got %q", got.CacheRole)
	}
}

func TestStorytellingTopologyFromDesign_RedisOnlyPrimary(t *testing.T) {
	t.Parallel()
	d := &DeploymentDesign{
		Nodes: []DeploymentNode{{Type: "cache"}},
	}
	got := StorytellingTopologyFromDesign(d, "")
	if got.HasDB || got.CacheRole != "primary" {
		t.Fatalf("want redis primary: %+v", got)
	}
}

func TestStorytellingTopology_HasLB(t *testing.T) {
	t.Parallel()
	d := &DeploymentDesign{
		Template: "ecommerce",
		Nodes:    []DeploymentNode{{Type: "lb"}},
	}
	got := StorytellingTopologyFromDesign(d, "ecommerce")
	if !got.HasLB {
		t.Fatal("expected HasLB for lb node type")
	}
}

func TestStorytellingTopology_HasMQRedisQueue(t *testing.T) {
	t.Parallel()
	design, ok := LayoutDesign("social-feed-wq-dedicated")
	if !ok {
		t.Fatal("layout")
	}
	got := StorytellingTopologyFromDesign(&design, design.Template)
	if !got.HasMQ {
		t.Fatal("redis work_queue binding should set HasMQ")
	}
}

func TestStorytellingTopology_CacheEngineFromAlias(t *testing.T) {
	t.Parallel()
	design, ok := LayoutDesign("ecommerce-hc-mc-wq-redis")
	if !ok {
		t.Fatal("layout")
	}
	got := StorytellingTopologyFromDesign(&design, design.Template)
	if got.CacheEngine != "memcached" {
		t.Fatalf("CacheEngine: want memcached, got %q", got.CacheEngine)
	}
}
