package api

import (
	"testing"

	"github.com/ywacch/cloud9/pkg/deployer"
)

func TestMetricsTopologyFromDeploymentDesign_Storage(t *testing.T) {
	t.Parallel()
	d := &deployer.DeploymentDesign{
		Nodes: []deployer.DeploymentNode{
			{Type: "db", Config: map[string]interface{}{"engine": "postgresql"}},
			{Type: "cache", Config: map[string]interface{}{"cacheStrategy": "cache-aside"}},
		},
	}
	topo := MetricsTopologyFromDeploymentDesign(d, "url-shortener")
	if !topo.HasDB || topo.DBEngine != "postgresql" || topo.DBRole != "primary" {
		t.Fatalf("db: has=%v engine=%q role=%q", topo.HasDB, topo.DBEngine, topo.DBRole)
	}
	if !topo.HasCache || topo.CacheEngine != "redis" || topo.CacheRole != "cache-aside" {
		t.Fatalf("cache: has=%v engine=%q role=%q", topo.HasCache, topo.CacheEngine, topo.CacheRole)
	}
}

func TestMetricsTopologyFromDeploymentDesign_RedisPrimary(t *testing.T) {
	t.Parallel()
	d := &deployer.DeploymentDesign{
		Nodes: []deployer.DeploymentNode{
			{Type: "cache", Config: map[string]interface{}{"strategy": "cache-aside"}},
			{Type: "app-server"},
		},
	}
	topo := MetricsTopologyFromDeploymentDesign(d, "url-shortener")
	if topo.HasDB {
		t.Fatal("expected no db node")
	}
	if topo.CacheRole != "primary" {
		t.Fatalf("redis-as-primary role: got %q", topo.CacheRole)
	}
}
