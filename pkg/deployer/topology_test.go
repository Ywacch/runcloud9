package deployer

import (
	"strings"
	"testing"
)

func TestDeriveEnvVars_ProxyStrategyInjectsDBIntoCacheProxyNode(t *testing.T) {
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "a", Type: "app-server", Name: "API Server"},
			{ID: "d", Type: "db", Name: "PostgreSQL"},
			{
				ID:   "c",
				Type: "cache",
				Name: "Redis",
				Config: map[string]any{
					"strategy":                   "write-through",
					"cacheTtlSeconds":            float64(120),
					"writeBehindIntervalSeconds": float64(45),
				},
			},
		},
		Connections: []DeploymentConn{
			{SourceID: "a", TargetID: "c", Protocol: "TCP"},
			{SourceID: "c", TargetID: "d", Protocol: "TCP"}, // required for proxy strategies
		},
	}

	// Derive env vars for the cache node (this is the caching-proxy container env contract).
	node := design.Nodes[2]
	env := DeriveEnvVars(node, design)

	if env["DB_HOST"] != "db" {
		t.Fatalf("expected DB_HOST=db, got %q", env["DB_HOST"])
	}
	if env["DB_PORT"] != "5432" {
		t.Fatalf("expected DB_PORT=5432, got %q", env["DB_PORT"])
	}
	if env["DB_NAME"] != "syscrack" {
		t.Fatalf("expected DB_NAME=syscrack, got %q", env["DB_NAME"])
	}
	if env["DB_USER"] != "syscrack" {
		t.Fatalf("expected DB_USER=syscrack, got %q", env["DB_USER"])
	}
	if env["DB_PASSWORD"] != "syscrack" {
		t.Fatalf("expected DB_PASSWORD=syscrack, got %q", env["DB_PASSWORD"])
	}

	if env["STRATEGY"] != "write-through" {
		t.Fatalf("expected STRATEGY=write-through, got %q", env["STRATEGY"])
	}
	if env["CACHE_TTL"] != "120" {
		t.Fatalf("expected CACHE_TTL=120, got %q", env["CACHE_TTL"])
	}
	if env["WRITE_BEHIND_INTERVAL"] != "45" {
		t.Fatalf("expected WRITE_BEHIND_INTERVAL=45, got %q", env["WRITE_BEHIND_INTERVAL"])
	}
}

func TestDeriveEnvVars_ProxyStrategyAppServerGetsNoDB(t *testing.T) {
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "a", Type: "app-server", Name: "API Server"},
			{ID: "d", Type: "db", Name: "PostgreSQL"},
			{
				ID:   "c",
				Type: "cache",
				Name: "Redis",
				Config: map[string]any{
					"strategy":                   "write-through",
					"cacheTtlSeconds":            float64(120),
					"writeBehindIntervalSeconds": float64(45),
				},
			},
		},
		Connections: []DeploymentConn{
			{SourceID: "a", TargetID: "c", Protocol: "TCP"},
			{SourceID: "c", TargetID: "d", Protocol: "TCP"},
		},
	}

	node := design.Nodes[0] // app-server
	env := DeriveEnvVars(node, design)

	if env["CACHE_MODE"] != "proxy" {
		t.Fatalf("expected CACHE_MODE=proxy, got %q", env["CACHE_MODE"])
	}
	if env["CACHE_HOST"] != "cache" {
		t.Fatalf("expected CACHE_HOST=cache, got %q", env["CACHE_HOST"])
	}
	if _, ok := env["DB_HOST"]; ok {
		t.Fatalf("expected DB_HOST to be absent for proxy-mode app-server, got %q", env["DB_HOST"])
	}
	if _, ok := env["CACHE_STRATEGY"]; ok {
		t.Fatalf("expected CACHE_STRATEGY to be absent for proxy-mode app-server, got %q", env["CACHE_STRATEGY"])
	}
}

func TestDeriveEnvVars_WriteAround_NoCacheVars(t *testing.T) {
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "a", Type: "app-server", Name: "API Server"},
			{ID: "d", Type: "db", Name: "PostgreSQL"},
		},
		Connections: []DeploymentConn{
			{SourceID: "a", TargetID: "d", Protocol: "TCP"},
		},
	}

	node := design.Nodes[0] // app-server
	env := DeriveEnvVars(node, design)

	if env["DB_HOST"] != "db" {
		t.Fatalf("expected DB_HOST=db, got %q", env["DB_HOST"])
	}
	if _, ok := env["CACHE_HOST"]; ok {
		t.Fatalf("expected CACHE_HOST absent, got %q", env["CACHE_HOST"])
	}
	if _, ok := env["CACHE_MODE"]; ok {
		t.Fatalf("expected CACHE_MODE absent, got %q", env["CACHE_MODE"])
	}
	if _, ok := env["CACHE_STRATEGY"]; ok {
		t.Fatalf("expected CACHE_STRATEGY absent for write-around, got %q", env["CACHE_STRATEGY"])
	}
}

// D-06 (QA): app-server -> db and app-server -> cache-aside Redis; strategy lives on cache node.
func TestDeriveEnvVars_AppServerConnectedToDBAndCache(t *testing.T) {
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{
				ID:   "a",
				Type: "app-server",
				Name: "API Server",
				Config: map[string]any{
					"cacheTtlSeconds":    float64(60),
					"stampedeProtection": "true",
				},
			},
			{ID: "d", Type: "db", Name: "PostgreSQL"},
			// Cache node uses two different config keys on purpose (see pkg/deployer/topology.go):
			//   "strategy"       — consumed by requiresProxy: cache-aside => direct Redis, not caching-proxy.
			//   "cacheStrategy"  — consumed by DeriveEnvVars for app-server CACHE_STRATEGY (url-shortener redis mode).
			// They are not duplicates: e.g. strategy=cache-aside with cacheStrategy=write-through means
			// "plain Redis deployment" + "app uses write-through against Redis+DB" — not a mistake.
			{
				ID:   "c",
				Type: "cache",
				Name: "Redis",
				Config: map[string]any{
					"strategy":        "cache-aside",
					"cacheStrategy":   "write-through",
					"cacheTtlSeconds": float64(999), // unused for app env here; app CACHE_TTL comes from app-server config below
				},
			},
		},
		Connections: []DeploymentConn{
			{SourceID: "a", TargetID: "d", Protocol: "TCP"},
			{SourceID: "a", TargetID: "c", Protocol: "TCP"},
		},
	}

	env := DeriveEnvVars(design.Nodes[0], design)

	if env["DB_HOST"] != "db" {
		t.Fatalf("expected DB_HOST=db, got %q", env["DB_HOST"])
	}
	if env["CACHE_HOST"] != "cache" {
		t.Fatalf("expected CACHE_HOST=cache, got %q", env["CACHE_HOST"])
	}
	if env["CACHE_PORT"] != "6379" {
		t.Fatalf("expected CACHE_PORT=6379, got %q", env["CACHE_PORT"])
	}
	if env["CACHE_STRATEGY"] != "write-through" {
		t.Fatalf("expected CACHE_STRATEGY=write-through from cache node, got %q", env["CACHE_STRATEGY"])
	}
	if env["CACHE_TTL"] != "60" {
		t.Fatalf("expected CACHE_TTL=60 from app-server config, got %q", env["CACHE_TTL"])
	}
	if env["STAMPEDE_PROTECTION"] != "true" {
		t.Fatalf("expected STAMPEDE_PROTECTION=true from app-server config, got %q", env["STAMPEDE_PROTECTION"])
	}
}

func TestValidateTopology_ProxyWithoutDBConnection(t *testing.T) {
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "a", Type: "app-server", Name: "API Server"},
			{ID: "d", Type: "db", Name: "PostgreSQL"},
			{
				ID:   "c",
				Type: "cache",
				Name: "Redis",
				Config: map[string]any{
					"strategy":        "write-through",
					"cacheTtlSeconds": float64(120),
				},
			},
		},
		Connections: []DeploymentConn{
			{SourceID: "a", TargetID: "c", Protocol: "TCP"},
			// cache is not connected to db — only violation
		},
	}

	err := ValidateTopology(design)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if want := "not connected to a db node"; !strings.Contains(err.Error(), want) {
		t.Fatalf("expected error to contain %q, got %q", want, err.Error())
	}
}

func TestValidateTopology_AppServerDbWithProxyCacheForbidden(t *testing.T) {
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "a", Type: "app-server", Name: "API Server"},
			{ID: "d", Type: "db", Name: "PostgreSQL"},
			{
				ID:   "c",
				Type: "cache",
				Name: "Redis",
				Config: map[string]any{
					"strategy":        "write-through",
					"cacheTtlSeconds": float64(120),
				},
			},
		},
		Connections: []DeploymentConn{
			{SourceID: "a", TargetID: "c", Protocol: "TCP"},
			{SourceID: "a", TargetID: "d", Protocol: "TCP"},
			{SourceID: "c", TargetID: "d", Protocol: "TCP"},
		},
	}

	err := ValidateTopology(design)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if want := "must not connect directly to a db node"; !strings.Contains(err.Error(), want) {
		t.Fatalf("expected error to contain %q, got %q", want, err.Error())
	}
}

func TestDisplayCacheStrategyForApp_ProxyUsesCacheNodeStrategy(t *testing.T) {
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "a", Type: "app-server", Name: "API Server"},
			{
				ID:   "c",
				Type: "cache",
				Name: "Redis",
				Config: map[string]any{
					"strategy": "write-through",
				},
			},
		},
		Connections: []DeploymentConn{
			{SourceID: "a", TargetID: "c", Protocol: "TCP"},
		},
	}
	if got := DisplayCacheStrategyForApp("a", design); got != "write-through" {
		t.Fatalf("want write-through, got %q", got)
	}
}

func TestDisplayCacheStrategyForApp_RedisPathUsesCacheConfig(t *testing.T) {
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "a", Type: "app-server", Name: "API Server"},
			{
				ID:   "c",
				Type: "cache",
				Name: "Redis",
				Config: map[string]any{
					"strategy":      "cache-aside",
					"cacheStrategy": "ignored-for-display",
				},
			},
		},
		Connections: []DeploymentConn{
			{SourceID: "a", TargetID: "c", Protocol: "TCP"},
		},
	}
	if got := DisplayCacheStrategyForApp("a", design); got != "cache-aside" {
		t.Fatalf("want cache-aside, got %q", got)
	}
}

func TestDisplayCacheStrategyForApp_FallbackCacheStrategyKey(t *testing.T) {
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "a", Type: "app-server", Name: "API Server"},
			{
				ID:   "c",
				Type: "cache",
				Name: "Redis",
				Config: map[string]any{
					"cacheStrategy": "write-around",
				},
			},
		},
		Connections: []DeploymentConn{
			{SourceID: "a", TargetID: "c", Protocol: "TCP"},
		},
	}
	if got := DisplayCacheStrategyForApp("a", design); got != "write-around" {
		t.Fatalf("want write-around from cacheStrategy, got %q", got)
	}
}

func TestDisplayCacheStrategyForApp_NoCacheEdge(t *testing.T) {
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "a", Type: "app-server", Name: "API Server"},
			{ID: "d", Type: "db", Name: "PostgreSQL"},
		},
		Connections: []DeploymentConn{
			{SourceID: "a", TargetID: "d", Protocol: "TCP"},
		},
	}
	if got := DisplayCacheStrategyForApp("a", design); got != "" {
		t.Fatalf("want empty, got %q", got)
	}
}
