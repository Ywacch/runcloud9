package deployer

import (
	"strings"
	"testing"
)

func TestDeploymentOrder_Chain(t *testing.T) {
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "app", Type: "app-server", Name: "app"},
			{ID: "db", Type: "db", Name: "db"},
		},
		Connections: []DeploymentConn{
			{SourceID: "app", TargetID: "db"},
		},
	}

	ordered, err := DeploymentOrder(design)
	if err != nil {
		t.Fatalf("DeploymentOrder returned error: %v", err)
	}
	if len(ordered) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(ordered))
	}
	if ordered[0].ID != "db" || ordered[1].ID != "app" {
		t.Fatalf("unexpected order: got [%s, %s]", ordered[0].ID, ordered[1].ID)
	}

	layers, err := DeploymentLayers(design)
	if err != nil {
		t.Fatalf("DeploymentLayers: %v", err)
	}
	if len(layers) != 2 || len(layers[0]) != 1 || len(layers[1]) != 1 {
		t.Fatalf("layers: want 2 layers of 1 node, got %#v", layers)
	}
	if layers[0][0].ID != "db" || layers[1][0].ID != "app" {
		t.Fatalf("layer order: got [%+v][%+v]", layers[0][0], layers[1][0])
	}
}

func TestDeploymentLayers_ParallelRoots(t *testing.T) {
	t.Parallel()
	// lb -> app -> db and app -> cache: first deploy wave is {db, cache}, then app, then lb.
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "lb", Type: "lb", Name: "lb"},
			{ID: "app", Type: "app-server", Name: "app"},
			{ID: "db", Type: "db", Name: "db"},
			{ID: "cache", Type: "cache", Name: "cache"},
		},
		Connections: []DeploymentConn{
			{SourceID: "lb", TargetID: "app"},
			{SourceID: "app", TargetID: "db"},
			{SourceID: "app", TargetID: "cache"},
		},
	}
	ordered, err := DeploymentOrder(design)
	if err != nil {
		t.Fatal(err)
	}
	layers, err := DeploymentLayers(design)
	if err != nil {
		t.Fatal(err)
	}
	var flat []string
	for _, L := range layers {
		for _, n := range L {
			flat = append(flat, n.ID)
		}
	}
	var want []string
	for _, n := range ordered {
		want = append(want, n.ID)
	}
	if len(flat) != len(want) {
		t.Fatalf("flatten len: got %v want %v", flat, want)
	}
	for i := range flat {
		if flat[i] != want[i] {
			t.Fatalf("flatten[%d]: got %s want %s (full got=%v want=%v)", i, flat[i], want[i], flat, want)
		}
	}
	if len(layers[0]) != 2 {
		t.Fatalf("first layer should have db+cache parallel, got %d nodes", len(layers[0]))
	}
}

func TestDeploymentLayers_Cycle(t *testing.T) {
	t.Parallel()
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "a", Type: "app-server", Name: "a"},
			{ID: "b", Type: "db", Name: "b"},
		},
		Connections: []DeploymentConn{
			{SourceID: "a", TargetID: "b"},
			{SourceID: "b", TargetID: "a"},
		},
	}
	if _, err := DeploymentLayers(design); err == nil {
		t.Fatal("expected cycle error")
	}
}

func TestDeploymentOrder_Cycle(t *testing.T) {
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "a", Type: "app-server", Name: "a"},
			{ID: "b", Type: "db", Name: "b"},
		},
		Connections: []DeploymentConn{
			{SourceID: "a", TargetID: "b"},
			{SourceID: "b", TargetID: "a"},
		},
	}

	_, err := DeploymentOrder(design)
	if err == nil {
		t.Fatalf("expected error for cycle, got nil")
	}
}

func TestDeploymentOrder_StableTieBreak_NoEdges(t *testing.T) {
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "b", Type: "db", Name: "b"},
			{ID: "a", Type: "db", Name: "a"},
			{ID: "c", Type: "db", Name: "c"},
		},
	}

	ordered, err := DeploymentOrder(design)
	if err != nil {
		t.Fatalf("DeploymentOrder returned error: %v", err)
	}
	// Kahn with stable queue produces [a,b,c] then reversed => [c,b,a].
	if ordered[0].ID != "c" || ordered[1].ID != "b" || ordered[2].ID != "a" {
		t.Fatalf("unexpected order: got [%s, %s, %s]", ordered[0].ID, ordered[1].ID, ordered[2].ID)
	}
}

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
			// Prefer strategy when strategy and cacheStrategy disagree (NormalizeDesign syncs both).
			{
				ID:   "c",
				Type: "cache",
				Name: "Redis",
				Config: map[string]any{
					"strategy":        "cache-aside",
					"cacheStrategy":   "write-through",
					"cacheTtlSeconds": float64(999), // cache node owns TTL; wins over the app-server's legacy value
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
	if env["CACHE_STRATEGY"] != "cache-aside" {
		t.Fatalf("expected CACHE_STRATEGY=cache-aside (strategy wins over cacheStrategy), got %q", env["CACHE_STRATEGY"])
	}
	if env["CACHE_TTL"] != "999" {
		t.Fatalf("expected CACHE_TTL=999 from cache-node config (owner), got %q", env["CACHE_TTL"])
	}
	if env["STAMPEDE_PROTECTION"] != "true" {
		t.Fatalf("expected STAMPEDE_PROTECTION=true from app-server config, got %q", env["STAMPEDE_PROTECTION"])
	}
}

// Worker dependencies are outbound: worker -> mq and worker -> db.
func TestDeriveEnvVars_WorkerGetsMQFromOutboundEdge(t *testing.T) {
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "mq1", Type: "mq", Name: "mq"},
			{ID: "w1", Type: "worker", Name: "worker"},
			{ID: "d1", Type: "db", Name: "db"},
		},
		Connections: []DeploymentConn{
			{SourceID: "w1", TargetID: "mq1"},
			{SourceID: "w1", TargetID: "d1"},
		},
	}
	worker := design.Nodes[1]
	env := DeriveEnvVars(worker, design)
	if env["MQ_HOST"] != "mq" {
		t.Fatalf("expected MQ_HOST=mq, got %q", env["MQ_HOST"])
	}
	if env["MQ_PORT"] != "5672" {
		t.Fatalf("expected MQ_PORT=5672, got %q", env["MQ_PORT"])
	}
	if env["DB_HOST"] != "db" {
		t.Fatalf("expected DB_HOST=db from worker->db, got %q", env["DB_HOST"])
	}
}

func TestDeriveEnvVars_WorkerGetsCacheFromOutboundEdge(t *testing.T) {
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "w1", Type: "worker", Name: "worker"},
			{ID: "c1", Type: "cache", Name: "cache"},
		},
		Connections: []DeploymentConn{
			{SourceID: "w1", TargetID: "c1"},
		},
	}
	env := DeriveEnvVars(design.Nodes[0], design)
	if env["CACHE_HOST"] != "cache" {
		t.Fatalf("expected CACHE_HOST=cache from worker->cache, got %q", env["CACHE_HOST"])
	}
	if env["CACHE_PORT"] != "6379" {
		t.Fatalf("expected CACHE_PORT=6379 from worker->cache, got %q", env["CACHE_PORT"])
	}
}

func TestDeriveEnvVars_AppServerGetsMQFromOutboundEdge(t *testing.T) {
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "a1", Type: "app-server", Name: "app"},
			{ID: "mq1", Type: "mq", Name: "mq"},
		},
		Connections: []DeploymentConn{
			{SourceID: "a1", TargetID: "mq1"},
		},
	}
	env := DeriveEnvVars(design.Nodes[0], design)
	if env["MQ_HOST"] != "mq" {
		t.Fatalf("expected MQ_HOST=mq, got %q", env["MQ_HOST"])
	}
	if env["MQ_PORT"] != "5672" {
		t.Fatalf("expected MQ_PORT=5672, got %q", env["MQ_PORT"])
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
	if want := "ambiguous data path"; !strings.Contains(err.Error(), want) {
		t.Fatalf("expected error to contain %q, got %q", want, err.Error())
	}
}

func TestValidateTopology_CacheAsideWithCacheToDBForbidden(t *testing.T) {
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "a", Type: "app-server", Name: "API Server"},
			{ID: "d", Type: "db", Name: "PostgreSQL"},
			{
				ID:   "c",
				Type: "cache",
				Name: "Redis",
				Config: map[string]any{
					"strategy": "cache-aside",
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
	if want := "Non-proxy cache stores do not connect to the database directly"; !strings.Contains(err.Error(), want) {
		t.Fatalf("expected error to contain %q, got %q", want, err.Error())
	}
}

func TestValidateTopology_AppCacheAsideAndDB_NoCacheToDB_Valid(t *testing.T) {
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "a", Type: "app-server", Name: "API Server"},
			{ID: "d", Type: "db", Name: "PostgreSQL"},
			{
				ID:   "c",
				Type: "cache",
				Name: "Redis",
				Config: map[string]any{
					"strategy": "cache-aside",
				},
			},
		},
		Connections: []DeploymentConn{
			{SourceID: "a", TargetID: "c", Protocol: "TCP"},
			{SourceID: "a", TargetID: "d", Protocol: "TCP"},
		},
	}

	if err := ValidateTopology(design); err != nil {
		t.Fatalf("expected valid topology, got %v", err)
	}
}

func TestValidateTopology_AppProxyCacheToDB_NoDirectDB_Valid(t *testing.T) {
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
			{SourceID: "c", TargetID: "d", Protocol: "TCP"},
		},
	}

	if err := ValidateTopology(design); err != nil {
		t.Fatalf("expected valid topology, got %v", err)
	}
}

func TestValidateTopology_WorkerWithoutDB(t *testing.T) {
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "w1", Type: "worker", Name: "Worker"},
			{ID: "mq1", Type: "mq", Name: "MQ"},
			{ID: "d1", Type: "db", Name: "DB"},
		},
		Connections: []DeploymentConn{
			{SourceID: "w1", TargetID: "mq1"},
		},
	}

	err := ValidateTopology(design)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if want := "worker -> db"; !strings.Contains(err.Error(), want) {
		t.Fatalf("expected error to contain %q, got %q", want, err.Error())
	}
}

func TestValidateTopology_WorkerWithoutMQ(t *testing.T) {
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "w1", Type: "worker", Name: "Worker"},
			{ID: "mq1", Type: "mq", Name: "MQ"},
			{ID: "d1", Type: "db", Name: "DB"},
		},
		Connections: []DeploymentConn{
			{SourceID: "w1", TargetID: "d1"},
		},
	}

	err := ValidateTopology(design)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if want := "worker -> mq"; !strings.Contains(err.Error(), want) {
		t.Fatalf("expected error to contain %q, got %q", want, err.Error())
	}
}

func TestValidateTopology_WorkerInboundMQRejected(t *testing.T) {
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "w1", Type: "worker", Name: "Worker"},
			{ID: "mq1", Type: "mq", Name: "MQ"},
			{ID: "d1", Type: "db", Name: "DB"},
		},
		Connections: []DeploymentConn{
			{SourceID: "mq1", TargetID: "w1"},
			{SourceID: "w1", TargetID: "d1"},
		},
	}

	err := ValidateTopology(design)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if want := "use worker -> mq"; !strings.Contains(err.Error(), want) {
		t.Fatalf("expected error to contain %q, got %q", want, err.Error())
	}
}

func TestValidateTopology_WorkerWithAllDeps(t *testing.T) {
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "w1", Type: "worker", Name: "Worker"},
			{ID: "mq1", Type: "mq", Name: "MQ"},
			{ID: "d1", Type: "db", Name: "DB"},
		},
		Connections: []DeploymentConn{
			{SourceID: "w1", TargetID: "mq1"},
			{SourceID: "w1", TargetID: "d1"},
		},
	}

	if err := ValidateTopology(design); err != nil {
		t.Fatalf("expected valid topology, got %v", err)
	}
}

func TestValidateTopology_MultipleAppServers(t *testing.T) {
	t.Parallel()
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "a1", Type: "app-server", Name: "App1"},
			{ID: "a2", Type: "app-server", Name: "App2"},
			{ID: "d1", Type: "db", Name: "DB"},
		},
		Connections: []DeploymentConn{
			{SourceID: "a1", TargetID: "d1"},
			{SourceID: "a2", TargetID: "d1"},
		},
	}
	err := ValidateTopology(design)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if want := "only one app-server"; !strings.Contains(err.Error(), want) {
		t.Fatalf("expected error to contain %q, got %q", want, err.Error())
	}
}

func TestValidateTopology_MultipleWorkers(t *testing.T) {
	t.Parallel()
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "w1", Type: "worker", Name: "W1"},
			{ID: "w2", Type: "worker", Name: "W2"},
			{ID: "mq1", Type: "mq", Name: "MQ"},
			{ID: "d1", Type: "db", Name: "DB"},
		},
		Connections: []DeploymentConn{
			{SourceID: "w1", TargetID: "mq1"},
			{SourceID: "w1", TargetID: "d1"},
			{SourceID: "w2", TargetID: "mq1"},
			{SourceID: "w2", TargetID: "d1"},
		},
	}
	err := ValidateTopology(design)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if want := "only one worker"; !strings.Contains(err.Error(), want) {
		t.Fatalf("expected error to contain %q, got %q", want, err.Error())
	}
}

func TestValidateTopology_MultipleCaches(t *testing.T) {
	t.Parallel()
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "a1", Type: "app-server", Name: "App"},
			{ID: "c1", Type: "cache", Name: "Cache 1"},
			{ID: "c2", Type: "cache", Name: "Cache 2"},
			{ID: "d1", Type: "db", Name: "DB"},
		},
		Connections: []DeploymentConn{
			{SourceID: "a1", TargetID: "d1"},
			{SourceID: "a1", TargetID: "c1"},
			{SourceID: "a1", TargetID: "c2"},
		},
	}
	err := ValidateTopology(design)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if want := "only one cache"; !strings.Contains(err.Error(), want) {
		t.Fatalf("expected error to contain %q, got %q", want, err.Error())
	}
}

func TestValidateTopology_MultipleDBs(t *testing.T) {
	t.Parallel()
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "a1", Type: "app-server", Name: "App"},
			{ID: "d1", Type: "db", Name: "DB1"},
			{ID: "d2", Type: "db", Name: "DB2"},
		},
		Connections: []DeploymentConn{
			{SourceID: "a1", TargetID: "d1"},
			{SourceID: "a1", TargetID: "d2"},
		},
	}
	err := ValidateTopology(design)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if want := "only one db"; !strings.Contains(err.Error(), want) {
		t.Fatalf("expected error to contain %q, got %q", want, err.Error())
	}
}

func TestValidateTopology_MultipleMQs(t *testing.T) {
	t.Parallel()
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "a1", Type: "app-server", Name: "App"},
			{ID: "d1", Type: "db", Name: "DB"},
			{ID: "mq1", Type: "mq", Name: "MQ1"},
			{ID: "mq2", Type: "mq", Name: "MQ2"},
			{ID: "w1", Type: "worker", Name: "Worker"},
		},
		Connections: []DeploymentConn{
			{SourceID: "a1", TargetID: "d1"},
			{SourceID: "a1", TargetID: "mq1"},
			{SourceID: "w1", TargetID: "d1"},
			{SourceID: "w1", TargetID: "mq1"},
		},
	}
	err := ValidateTopology(design)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if want := "only one mq"; !strings.Contains(err.Error(), want) {
		t.Fatalf("expected error to contain %q, got %q", want, err.Error())
	}
}

func TestValidateTopology_MQWithoutWorker(t *testing.T) {
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "mq1", Type: "mq", Name: "MQ"},
			{ID: "a1", Type: "app-server", Name: "App"},
			{ID: "d1", Type: "db", Name: "DB"},
		},
		Connections: []DeploymentConn{
			{SourceID: "a1", TargetID: "d1"},
		},
	}

	err := ValidateTopology(design)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if want := "requires a worker node"; !strings.Contains(err.Error(), want) {
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

func TestDeriveEnvVars_WorkerTuningKnobs(t *testing.T) {
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{
				ID:   "w",
				Type: "worker",
				Name: "Fan-out Worker",
				Config: map[string]any{
					"workerPrefetch":    25,
					"workerConcurrency": 4,
				},
			},
		},
	}

	env := DeriveEnvVars(design.Nodes[0], design)
	if env["WORKER_PREFETCH"] != "25" {
		t.Fatalf("WORKER_PREFETCH: want 25, got %q", env["WORKER_PREFETCH"])
	}
	if env["WORKER_CONCURRENCY"] != "4" {
		t.Fatalf("WORKER_CONCURRENCY: want 4, got %q", env["WORKER_CONCURRENCY"])
	}
}

func TestDeriveEnvVars_WorkerTuningKnobsRejectNonPositive(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		config map[string]any
		want   map[string]string
	}{
		{
			name:   "zero and negative omitted",
			config: map[string]any{"workerPrefetch": 0, "workerConcurrency": -1},
			want:   map[string]string{},
		},
		{
			name:   "non-integer omitted",
			config: map[string]any{"workerPrefetch": "many", "workerConcurrency": "x"},
			want:   map[string]string{},
		},
		{
			name:   "string digits accepted",
			config: map[string]any{"workerPrefetch": "10", "workerConcurrency": "2"},
			want:   map[string]string{"WORKER_PREFETCH": "10", "WORKER_CONCURRENCY": "2"},
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			design := DeploymentDesign{
				Nodes: []DeploymentNode{{
					ID: "w", Type: "worker", Name: "Worker", Config: tc.config,
				}},
			}
			env := DeriveEnvVars(design.Nodes[0], design)
			for key, want := range tc.want {
				if env[key] != want {
					t.Fatalf("%s: want %q, got %q", key, want, env[key])
				}
			}
			if _, ok := tc.want["WORKER_PREFETCH"]; !ok {
				if _, present := env["WORKER_PREFETCH"]; present {
					t.Fatalf("WORKER_PREFETCH should be omitted, got %q", env["WORKER_PREFETCH"])
				}
			}
			if _, ok := tc.want["WORKER_CONCURRENCY"]; !ok {
				if _, present := env["WORKER_CONCURRENCY"]; present {
					t.Fatalf("WORKER_CONCURRENCY should be omitted, got %q", env["WORKER_CONCURRENCY"])
				}
			}
		})
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

func TestCanonicalCacheStrategy(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in, want string
	}{
		{"write-through", "write-through"},
		{"write-behind", "write-behind"},
		{"cache-aside", "cache-aside"},
		{"read-through", "write-through"},
		{"READ-THROUGH", "write-through"},
		{"  read-through  ", "write-through"},
		{"", ""},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			if got := CanonicalCacheStrategy(tc.in); got != tc.want {
				t.Fatalf("CanonicalCacheStrategy(%q)=%q want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestRequiresProxy_ReadThroughLegacy(t *testing.T) {
	t.Parallel()
	n := DeploymentNode{
		Type:   "cache",
		Config: map[string]any{"strategy": "read-through"},
	}
	if !RequiresProxy(n) {
		t.Fatal("legacy read-through must still require proxy topology")
	}
	if got := cacheNodeStrategy(n); got != "write-through" {
		t.Fatalf("cacheNodeStrategy: got %q want write-through", got)
	}
}

func TestDeriveEnvVars_ReadThroughNormalizesSTRATEGY(t *testing.T) {
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "a", Type: "app-server", Name: "API Server"},
			{ID: "d", Type: "db", Name: "PostgreSQL"},
			{
				ID:   "c",
				Type: "cache",
				Name: "Redis",
				Config: map[string]any{
					"strategy":        "read-through",
					"cacheTtlSeconds": float64(90),
				},
			},
		},
		Connections: []DeploymentConn{
			{SourceID: "a", TargetID: "c", Protocol: "TCP"},
			{SourceID: "c", TargetID: "d", Protocol: "TCP"},
		},
	}
	env := DeriveEnvVars(design.Nodes[2], design)
	if env["STRATEGY"] != "write-through" {
		t.Fatalf("STRATEGY: got %q want write-through", env["STRATEGY"])
	}
	if got := DisplayCacheStrategyForApp("a", design); got != "write-through" {
		t.Fatalf("DisplayCacheStrategyForApp: got %q want write-through", got)
	}
}

func TestDeriveEnvVars_AppServerPurchaseMode(t *testing.T) {
	t.Parallel()
	design := DeploymentDesign{
		Nodes: []DeploymentNode{{
			ID:     "a",
			Type:   "app-server",
			Name:   "Ticket API",
			Config: map[string]any{"purchaseMode": "safe"},
		}},
	}
	env := DeriveEnvVars(design.Nodes[0], design)
	if env["PURCHASE_MODE"] != "safe" {
		t.Fatalf("PURCHASE_MODE: got %q want safe", env["PURCHASE_MODE"])
	}
}

func TestDeriveEnvVars_AppServerTicketDropSeed(t *testing.T) {
	t.Parallel()
	design := DeploymentDesign{
		Nodes: []DeploymentNode{{
			ID:   "a",
			Type: "app-server",
			Name: "Ticket API",
			Config: map[string]any{
				"seedEventName":     "Midnight Arena — Floor",
				"seedEventCapacity": 120,
			},
		}},
	}
	env := DeriveEnvVars(design.Nodes[0], design)
	if env["SEED_EVENT_NAME"] != "Midnight Arena — Floor" {
		t.Fatalf("SEED_EVENT_NAME: got %q", env["SEED_EVENT_NAME"])
	}
	if env["SEED_EVENT_CAPACITY"] != "120" {
		t.Fatalf("SEED_EVENT_CAPACITY: got %q want 120", env["SEED_EVENT_CAPACITY"])
	}
}

func TestDeriveEnvVars_AppServerOptimisticPurchaseMode(t *testing.T) {
	t.Parallel()
	design := DeploymentDesign{
		Nodes: []DeploymentNode{{
			ID:     "a",
			Type:   "app-server",
			Name:   "Ticket API",
			Config: map[string]any{"purchaseMode": "optimistic"},
		}},
	}
	env := DeriveEnvVars(design.Nodes[0], design)
	if env["PURCHASE_MODE"] != "optimistic" {
		t.Fatalf("PURCHASE_MODE: got %q want optimistic", env["PURCHASE_MODE"])
	}
}

func TestDeriveEnvVars_AppServerInventoryMode(t *testing.T) {
	t.Parallel()
	design := DeploymentDesign{
		Nodes: []DeploymentNode{{
			ID:     "a",
			Type:   "app-server",
			Name:   "Shop API",
			Config: map[string]any{"inventoryMode": "pessimistic"},
		}},
	}
	env := DeriveEnvVars(design.Nodes[0], design)
	if env["INVENTORY_MODE"] != "pessimistic" {
		t.Fatalf("INVENTORY_MODE: got %q want pessimistic", env["INVENTORY_MODE"])
	}
}

func TestDeriveEnvVars_AppServerOmitsModeWhenUnset(t *testing.T) {
	t.Parallel()
	design := DeploymentDesign{
		Nodes: []DeploymentNode{{
			ID:   "a",
			Type: "app-server",
			Name: "API",
		}},
	}
	env := DeriveEnvVars(design.Nodes[0], design)
	if _, ok := env["PURCHASE_MODE"]; ok {
		t.Fatalf("expected PURCHASE_MODE absent, got %q", env["PURCHASE_MODE"])
	}
	if _, ok := env["INVENTORY_MODE"]; ok {
		t.Fatalf("expected INVENTORY_MODE absent, got %q", env["INVENTORY_MODE"])
	}
}

func TestDeriveEnvVars_DBReplicationMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  map[string]interface{}
		want string
	}{
		{name: "default async", cfg: nil, want: "async"},
		{name: "none maps async", cfg: map[string]interface{}{"replicationMode": "none"}, want: "async"},
		{name: "sync", cfg: map[string]interface{}{"replicationMode": "sync"}, want: "sync"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			node := DeploymentNode{ID: "db1", Type: "db", Name: "PostgreSQL", Config: tc.cfg}
			env := DeriveEnvVars(node, DeploymentDesign{})
			if env["DB_REPLICATION"] != tc.want {
				t.Fatalf("DB_REPLICATION: got %q want %q", env["DB_REPLICATION"], tc.want)
			}
		})
	}
}

func TestDeriveEnvVars_ReadConsistencyEnvInjection(t *testing.T) {
	t.Parallel()

	dbConfig := func(readReplicas int, readConsistency string) map[string]any {
		cfg := map[string]any{"readReplicas": readReplicas}
		if readConsistency != "" {
			cfg["readConsistency"] = readConsistency
		}
		return cfg
	}

	tests := []struct {
		name       string
		design     DeploymentDesign
		nodeID     string
		wantHost   string
		wantHosts  string
		wantPort   string
		wantPolicy string
	}{
		{
			name: "app-server injects replica policy",
			design: DeploymentDesign{
				Nodes: []DeploymentNode{
					{ID: "app", Type: "app-server", Name: "App"},
					{ID: "db", Type: "db", Name: "PostgreSQL", Config: dbConfig(1, "replica")},
				},
				Connections: []DeploymentConn{{SourceID: "app", TargetID: "db"}},
			},
			nodeID:     "app",
			wantHost:   "postgres-replica-0",
			wantHosts:  "postgres-replica-0",
			wantPort:   "5432",
			wantPolicy: "replica",
		},
		{
			name: "leader-only omits read replica env",
			design: DeploymentDesign{
				Nodes: []DeploymentNode{
					{ID: "app", Type: "app-server", Name: "App"},
					{ID: "db", Type: "db", Name: "PostgreSQL", Config: dbConfig(1, "leader-only")},
				},
				Connections: []DeploymentConn{{SourceID: "app", TargetID: "db"}},
			},
			nodeID: "app",
		},
		{
			name: "zero read replicas omits read replica env",
			design: DeploymentDesign{
				Nodes: []DeploymentNode{
					{ID: "app", Type: "app-server", Name: "App"},
					{ID: "db", Type: "db", Name: "PostgreSQL", Config: dbConfig(0, "replica")},
				},
				Connections: []DeploymentConn{{SourceID: "app", TargetID: "db"}},
			},
			nodeID: "app",
		},
		{
			name: "monotonic with one replica clamps to ryw in env",
			design: DeploymentDesign{
				Nodes: []DeploymentNode{
					{ID: "app", Type: "app-server", Name: "App"},
					{ID: "db", Type: "db", Name: "PostgreSQL", Config: dbConfig(1, "monotonic")},
				},
				Connections: []DeploymentConn{{SourceID: "app", TargetID: "db"}},
			},
			nodeID:     "app",
			wantHost:   "postgres-replica-0",
			wantHosts:  "postgres-replica-0",
			wantPort:   "5432",
			wantPolicy: "ryw",
		},
		{
			name: "monotonic with two replicas keeps policy",
			design: DeploymentDesign{
				Nodes: []DeploymentNode{
					{ID: "app", Type: "app-server", Name: "App"},
					{ID: "db", Type: "db", Name: "PostgreSQL", Config: dbConfig(2, "monotonic")},
				},
				Connections: []DeploymentConn{{SourceID: "app", TargetID: "db"}},
			},
			nodeID:     "app",
			wantHost:   "postgres-replica-0",
			wantHosts:  "postgres-replica-0,postgres-replica-1",
			wantPort:   "5432",
			wantPolicy: "monotonic",
		},
		{
			name: "mixed instance sizes inject distinct hostnames",
			design: DeploymentDesign{
				Nodes: []DeploymentNode{
					{ID: "app", Type: "app-server", Name: "App"},
					{ID: "db", Type: "db", Name: "PostgreSQL", Config: map[string]any{
						"readReplicas":    2,
						"readConsistency": "replica",
						"instanceSizes":   []any{"large", "large", "small"},
					}},
				},
				Connections: []DeploymentConn{{SourceID: "app", TargetID: "db"}},
			},
			nodeID:     "app",
			wantHost:   "postgres-replica-0",
			wantHosts:  "postgres-replica-0,postgres-replica-small-0",
			wantPort:   "5432",
			wantPolicy: "replica",
		},
		{
			name: "proxy cache injects read replica env",
			design: DeploymentDesign{
				Nodes: []DeploymentNode{
					{ID: "app", Type: "app-server", Name: "App"},
					{
						ID:   "cache",
						Type: "cache",
						Name: "Redis",
						Config: map[string]any{
							"strategy": "write-through",
						},
					},
					{ID: "db", Type: "db", Name: "PostgreSQL", Config: dbConfig(1, "ryw")},
				},
				Connections: []DeploymentConn{
					{SourceID: "app", TargetID: "cache"},
					{SourceID: "cache", TargetID: "db"},
				},
			},
			nodeID:     "cache",
			wantHost:   "postgres-replica-0",
			wantHosts:  "postgres-replica-0",
			wantPort:   "5432",
			wantPolicy: "ryw",
		},
		{
			name: "proxy app-server omits read replica env",
			design: DeploymentDesign{
				Nodes: []DeploymentNode{
					{ID: "app", Type: "app-server", Name: "App"},
					{
						ID:   "cache",
						Type: "cache",
						Name: "Redis",
						Config: map[string]any{
							"strategy": "write-through",
						},
					},
					{ID: "db", Type: "db", Name: "PostgreSQL", Config: dbConfig(1, "ryw")},
				},
				Connections: []DeploymentConn{
					{SourceID: "app", TargetID: "cache"},
					{SourceID: "cache", TargetID: "db"},
				},
			},
			nodeID: "app",
		},
		{
			name: "worker omits read replica env",
			design: DeploymentDesign{
				Nodes: []DeploymentNode{
					{ID: "worker", Type: "worker", Name: "Worker"},
					{ID: "db", Type: "db", Name: "PostgreSQL", Config: dbConfig(1, "replica")},
				},
				Connections: []DeploymentConn{{SourceID: "worker", TargetID: "db"}},
			},
			nodeID: "worker",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			node := nodeByID(tc.design, tc.nodeID)
			if node == nil {
				t.Fatalf("node %q not found", tc.nodeID)
			}
			env := DeriveEnvVars(*node, tc.design)
			if tc.wantHost == "" {
				if _, ok := env["READ_REPLICA_HOST"]; ok {
					t.Fatalf("expected READ_REPLICA_HOST absent, got %q", env["READ_REPLICA_HOST"])
				}
				if _, ok := env["READ_REPLICA_HOSTS"]; ok {
					t.Fatalf("expected READ_REPLICA_HOSTS absent, got %q", env["READ_REPLICA_HOSTS"])
				}
				if _, ok := env["READ_REPLICA_PORT"]; ok {
					t.Fatalf("expected READ_REPLICA_PORT absent, got %q", env["READ_REPLICA_PORT"])
				}
				if _, ok := env["READ_CONSISTENCY"]; ok {
					t.Fatalf("expected READ_CONSISTENCY absent, got %q", env["READ_CONSISTENCY"])
				}
				return
			}
			if env["READ_REPLICA_HOSTS"] != tc.wantHosts {
				t.Fatalf("READ_REPLICA_HOSTS: got %q want %q", env["READ_REPLICA_HOSTS"], tc.wantHosts)
			}
			if env["READ_REPLICA_HOST"] != tc.wantHost {
				t.Fatalf("READ_REPLICA_HOST: got %q want %q", env["READ_REPLICA_HOST"], tc.wantHost)
			}
			if env["READ_REPLICA_PORT"] != tc.wantPort {
				t.Fatalf("READ_REPLICA_PORT: got %q want %q", env["READ_REPLICA_PORT"], tc.wantPort)
			}
			if env["READ_CONSISTENCY"] != tc.wantPolicy {
				t.Fatalf("READ_CONSISTENCY: got %q want %q", env["READ_CONSISTENCY"], tc.wantPolicy)
			}
		})
	}
}
