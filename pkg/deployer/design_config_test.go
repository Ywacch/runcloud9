package deployer

import "testing"

func TestDesignConfigFromDesign_Table(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		design DeploymentDesign
		check  func(t *testing.T, got DesignConfig)
	}{
		{
			name: "url-shortener proxy cache",
			design: DeploymentDesign{
				Template: "url-shortener",
				Nodes: []DeploymentNode{
					{ID: "lb1", Type: "lb", Name: "LB"},
					{ID: "app1", Type: "app-server", Name: "API", Config: map[string]interface{}{"replicas": 2}},
					{ID: "cache1", Type: "cache", Name: "Redis", Config: map[string]interface{}{
						"strategy":                   "write-through",
						"cacheTtlSeconds":            300,
						"evictionPolicy":             "allkeys-lfu",
						"writeBehindIntervalSeconds": 60,
					}},
					{ID: "db1", Type: "db", Name: "PostgreSQL"},
				},
			},
			check: func(t *testing.T, got DesignConfig) {
				if got.Template != "url-shortener" {
					t.Fatalf("template: %q", got.Template)
				}
				cache := got.Roles["cache"][0]
				if cache.Engine != "redis" || cache.Settings["cacheStrategy"] != "write-through" {
					t.Fatalf("cache: %+v", cache)
				}
				if cache.Settings["cacheTtlSeconds"] != 300 {
					t.Fatalf("cacheTtlSeconds: %+v", cache.Settings)
				}
				if cache.Settings["evictionPolicy"] != "allkeys-lfu" {
					t.Fatalf("evictionPolicy: %+v", cache.Settings)
				}
				if cache.Settings["writeBehindIntervalSeconds"] != 60 {
					t.Fatalf("writeBehindIntervalSeconds: %+v", cache.Settings)
				}
				app := got.Roles["app-server"][0]
				if app.Settings["replicas"] != 2 {
					t.Fatalf("app replicas: %+v", app.Settings)
				}
				lb := got.Roles["lb"][0]
				if lb.Settings["lbAlgorithm"] != lbAlgoRoundRobin {
					t.Fatalf("lb algorithm: %+v", lb.Settings)
				}
			},
		},
		{
			name: "db with replicas",
			design: DeploymentDesign{
				Template: "social-feed",
				Nodes: []DeploymentNode{
					{ID: "db1", Type: "db", Name: "PostgreSQL", Config: map[string]interface{}{
						"engine":          "postgresql",
						"replicationMode": "semi-sync",
						"syncCommitLevel": "remote_apply",
						"readReplicas":    2,
						"readConsistency": "ryw+monotonic",
						"instanceSize":    "medium",
					}},
				},
			},
			check: func(t *testing.T, got DesignConfig) {
				db := got.Roles["db"][0]
				if db.Engine != "postgresql" {
					t.Fatalf("engine: %q", db.Engine)
				}
				if db.Settings["replicationMode"] != "semi-sync" {
					t.Fatalf("replicationMode: %+v", db.Settings)
				}
				if db.Settings["syncCommitLevel"] != "remote_apply" {
					t.Fatalf("syncCommitLevel: %+v", db.Settings)
				}
				if db.Settings["readReplicas"] != 2 {
					t.Fatalf("readReplicas: %+v", db.Settings)
				}
				if db.Settings["readConsistency"] != "ryw+monotonic" {
					t.Fatalf("readConsistency: %+v", db.Settings)
				}
				if db.Settings["resourceProfile"] != "medium" {
					t.Fatalf("resourceProfile: %+v", db.Settings)
				}
			},
		},
		{
			name: "ecommerce inventory and worker",
			design: DeploymentDesign{
				Template: "ecommerce",
				Nodes: []DeploymentNode{
					{ID: "app1", Type: "app-server", Name: "Shop API", Config: map[string]interface{}{
						"inventoryMode": "pessimistic",
						"purchaseMode":  "safe",
					}},
					{ID: "w1", Type: "worker", Name: "Worker", Config: map[string]interface{}{
						"workerPrefetch":    10,
						"workerConcurrency": 4,
					}},
					{ID: "lb1", Type: "lb", Name: "LB", Config: map[string]interface{}{
						"lbAlgorithm":          "least_conn",
						"rateLimitPerIp":       25,
						"weightByInstanceSize": true,
					}},
				},
			},
			check: func(t *testing.T, got DesignConfig) {
				app := got.Roles["app-server"][0]
				if app.Settings["inventoryMode"] != "pessimistic" || app.Settings["purchaseMode"] != "safe" {
					t.Fatalf("app settings: %+v", app.Settings)
				}
				worker := got.Roles["worker"][0]
				if worker.Settings["workerPrefetch"] != 10 || worker.Settings["workerConcurrency"] != 4 {
					t.Fatalf("worker settings: %+v", worker.Settings)
				}
				lb := got.Roles["lb"][0]
				if lb.Settings["lbAlgorithm"] != lbAlgoLeastConn {
					t.Fatalf("lbAlgorithm: %+v", lb.Settings)
				}
				if lb.Settings["rateLimitPerIp"] != 25 {
					t.Fatalf("rateLimitPerIp: %+v", lb.Settings)
				}
				if lb.Settings["weightByInstanceSize"] != true {
					t.Fatalf("weightByInstanceSize: %+v", lb.Settings)
				}
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := DesignConfigFromDesign(&tt.design)
			tt.check(t, got)
		})
	}
}

func TestDesignConfigFromDesign_OmitsUnsetKeys(t *testing.T) {
	t.Parallel()
	d := &DeploymentDesign{
		Template: "url-shortener",
		Nodes: []DeploymentNode{
			{ID: "app1", Type: "app-server", Name: "API"},
			{ID: "db1", Type: "db", Name: "PostgreSQL"},
		},
	}
	got := DesignConfigFromDesign(d)
	app := got.Roles["app-server"][0]
	for _, key := range []string{"purchaseMode", "inventoryMode", "stampedeProtection", "readConsistency"} {
		if _, ok := app.Settings[key]; ok {
			t.Fatalf("app-server should not include %q, got %+v", key, app.Settings)
		}
	}
	db := got.Roles["db"][0]
	for _, key := range []string{"readReplicas", "replicationMode", "syncCommitLevel", "readConsistency"} {
		if _, ok := db.Settings[key]; ok {
			t.Fatalf("db should not include %q, got %+v", key, db.Settings)
		}
	}
}

func TestDesignConfigFromDesign_MultipleInstancesUnderRole(t *testing.T) {
	t.Parallel()
	d := &DeploymentDesign{
		Template: "url-shortener",
		Nodes: []DeploymentNode{
			{ID: "app-a", Type: "app-server", Name: "API A", Config: map[string]interface{}{"replicas": 2}},
			{ID: "app-b", Type: "app-server", Name: "API B", Config: map[string]interface{}{"replicas": 3, "instanceSize": "large"}},
		},
	}
	got := DesignConfigFromDesign(d)
	apps := got.Roles["app-server"]
	if len(apps) != 2 {
		t.Fatalf("want 2 app-server instances, got %d", len(apps))
	}
	byID := map[string]InstanceConfig{apps[0].ID: apps[0], apps[1].ID: apps[1]}
	if byID["app-a"].Settings["replicas"] != 2 {
		t.Fatalf("app-a replicas: %+v", byID["app-a"].Settings)
	}
	if byID["app-b"].Settings["replicas"] != 3 {
		t.Fatalf("app-b replicas: %+v", byID["app-b"].Settings)
	}
	if byID["app-b"].Settings["resourceProfile"] != "large" {
		t.Fatalf("app-b resourceProfile: %+v", byID["app-b"].Settings)
	}
}
