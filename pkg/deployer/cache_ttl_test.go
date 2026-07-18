package deployer

import "testing"

func TestNormalizeCacheTTLSeconds(t *testing.T) {
	tests := []struct {
		name   string
		config map[string]any
		want   string
		wantOK bool
	}{
		{name: "absent config", config: nil, wantOK: false},
		{name: "absent key", config: map[string]any{}, wantOK: false},
		{name: "zero means no expiry", config: map[string]any{"cacheTtlSeconds": float64(0)}, want: "0", wantOK: true},
		{name: "timed expiry", config: map[string]any{"cacheTtlSeconds": float64(120)}, want: "120", wantOK: true},
		{name: "max bound", config: map[string]any{"cacheTtlSeconds": float64(86400)}, want: "86400", wantOK: true},
		{name: "over max is invalid", config: map[string]any{"cacheTtlSeconds": float64(86401)}, wantOK: false},
		{name: "negative is invalid", config: map[string]any{"cacheTtlSeconds": float64(-5)}, wantOK: false},
		{name: "string form accepted", config: map[string]any{"cacheTtlSeconds": "45"}, want: "45", wantOK: true},
		{name: "malformed string is invalid", config: map[string]any{"cacheTtlSeconds": "soon"}, wantOK: false},
		{name: "fractional is invalid", config: map[string]any{"cacheTtlSeconds": float64(1.5)}, wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := normalizeCacheTTLSeconds(tt.config)
			if ok != tt.wantOK {
				t.Fatalf("normalizeCacheTTLSeconds ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && got != tt.want {
				t.Fatalf("normalizeCacheTTLSeconds = %q, want %q", got, tt.want)
			}
		})
	}
}

// cacheTTLDesign builds an app -> cache (+ optional worker -> cache, cache -> db)
// design with the given per-node configs.
func cacheTTLDesign(appConfig, cacheConfig map[string]any) DeploymentDesign {
	return DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "a", Type: "app-server", Name: "API Server", Config: appConfig},
			{ID: "c", Type: "cache", Name: "Redis", Config: cacheConfig},
			{ID: "d", Type: "db", Name: "PostgreSQL"},
			{ID: "w", Type: "worker", Name: "Fanout Worker"},
		},
		Connections: []DeploymentConn{
			{SourceID: "a", TargetID: "c", Protocol: "TCP"},
			{SourceID: "a", TargetID: "d", Protocol: "TCP"},
			{SourceID: "c", TargetID: "d", Protocol: "TCP"},
			{SourceID: "w", TargetID: "c", Protocol: "TCP"},
		},
	}
}

func TestDeriveEnvVars_CacheTTLOwnership(t *testing.T) {
	tests := []struct {
		name        string
		appConfig   map[string]any
		cacheConfig map[string]any
		node        string // node ID whose env we derive
		wantTTL     string
		wantPresent bool
	}{
		{
			name:        "cache-aside app: cache node owns TTL",
			appConfig:   map[string]any{"cacheTtlSeconds": float64(60)},
			cacheConfig: map[string]any{"strategy": "cache-aside", "cacheTtlSeconds": float64(120)},
			node:        "a",
			wantTTL:     "120",
			wantPresent: true,
		},
		{
			name:        "cache-aside app: legacy app value when cache node unset",
			appConfig:   map[string]any{"cacheTtlSeconds": float64(60)},
			cacheConfig: map[string]any{"strategy": "cache-aside"},
			node:        "a",
			wantTTL:     "60",
			wantPresent: true,
		},
		{
			name:        "cache-aside app: no value anywhere leaves CACHE_TTL unset",
			appConfig:   nil,
			cacheConfig: map[string]any{"strategy": "cache-aside"},
			node:        "a",
			wantPresent: false,
		},
		{
			name:        "cache-aside app: zero propagates as no expiry",
			appConfig:   nil,
			cacheConfig: map[string]any{"strategy": "cache-aside", "cacheTtlSeconds": float64(0)},
			node:        "a",
			wantTTL:     "0",
			wantPresent: true,
		},
		{
			name:        "cache-aside app: invalid cache value falls back to legacy app value",
			appConfig:   map[string]any{"cacheTtlSeconds": float64(60)},
			cacheConfig: map[string]any{"strategy": "cache-aside", "cacheTtlSeconds": float64(-1)},
			node:        "a",
			wantTTL:     "60",
			wantPresent: true,
		},
		{
			name:        "proxy cache node: own config value",
			cacheConfig: map[string]any{"strategy": "write-through", "cacheTtlSeconds": float64(45)},
			node:        "c",
			wantTTL:     "45",
			wantPresent: true,
		},
		{
			name:        "proxy cache node: unset leaves CACHE_TTL to image default",
			cacheConfig: map[string]any{"strategy": "write-through"},
			node:        "c",
			wantPresent: false,
		},
		{
			name:        "proxy-mode app server gets TTL from the proxy cache node",
			cacheConfig: map[string]any{"strategy": "write-behind", "cacheTtlSeconds": float64(90)},
			node:        "a",
			wantTTL:     "90",
			wantPresent: true,
		},
		{
			name:        "proxy-mode app server: no expiry syncs to app writes too",
			cacheConfig: map[string]any{"strategy": "write-through", "cacheTtlSeconds": float64(0)},
			node:        "a",
			wantTTL:     "0",
			wantPresent: true,
		},
		{
			name:        "worker inherits TTL from the cache node it writes to",
			cacheConfig: map[string]any{"strategy": "cache-aside", "cacheTtlSeconds": float64(300)},
			node:        "w",
			wantTTL:     "300",
			wantPresent: true,
		},
		{
			name:        "worker: unset cache TTL leaves CACHE_TTL to image default",
			cacheConfig: map[string]any{"strategy": "cache-aside"},
			node:        "w",
			wantPresent: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			design := cacheTTLDesign(tt.appConfig, tt.cacheConfig)
			var node DeploymentNode
			for _, n := range design.Nodes {
				if n.ID == tt.node {
					node = n
				}
			}
			env := DeriveEnvVars(node, design)

			got, present := env["CACHE_TTL"]
			if present != tt.wantPresent {
				t.Fatalf("CACHE_TTL present = %v, want %v (env=%v)", present, tt.wantPresent, env)
			}
			if present && got != tt.wantTTL {
				t.Fatalf("CACHE_TTL = %q, want %q", got, tt.wantTTL)
			}
		})
	}
}
