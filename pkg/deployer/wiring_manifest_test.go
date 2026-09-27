package deployer

import (
	"strings"
	"testing"
)

func TestWiringManifest_OmitsCredentials(t *testing.T) {
	t.Parallel()
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "a", Type: "app-server", Name: "API"},
			{ID: "d", Type: "db", Name: "PostgreSQL", Config: map[string]any{
				"readReplicas":    float64(1),
				"readConsistency": "replica",
			}},
		},
		Connections: []DeploymentConn{
			{SourceID: "a", TargetID: "d", Protocol: "TCP"},
		},
	}
	links := WiringLinksFromDesign(&design)
	if len(links) != 1 {
		t.Fatalf("links: %+v", links)
	}
	for key := range links[0].Fields {
		if isCredentialEnvKey(key) || strings.Contains(strings.ToUpper(key), "PASSWORD") || strings.Contains(strings.ToUpper(key), "_USER") {
			t.Fatalf("credential key leaked: %q", key)
		}
	}
	if links[0].Fields["DB_HOST"] != "db" {
		t.Fatalf("fields: %+v", links[0].Fields)
	}
	if _, ok := links[0].Fields["DB_USER"]; ok {
		t.Fatal("DB_USER should not appear in wiring fields")
	}
	if _, ok := links[0].Fields["DB_PASSWORD"]; ok {
		t.Fatal("DB_PASSWORD should not appear in wiring fields")
	}
}

func TestWiringManifest_ProxyTopologyHidesDBFromApp(t *testing.T) {
	t.Parallel()
	design := DeploymentDesign{
		Nodes: []DeploymentNode{
			{ID: "a", Type: "app-server", Name: "API"},
			{ID: "c", Type: "cache", Name: "Redis", Config: map[string]any{"strategy": "write-through"}},
			{ID: "d", Type: "db", Name: "PostgreSQL"},
		},
		Connections: []DeploymentConn{
			{SourceID: "a", TargetID: "c", Protocol: "TCP"},
			{SourceID: "c", TargetID: "d", Protocol: "TCP"},
		},
	}
	links := WiringLinksFromDesign(&design)
	for _, link := range links {
		if link.SourceID == "a" && link.TargetType == "db" {
			t.Fatalf("app should not have db wiring link: %+v", link)
		}
		if link.SourceID == "a" {
			if _, ok := link.Fields["DB_HOST"]; ok {
				t.Fatalf("app wiring must not claim DB_HOST: %+v", link.Fields)
			}
		}
	}
	var sawCacheProxy bool
	for _, link := range links {
		if link.SourceID == "a" && link.TargetID == "c" {
			sawCacheProxy = true
			if link.Fields["CACHE_MODE"] != "proxy" {
				t.Fatalf("cache link: %+v", link.Fields)
			}
		}
		if link.SourceID == "c" && link.TargetID == "d" {
			if link.Fields["DB_HOST"] != "db" {
				t.Fatalf("proxy cache db link: %+v", link.Fields)
			}
		}
	}
	if !sawCacheProxy {
		t.Fatalf("links: %+v", links)
	}
}
