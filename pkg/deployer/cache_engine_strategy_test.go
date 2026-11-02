package deployer

import (
	"testing"

	"github.com/ywacch/cloud9/pkg/templates"
)

func TestValidateCacheEngineStrategy_ProxyRequiresRedis(t *testing.T) {
	t.Parallel()
	tmpl := templates.GetTemplate("url-shortener")
	if tmpl == nil {
		t.Fatal("template not found")
	}
	design := DeploymentDesign{
		Template: "url-shortener",
		Nodes: []DeploymentNode{
			{ID: "app1", Type: "app-server", Name: "App"},
			{ID: "c1", Type: "cache", Name: "Cache", Config: map[string]interface{}{
				"engine":   "memcached",
				"strategy": "write-through",
			}},
			{ID: "db1", Type: "db", Name: "DB"},
		},
		Connections: []DeploymentConn{
			{SourceID: "app1", TargetID: "c1"},
			{SourceID: "c1", TargetID: "db1"},
		},
	}
	err := ValidateRuntimeContract(design, tmpl)
	if err == nil {
		t.Fatal("expected error")
	}
}
