package deployer

import (
	"strings"
	"testing"

	"github.com/ywacch/cloud9/pkg/templates"
)

func TestValidateRuntimeContract_Table(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		design     DeploymentDesign
		templateID string
		wantErr    string
	}{
		{
			name:       "lb requires app backends",
			templateID: "url-shortener",
			design: DeploymentDesign{
				Template: "url-shortener",
				Nodes: []DeploymentNode{
					{ID: "lb1", Type: "lb", Name: "Load Balancer"},
					{ID: "db1", Type: "db", Name: "PostgreSQL"},
				},
				Connections: nil,
			},
			wantErr: "nowhere to send traffic",
		},
		{
			name:       "proxy strategy requires proxy queries",
			templateID: "social-feed",
			design: DeploymentDesign{
				Template: "social-feed",
				Nodes: []DeploymentNode{
					{ID: "app1", Type: "app-server", Name: "App Server"},
					{ID: "cache1", Type: "cache", Name: "Redis", Config: map[string]interface{}{"strategy": "write-through"}},
					{ID: "db1", Type: "db", Name: "PostgreSQL"},
				},
				Connections: []DeploymentConn{
					{SourceID: "app1", TargetID: "cache1"},
					{SourceID: "cache1", TargetID: "db1"},
				},
			},
			wantErr: "needs query rules for cache misses",
		},
		{
			name:       "ecommerce app requires direct db env",
			templateID: "ecommerce",
			design: DeploymentDesign{
				Template: "ecommerce",
				Nodes: []DeploymentNode{
					{ID: "app1", Type: "app-server", Name: "App Server"},
					{ID: "cache1", Type: "cache", Name: "Redis", Config: map[string]interface{}{"strategy": "write-through"}},
					{ID: "db1", Type: "db", Name: "PostgreSQL"},
				},
				Connections: []DeploymentConn{
					{SourceID: "app1", TargetID: "cache1"},
					{SourceID: "cache1", TargetID: "db1"},
				},
			},
			wantErr: "needs a direct database connection",
		},
		{
			name:       "url shortener allows proxy mode without app db",
			templateID: "url-shortener",
			design: DeploymentDesign{
				Template: "url-shortener",
				Nodes: []DeploymentNode{
					{ID: "app1", Type: "app-server", Name: "App Server"},
					{ID: "cache1", Type: "cache", Name: "Redis", Config: map[string]interface{}{"strategy": "write-through"}},
					{ID: "db1", Type: "db", Name: "PostgreSQL"},
				},
				Connections: []DeploymentConn{
					{SourceID: "app1", TargetID: "cache1"},
					{SourceID: "cache1", TargetID: "db1"},
				},
			},
		},
		{
			name:       "worker requires db env safety net",
			templateID: "ecommerce",
			design: DeploymentDesign{
				Template: "ecommerce",
				Nodes: []DeploymentNode{
					{ID: "w1", Type: "worker", Name: "Worker"},
					{ID: "mq1", Type: "mq", Name: "MQ"},
					{ID: "db1", Type: "db", Name: "DB"},
				},
				Connections: []DeploymentConn{
					{SourceID: "w1", TargetID: "mq1"},
				},
			},
			wantErr: "needs a database connection",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tmpl := templates.GetTemplate(tc.templateID)
			if tmpl == nil {
				t.Fatalf("template %q not found", tc.templateID)
			}
			err := ValidateRuntimeContract(tc.design, tmpl)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("expected nil error, got %v", err)
			}
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error to contain %q, got %q", tc.wantErr, err.Error())
				}
			}
		})
	}
}
