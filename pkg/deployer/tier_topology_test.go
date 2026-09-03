package deployer

import (
	"strings"
	"testing"
)

func TestValidateTierTopology(t *testing.T) {
	t.Parallel()

	launchFree := TierTopologyLimits{
		MaxAppServerReplicas: 3,
		AllowLoadBalancer:    true,
		AllowMQWorker:        true,
		AllowProxyCache:      true,
	}
	legacyExplorer := TierTopologyLimits{
		MaxAppServerReplicas: 1,
		AllowLoadBalancer:    false,
		AllowMQWorker:        false,
		AllowProxyCache:      false,
	}
	engineer := TierTopologyLimits{
		MaxAppServerReplicas: 3,
		AllowLoadBalancer:    true,
		AllowMQWorker:        true,
		AllowProxyCache:      true,
	}

	tests := []struct {
		name    string
		limits  TierTopologyLimits
		design  DeploymentDesign
		wantErr string
	}{
		{
			name:   "lite design passes launch free tier",
			limits: launchFree,
			design: urlShortenerLiteDesign(),
		},
		{
			name:   "async ecommerce passes launch free tier",
			limits: launchFree,
			design: ecommerceAsyncOnlyDesign(),
		},
		{
			name:   "proxy cache passes launch free tier",
			limits: launchFree,
			design: func() DeploymentDesign {
				d := urlShortenerLiteDesign()
				d.Nodes[1].Config["strategy"] = "write-through"
				return d
			}(),
		},
		{
			name:    "load balancer blocked on legacy explorer",
			limits:  legacyExplorer,
			design:  urlShortenerDefaultDesign(),
			wantErr: "load balancers require",
		},
		{
			name:   "full ecommerce passes engineer tier",
			limits: engineer,
			design: ecommerceFullDesign(),
		},
		{
			name:    "mq blocked on legacy explorer",
			limits:  legacyExplorer,
			design:  ecommerceAsyncOnlyDesign(),
			wantErr: "message queues and workers",
		},
		{
			name:   "proxy cache blocked on legacy explorer",
			limits: legacyExplorer,
			design: func() DeploymentDesign {
				d := urlShortenerLiteDesign()
				d.Nodes[1].Config["strategy"] = "write-through"
				return d
			}(),
			wantErr: "proxy cache strategies require",
		},
		{
			name:   "proxy cache allowed on engineer tier",
			limits: engineer,
			design: func() DeploymentDesign {
				d := urlShortenerLiteDesign()
				d.Nodes[1].Config["strategy"] = "write-through"
				return d
			}(),
		},
		{
			name:   "scaled replicas blocked on legacy explorer",
			limits: legacyExplorer,
			design: func() DeploymentDesign {
				d := urlShortenerLiteDesign()
				d.Nodes[0].Config["replicas"] = 2
				return d
			}(),
			wantErr: "replicas 2 exceed your plan limit",
		},
		{
			name:   "two app replicas pass launch free tier",
			limits: launchFree,
			design: func() DeploymentDesign {
				d := urlShortenerLiteDesign()
				d.Nodes[0].Config["replicas"] = 2
				return d
			}(),
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateTierTopology(tt.design, tt.limits)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateTierTopology() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func ecommerceFullDesign() DeploymentDesign {
	d := DeploymentDesign{
		Template: "ecommerce",
		Nodes: []DeploymentNode{
			{ID: "lb-1", Type: "lb", Name: "Load Balancer"},
			{ID: "app-1", Type: "app-server", Name: "App", Config: map[string]interface{}{"replicas": 1}},
			{ID: "mq-1", Type: "mq", Name: "MQ"},
			{ID: "worker-1", Type: "worker", Name: "Worker"},
			{ID: "cache-1", Type: "cache", Name: "Redis"},
			{ID: "db-1", Type: "db", Name: "PostgreSQL"},
		},
	}
	NormalizeDesign(&d)
	return d
}

// ecommerceAsyncOnlyDesign is full async stack without a load balancer (lite topology + mq/worker).
func ecommerceAsyncOnlyDesign() DeploymentDesign {
	d := urlShortenerLiteDesign()
	d.Template = "ecommerce"
	d.Nodes = append(d.Nodes,
		DeploymentNode{ID: "mq-1", Type: "mq", Name: "MQ"},
		DeploymentNode{ID: "worker-1", Type: "worker", Name: "Worker"},
	)
	NormalizeDesign(&d)
	return d
}
