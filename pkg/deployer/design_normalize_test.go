package deployer

import "testing"

func TestNormalizeDesign(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		in       DeploymentDesign
		wantType string
		wantAlgo string
		hasAlgo  bool
	}{
		{
			name: "load_balancer to lb",
			in: DeploymentDesign{
				Nodes: []DeploymentNode{
					{ID: "1", Type: "load_balancer", Name: "LB", Config: map[string]interface{}{"algorithm": "least_conn"}},
				},
			},
			wantType: "lb",
			wantAlgo: "least_conn",
			hasAlgo:  true,
		},
		{
			name: "compute to app-server",
			in: DeploymentDesign{
				Nodes: []DeploymentNode{
					{ID: "1", Type: "compute", Name: "API", Config: map[string]interface{}{"replicas": 2}},
				},
			},
			wantType: "app-server",
		},
		{
			name: "lbAlgorithm preserved over algorithm alias",
			in: DeploymentDesign{
				Nodes: []DeploymentNode{
					{
						ID:   "1",
						Type: "load_balancer",
						Name: "LB",
						Config: map[string]interface{}{
							"algorithm":   "ip_hash",
							"lbAlgorithm": "round-robin",
						},
					},
				},
			},
			wantType: "lb",
			wantAlgo: "round-robin",
			hasAlgo:  true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			d := tt.in
			NormalizeDesign(&d)
			if len(d.Nodes) != 1 {
				t.Fatalf("nodes: got %d want 1", len(d.Nodes))
			}
			if d.Nodes[0].Type != tt.wantType {
				t.Fatalf("type: got %q want %q", d.Nodes[0].Type, tt.wantType)
			}
			if tt.hasAlgo {
				got := configString(d.Nodes[0].Config, "lbAlgorithm", "")
				if got != tt.wantAlgo {
					t.Fatalf("lbAlgorithm: got %q want %q", got, tt.wantAlgo)
				}
				if _, ok := d.Nodes[0].Config["algorithm"]; ok {
					t.Fatal("algorithm alias should be removed after normalize")
				}
			}
		})
	}
}
