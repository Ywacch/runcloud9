package deployer

import (
	"strings"
	"testing"
)

func TestValidateInstanceSizeForTier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		maxSize string
		design  DeploymentDesign
		wantErr string
	}{
		{
			name:    "small design passes medium cap",
			maxSize: "medium",
			design:  urlShortenerLiteDesign(),
		},
		{
			name:    "large app blocked on medium cap",
			maxSize: "medium",
			design: func() DeploymentDesign {
				d := urlShortenerLiteDesign()
				d.Nodes[0].Config["instanceSize"] = "large"
				return d
			}(),
			wantErr: "instance size large exceeds your plan limit of medium",
		},
		{
			name:    "large allowed when cap is large",
			maxSize: "large",
			design: func() DeploymentDesign {
				d := urlShortenerLiteDesign()
				d.Nodes[0].Config["instanceSize"] = "large"
				return d
			}(),
		},
		{
			name:    "per-replica large blocked",
			maxSize: "medium",
			design: func() DeploymentDesign {
				d := urlShortenerLiteDesign()
				d.Nodes[0].Config["replicas"] = 2
				d.Nodes[0].Config["instanceSizes"] = []any{"small", "large"}
				return d
			}(),
			wantErr: "instance size large exceeds your plan limit of medium",
		},
		{
			name:    "proxy profile large blocked",
			maxSize: "medium",
			design: func() DeploymentDesign {
				d := urlShortenerLiteDesign()
				d.Nodes[1].Config["strategy"] = "write-through"
				d.Nodes[1].Config["proxyResourceProfile"] = "large"
				return d
			}(),
			wantErr: "instance size large exceeds your plan limit of medium",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateInstanceSizeForTier(tt.design, tt.maxSize)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateInstanceSizeForTier() = %v, want nil", err)
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
