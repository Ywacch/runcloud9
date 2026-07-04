package deployer

import (
	"testing"
)

func TestResourceProfile_tiers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		profile string
		wantCPU string
		wantMem string
	}{
		{profile: "small", wantCPU: "125m", wantMem: "256Mi"},
		{profile: "alpha", wantCPU: "125m", wantMem: "256Mi"},
		{profile: "medium", wantCPU: "250m", wantMem: "512Mi"},
		{profile: "large", wantCPU: "500m", wantMem: "1Gi"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.profile, func(t *testing.T) {
			t.Parallel()
			got := ResourceProfile(tt.profile)
			if got.Limits.Cpu().String() != tt.wantCPU {
				t.Fatalf("cpu: got %s want %s", got.Limits.Cpu(), tt.wantCPU)
			}
			if got.Limits.Memory().String() != tt.wantMem {
				t.Fatalf("memory: got %s want %s", got.Limits.Memory(), tt.wantMem)
			}
		})
	}
}
