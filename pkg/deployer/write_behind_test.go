package deployer

import "testing"

func TestNormalizeWriteBehindIntervalSeconds(t *testing.T) {
	tests := []struct {
		name   string
		config map[string]any
		want   string
	}{
		{name: "absent config", config: nil, want: "30"},
		{name: "absent key", config: map[string]any{}, want: "30"},
		{name: "valid mid-range", config: map[string]any{"writeBehindIntervalSeconds": float64(45)}, want: "45"},
		{name: "min bound", config: map[string]any{"writeBehindIntervalSeconds": float64(1)}, want: "1"},
		{name: "max bound", config: map[string]any{"writeBehindIntervalSeconds": float64(300)}, want: "300"},
		{name: "zero falls back", config: map[string]any{"writeBehindIntervalSeconds": float64(0)}, want: "30"},
		{name: "negative falls back", config: map[string]any{"writeBehindIntervalSeconds": float64(-5)}, want: "30"},
		{name: "over max clamps", config: map[string]any{"writeBehindIntervalSeconds": float64(999)}, want: "300"},
		{name: "string form accepted", config: map[string]any{"writeBehindIntervalSeconds": "90"}, want: "90"},
		{name: "malformed falls back", config: map[string]any{"writeBehindIntervalSeconds": "soon"}, want: "30"},
		{name: "fractional falls back", config: map[string]any{"writeBehindIntervalSeconds": float64(1.5)}, want: "30"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeWriteBehindIntervalSeconds(tt.config); got != tt.want {
				t.Fatalf("normalizeWriteBehindIntervalSeconds = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDeriveEnvVars_WriteBehindIntervalClamp(t *testing.T) {
	tests := []struct {
		name     string
		interval any
		want     string
	}{
		{name: "valid value propagates", interval: float64(45), want: "45"},
		{name: "zero becomes default", interval: float64(0), want: "30"},
		{name: "oversize clamps to max", interval: float64(500), want: "300"},
		{name: "unset uses default", interval: nil, want: "30"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := map[string]any{"strategy": "write-behind"}
			if tt.interval != nil {
				cfg["writeBehindIntervalSeconds"] = tt.interval
			}
			design := DeploymentDesign{
				Nodes: []DeploymentNode{
					{ID: "a", Type: "app-server", Name: "API Server"},
					{ID: "c", Type: "cache", Name: "Redis", Config: cfg},
					{ID: "d", Type: "db", Name: "PostgreSQL"},
				},
				Connections: []DeploymentConn{
					{SourceID: "a", TargetID: "c", Protocol: "TCP"},
					{SourceID: "c", TargetID: "d", Protocol: "TCP"},
				},
			}
			env := DeriveEnvVars(design.Nodes[1], design)
			if got := env["WRITE_BEHIND_INTERVAL"]; got != tt.want {
				t.Fatalf("WRITE_BEHIND_INTERVAL = %q, want %q", got, tt.want)
			}
		})
	}
}
