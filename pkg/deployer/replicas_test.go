package deployer

import "testing"

func TestParseReplicaCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  map[string]any
		def  int
		want int32
	}{
		{name: "default when nil config", cfg: nil, def: 1, want: 1},
		{name: "int value", cfg: map[string]any{"replicas": 2}, def: 1, want: 2},
		{name: "string value", cfg: map[string]any{"replicas": "3"}, def: 1, want: 3},
		{name: "invalid string", cfg: map[string]any{"replicas": "x"}, def: 1, want: 1},
		{name: "zero falls back", cfg: map[string]any{"replicas": 0}, def: 1, want: 1},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := parseReplicaCount(tc.cfg, tc.def); got != tc.want {
				t.Fatalf("parseReplicaCount() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestReplicaCountFromConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     map[string]any
		min     int
		max     int
		want    int32
		wantErr bool
	}{
		{name: "in range", cfg: map[string]any{"replicas": 2}, min: 1, max: 3, want: 2},
		{name: "below min", cfg: map[string]any{"replicas": 0}, min: 1, max: 3, wantErr: true},
		{name: "above max", cfg: map[string]any{"replicas": 4}, min: 1, max: 3, wantErr: true},
		{name: "not integer", cfg: map[string]any{"replicas": "many"}, min: 1, max: 3, wantErr: true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := replicaCountFromConfig(tc.cfg, 1, tc.min, tc.max)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %d want %d", got, tc.want)
			}
		})
	}
}
