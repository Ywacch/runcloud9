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

func TestParseReadReplicaCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  map[string]any
		want int32
	}{
		{name: "default when nil config", cfg: nil, want: 0},
		{name: "int value", cfg: map[string]any{"readReplicas": 2}, want: 2},
		{name: "string value", cfg: map[string]any{"readReplicas": "1"}, want: 1},
		{name: "invalid string", cfg: map[string]any{"readReplicas": "x"}, want: 0},
		{name: "negative falls back", cfg: map[string]any{"readReplicas": -1}, want: 0},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := parseReadReplicaCount(tc.cfg); got != tc.want {
				t.Fatalf("parseReadReplicaCount() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestReadReplicaCountFromConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     map[string]any
		want    int32
		wantErr bool
	}{
		{name: "zero allowed", cfg: map[string]any{"readReplicas": 0}, want: 0},
		{name: "in range", cfg: map[string]any{"readReplicas": 2}, want: 2},
		{name: "above max", cfg: map[string]any{"readReplicas": 3}, wantErr: true},
		{name: "below min negative", cfg: map[string]any{"readReplicas": -1}, wantErr: true},
		{name: "not integer", cfg: map[string]any{"readReplicas": "many"}, wantErr: true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := readReplicaCountFromConfig(tc.cfg, minReadReplicas, maxReadReplicas)
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

func TestParseReplicationMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     map[string]any
		want    string
		wantErr bool
	}{
		{name: "default async", cfg: nil, want: "async"},
		{name: "none maps async", cfg: map[string]any{"replicationMode": "none"}, want: "async"},
		{name: "sync", cfg: map[string]any{"replicationMode": "sync"}, want: "sync"},
		{name: "synchronous alias", cfg: map[string]any{"replicationMode": "synchronous"}, want: "sync"},
		{name: "asynchronous alias", cfg: map[string]any{"replicationMode": "asynchronous"}, want: "async"},
		{name: "legacy read-replica", cfg: map[string]any{"replicationMode": "read-replica"}, want: "async"},
		{name: "unknown rejected", cfg: map[string]any{"replicationMode": "quorum"}, wantErr: true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseReplicationMode(tc.cfg)
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
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestDBEngineSupportsStreamingReplicas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		engine string
		want   bool
	}{
		{engine: "", want: true},
		{engine: "postgresql", want: true},
		{engine: "postgres", want: true},
		{engine: "PostgreSQL", want: true},
		{engine: "mysql", want: false},
		{engine: "mongodb", want: false},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.engine, func(t *testing.T) {
			t.Parallel()
			if got := dbEngineSupportsStreamingReplicas(tc.engine); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}
