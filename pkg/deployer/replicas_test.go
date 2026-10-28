package deployer

import "testing"

func TestSeedReplicaReadyBeforeRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		mode         string
		replicaCount int32
		wantMinReady int32
	}{
		{name: "async zero replicas", mode: "async", replicaCount: 0, wantMinReady: 0},
		{name: "async with replicas", mode: "async", replicaCount: 2, wantMinReady: 0},
		{name: "semi-sync one standby", mode: "semi-sync", replicaCount: 3, wantMinReady: 1},
		{name: "sync all standbys", mode: "sync", replicaCount: 2, wantMinReady: 2},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := seedReplicaReadyBeforeRestore(tc.mode, tc.replicaCount); got != tc.wantMinReady {
				t.Fatalf("seedReplicaReadyBeforeRestore(%q, %d) = %d, want %d", tc.mode, tc.replicaCount, got, tc.wantMinReady)
			}
		})
	}
}

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
		{name: "semi-sync", cfg: map[string]any{"replicationMode": "semi-sync"}, want: "semi-sync"},
		{name: "semi_sync alias", cfg: map[string]any{"replicationMode": "semi_sync"}, want: "semi-sync"},
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

func TestSyncCommitLevelFromConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     map[string]any
		want    string
		wantErr bool
	}{
		{name: "default on", cfg: nil, want: "on"},
		{name: "remote_write", cfg: map[string]any{"syncCommitLevel": "remote_write"}, want: "remote_write"},
		{name: "remote_apply", cfg: map[string]any{"syncCommitLevel": "remote_apply"}, want: "remote_apply"},
		{name: "unknown rejected", cfg: map[string]any{"syncCommitLevel": "quorum"}, wantErr: true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := syncCommitLevelFromConfig(tc.cfg)
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

func TestParseReadConsistency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  map[string]any
		want string
	}{
		{name: "default leader-only", cfg: nil, want: "leader-only"},
		{name: "explicit replica", cfg: map[string]any{"readConsistency": "replica"}, want: "replica"},
		{name: "trim and lower", cfg: map[string]any{"readConsistency": " RYW "}, want: "ryw"},
		{name: "ryw+monotonic", cfg: map[string]any{"readConsistency": "ryw+monotonic"}, want: "ryw+monotonic"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := parseReadConsistency(tc.cfg); got != tc.want {
				t.Fatalf("parseReadConsistency() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestReadConsistencyFromConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     map[string]any
		want    string
		wantErr bool
	}{
		{name: "default leader-only", cfg: nil, want: "leader-only"},
		{name: "monotonic valid", cfg: map[string]any{"readConsistency": "monotonic"}, want: "monotonic"},
		{name: "unknown rejected", cfg: map[string]any{"readConsistency": "quorum"}, wantErr: true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := readConsistencyFromConfig(tc.cfg)
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

func TestEffectiveReadConsistency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		cfg          map[string]any
		readReplicas int32
		want         string
	}{
		{name: "leader-only unchanged", cfg: map[string]any{"readConsistency": "leader-only"}, readReplicas: 1, want: "leader-only"},
		{name: "replica with one standby", cfg: map[string]any{"readConsistency": "replica"}, readReplicas: 1, want: "replica"},
		{name: "sticky with one standby", cfg: map[string]any{"readConsistency": "sticky"}, readReplicas: 1, want: "sticky"},
		{name: "monotonic clamps with one standby", cfg: map[string]any{"readConsistency": "monotonic"}, readReplicas: 1, want: "ryw"},
		{name: "ryw+monotonic clamps with one standby", cfg: map[string]any{"readConsistency": "ryw+monotonic"}, readReplicas: 1, want: "ryw"},
		{name: "monotonic allowed with two standbys", cfg: map[string]any{"readConsistency": "monotonic"}, readReplicas: 2, want: "monotonic"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := effectiveReadConsistency(tc.cfg, tc.readReplicas); got != tc.want {
				t.Fatalf("effectiveReadConsistency() = %q, want %q", got, tc.want)
			}
		})
	}
}
