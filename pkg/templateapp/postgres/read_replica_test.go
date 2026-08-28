package postgres

import "testing"

func TestReadReplicaHostsFromEnv(t *testing.T) {
	// Cannot use t.Parallel with t.Setenv (Go 1.25).
	tests := []struct {
		name     string
		env      string
		fallback string
		want     []string
	}{
		{name: "comma list", env: "postgres-replica-0, postgres-replica-1", want: []string{"postgres-replica-0", "postgres-replica-1"}},
		{name: "fallback", fallback: "db-replica", want: []string{"db-replica"}},
		{name: "empty list falls back", env: " , ", fallback: "postgres-replica-0", want: []string{"postgres-replica-0"}},
		{name: "unset", want: nil},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("READ_REPLICA_HOSTS", tc.env)
			got := ReadReplicaHostsFromEnv(tc.fallback)
			if len(got) != len(tc.want) {
				t.Fatalf("len=%d want %d: %v", len(got), len(tc.want), got)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("[%d]=%q want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}
