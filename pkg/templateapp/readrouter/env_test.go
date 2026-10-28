package readrouter

import "testing"

func TestPolicyFromEnv(t *testing.T) {
	t.Parallel()
	tests := []struct {
		raw  string
		want RoutingPolicy
	}{
		{"", PolicyLeaderOnly},
		{"replica", PolicyReplica},
		{"sticky", PolicySticky},
		{"RYW", PolicyRYW},
		{"bogus", PolicyLeaderOnly},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.raw, func(t *testing.T) {
			t.Parallel()
			if got := PolicyFromEnv(tc.raw); got != tc.want {
				t.Fatalf("PolicyFromEnv(%q) = %q want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestReadTarget_rotatesReplicaHosts(t *testing.T) {
	t.Parallel()

	replicaA := stubPool{row: stubRow{}}
	replicaB := stubPool{row: stubRow{}}
	leader := stubPool{row: stubRow{}}

	r := &ReadRouter{
		engine:      PostgresEngine{},
		policy:      PolicyReplica,
		clients:     make(map[string]*clientState),
		maxSize:     defaultMaxClients,
		clientTTL:   defaultClientTTL,
		writePool:   leader,
		writeTarget: defaultWriteTarget,
		readPools: []replicaEntry{
			{pool: replicaA, host: "postgres-replica-0"},
			{pool: replicaB, host: "postgres-replica-1"},
		},
	}

	_, target1 := r.ReadTarget("203.0.113.1")
	_, target2 := r.ReadTarget("203.0.113.1")
	if target1 == target2 {
		t.Fatalf("expected rotation, got %q twice", target1)
	}
}
