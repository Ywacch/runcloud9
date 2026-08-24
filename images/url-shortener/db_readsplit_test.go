package main

import (
	"strings"
	"testing"

	"github.com/ywacch/cloud9/pkg/templateapp/readrouter"
)

func TestReadConsistencyFromEnv(t *testing.T) {
	t.Parallel()

	tests := []struct {
		raw  string
		want readrouter.RoutingPolicy
	}{
		{"", readrouter.PolicyLeaderOnly},
		{"leader-only", readrouter.PolicyLeaderOnly},
		{"REPLICA", readrouter.PolicyReplica},
		{"ryw", readrouter.PolicyRYW},
		{"monotonic", readrouter.PolicyMonotonic},
		{"ryw+monotonic", readrouter.PolicyRYWMonotonic},
		{"bogus", readrouter.PolicyLeaderOnly},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.raw, func(t *testing.T) {
			t.Parallel()
			if got := readConsistencyFromEnv(tc.raw); got != tc.want {
				t.Fatalf("readConsistencyFromEnv(%q) = %q want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestGetURLQuery_replayToken(t *testing.T) {
	t.Parallel()

	leaderSQL := strings.TrimSpace(getURLQuery(""))
	if strings.Contains(leaderSQL, "pg_last_wal_replay_lsn") {
		t.Fatalf("leader query should not include replay token expr: %s", leaderSQL)
	}

	replicaExpr := readrouter.PostgresEngine{}.ReadTokenExpr()
	replicaSQL := getURLQuery(replicaExpr)
	if !strings.Contains(replicaSQL, replicaExpr) {
		t.Fatalf("replica query missing %q: %s", replicaExpr, replicaSQL)
	}
}

func TestURLExistsQuery_replayToken(t *testing.T) {
	t.Parallel()

	leaderSQL := strings.TrimSpace(urlExistsQuery(""))
	if strings.Contains(leaderSQL, "pg_last_wal_replay_lsn") {
		t.Fatalf("leader exists query should not include replay token expr: %s", leaderSQL)
	}

	replicaExpr := readrouter.PostgresEngine{}.ReadTokenExpr()
	replicaSQL := urlExistsQuery(replicaExpr)
	if !strings.Contains(replicaSQL, replicaExpr) {
		t.Fatalf("replica exists query missing %q: %s", replicaExpr, replicaSQL)
	}
}

func TestReadRouterPolicyRoutes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy readrouter.RoutingPolicy
		want   readrouter.Route
	}{
		{name: "leader-only", policy: readrouter.PolicyLeaderOnly, want: readrouter.RouteLeader},
		{name: "replica", policy: readrouter.PolicyReplica, want: readrouter.RouteReplica},
		{name: "ryw fresh client", policy: readrouter.PolicyRYW, want: readrouter.RouteReplica},
		{name: "monotonic", policy: readrouter.PolicyMonotonic, want: readrouter.RouteReplica},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := readrouter.NewReadRouter(nil, nil, readrouter.PostgresEngine{}, tc.policy)
			if got := r.RouteRead("203.0.113.1", tc.policy); got != tc.want {
				t.Fatalf("RouteRead() = %v want %v", got, tc.want)
			}
		})
	}
}

func TestReadRouterRYWRedirectsToLeaderAfterWrite(t *testing.T) {
	t.Parallel()

	r := readrouter.NewReadRouter(nil, nil, readrouter.PostgresEngine{}, readrouter.PolicyRYW)
	eng := readrouter.PostgresEngine{}
	writeTok, err := eng.ParseToken("0/20")
	if err != nil {
		t.Fatal(err)
	}
	readTok, err := eng.ParseToken("0/10")
	if err != nil {
		t.Fatal(err)
	}
	r.AfterWrite("203.0.113.1", writeTok)
	r.RecordRead("203.0.113.1", readTok)

	if got := r.RouteRead("203.0.113.1", readrouter.PolicyRYW); got != readrouter.RouteLeader {
		t.Fatalf("RouteRead after stale read = %v want leader", got)
	}
}
