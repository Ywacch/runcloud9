package main

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/ywacch/cloud9/pkg/templateapp/postgres"
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

func TestRouterActive_nilTypedInterface(t *testing.T) {
	t.Parallel()

	var typedNil *readrouter.ReadRouter
	if routerActive(typedNil) {
		t.Fatal("typed nil *ReadRouter must not be treated as active")
	}
	var unset readSplitRouter
	if routerActive(unset) {
		t.Fatal("nil interface must not be treated as active")
	}
	active := readrouter.NewReadRouter(nil, nil, readrouter.PostgresEngine{}, readrouter.PolicyLeaderOnly)
	if !routerActive(active) {
		t.Fatal("real router must be active")
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

func TestReadReplicaHostsFromEnv(t *testing.T) {
	tests := []struct {
		name      string
		hostsEnv  string
		fallback  string
		wantHosts []string
	}{
		{
			name:      "comma list",
			hostsEnv:  "postgres-replica-0, postgres-replica-1",
			wantHosts: []string{"postgres-replica-0", "postgres-replica-1"},
		},
		{
			name:      "fallback host",
			fallback:  "db-replica",
			wantHosts: []string{"db-replica"},
		},
		{
			name:      "empty list falls back",
			hostsEnv:  " , ",
			fallback:  "postgres-replica-0",
			wantHosts: []string{"postgres-replica-0"},
		},
		{
			name:      "unset",
			wantHosts: nil,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("READ_REPLICA_HOSTS", tc.hostsEnv)
			cfg := Config{ReadReplicaHost: tc.fallback}
			got := readReplicaHostsFromEnv(cfg)
			if len(got) != len(tc.wantHosts) {
				t.Fatalf("len=%d want %d: %v", len(got), len(tc.wantHosts), got)
			}
			for i := range got {
				if got[i] != tc.wantHosts[i] {
					t.Fatalf("host[%d]=%q want %q", i, got[i], tc.wantHosts[i])
				}
			}
		})
	}
}

func TestReadReplicaHosts_buildsMultiPoolRouter(t *testing.T) {
	t.Setenv("READ_REPLICA_HOSTS", "postgres-replica-0,postgres-replica-1")
	hosts := readReplicaHostsFromEnv(Config{})
	replicas := make([]readrouter.ReplicaPool, len(hosts))
	for i, host := range hosts {
		replicas[i] = readrouter.ReplicaPool{Host: host}
	}
	r := readrouter.NewReadRouter(nil, replicas, readrouter.PostgresEngine{}, readrouter.PolicyReplica)
	if r == nil {
		t.Fatal("nil router")
	}
	if got := r.RouteRead("203.0.113.1", readrouter.PolicyReplica); got != readrouter.RouteReplica {
		t.Fatalf("RouteRead() = %v want replica", got)
	}
}

func TestObserveTarget_recordsQueryHistogramTarget(t *testing.T) {
	t.Parallel()
	if err := postgres.ObserveTarget(context.Background(), "get_url", func() (string, error) {
		return "postgres-replica-0", nil
	}); err != nil {
		t.Fatalf("ObserveTarget: %v", err)
	}
	if err := postgres.Observe(context.Background(), "insert_url", "postgres", func() error {
		return nil
	}); err != nil {
		t.Fatalf("Observe: %v", err)
	}

	found := false
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "db_query_duration_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			var op, target string
			for _, lp := range m.GetLabel() {
				switch lp.GetName() {
				case "operation":
					op = lp.GetValue()
				case "target":
					target = lp.GetValue()
				}
			}
			if op == "get_url" && target == "postgres-replica-0" {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("expected db_query_duration_seconds{operation=get_url,target=postgres-replica-0}")
	}
}
