package main

import (
	"strings"
	"testing"

	"github.com/ywacch/cloud9/pkg/templateapp/postgres"
	"github.com/ywacch/cloud9/pkg/templateapp/readrouter"
)

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

func TestReadReplicaHostsFromEnv_cachingProxy(t *testing.T) {
	t.Setenv("READ_REPLICA_HOSTS", "postgres-replica-0,postgres-replica-1")
	got := postgres.ReadReplicaHostsFromEnv("")
	if len(got) != 2 || got[0] != "postgres-replica-0" {
		t.Fatalf("hosts: %v", got)
	}
}

func TestRouterActive_cachingProxy(t *testing.T) {
	t.Parallel()
	var typedNil *readrouter.ReadRouter
	if routerActive(typedNil) {
		t.Fatal("typed nil must not be active")
	}
	active := readrouter.NewReadRouter(nil, nil, readrouter.PostgresEngine{}, readrouter.PolicyReplica)
	if !routerActive(active) {
		t.Fatal("real router must be active")
	}
}

func TestReplicaConnectHost_cachingProxy(t *testing.T) {
	t.Parallel()
	if got := postgres.ReplicaConnectHost("postgres-replica-0"); got != "postgres-replica-0.postgres-replica" {
		t.Fatalf("connect host = %q", got)
	}
}
