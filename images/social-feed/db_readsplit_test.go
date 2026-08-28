package main

import (
	"strings"
	"testing"

	"github.com/ywacch/cloud9/pkg/templateapp/readrouter"
)

func TestGetUserQuery_replayToken(t *testing.T) {
	t.Parallel()

	leaderSQL := strings.TrimSpace(getUserQuery(""))
	if strings.Contains(leaderSQL, "pg_last_wal_replay_lsn") {
		t.Fatalf("leader query should not include replay token expr: %s", leaderSQL)
	}

	replicaExpr := readrouter.PostgresEngine{}.ReadTokenExpr()
	replicaSQL := getUserQuery(replicaExpr)
	if !strings.Contains(replicaSQL, replicaExpr) {
		t.Fatalf("replica query missing %q: %s", replicaExpr, replicaSQL)
	}
}

func TestRouterActive_socialFeed(t *testing.T) {
	t.Parallel()
	active := readrouter.NewReadRouter(nil, nil, readrouter.PostgresEngine{}, readrouter.PolicyReplica)
	if !routerActive(active) {
		t.Fatal("real router must be active")
	}
}
