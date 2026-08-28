package readrouter

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
)

type stubRow struct {
	err error
}

func (s stubRow) Scan(dest ...any) error {
	return s.err
}

type stubPool struct {
	row stubRow
}

func (s stubPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return s.row
}

func TestQueryRead_replicaRoundRobinRotatesTargets(t *testing.T) {
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

	build := func(readTokenExpr string) (string, []any) {
		if readTokenExpr == "" {
			return "SELECT 1", nil
		}
		return "SELECT 1, " + readTokenExpr, nil
	}
	scan := func(row pgx.Row, replayToken *string) error {
		if replayToken != nil {
			return row.Scan(replayToken)
		}
		return row.Scan(new(int))
	}

	route1, target1, err := r.QueryRead(context.Background(), "client-a", build, scan)
	if err != nil {
		t.Fatalf("first read: %v", err)
	}
	route2, target2, err := r.QueryRead(context.Background(), "client-a", build, scan)
	if err != nil {
		t.Fatalf("second read: %v", err)
	}
	if route1 != RouteReplica || route2 != RouteReplica {
		t.Fatalf("routes: %v %v want replica", route1, route2)
	}
	if target1 == target2 {
		t.Fatalf("expected rotation between replica targets, got %q twice", target1)
	}
	if target1 != "postgres-replica-0" && target1 != "postgres-replica-1" {
		t.Fatalf("unexpected target1: %q", target1)
	}
}

func TestQueryRead_failFirstReplicaUsesSecond(t *testing.T) {
	t.Parallel()

	replicaA := stubPool{row: stubRow{err: errors.New("connection reset")}}
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

	route, target, err := r.QueryRead(context.Background(), "client-a",
		func(readTokenExpr string) (string, []any) {
			return "SELECT 1, " + readTokenExpr, nil
		},
		func(row pgx.Row, replayToken *string) error {
			return row.Scan(replayToken)
		},
	)
	if err != nil {
		t.Fatalf("QueryRead: %v", err)
	}
	if route != RouteReplica {
		t.Fatalf("route: got %v want replica", route)
	}
	if target != "postgres-replica-1" {
		t.Fatalf("target: got %q want postgres-replica-1", target)
	}
}

func TestQueryRead_failBothReplicasUsesLeader(t *testing.T) {
	t.Parallel()

	replicaA := stubPool{row: stubRow{err: errors.New("replica down")}}
	replicaB := stubPool{row: stubRow{err: errors.New("replica down")}}
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

	route, target, err := r.QueryRead(context.Background(), "client-a",
		func(readTokenExpr string) (string, []any) {
			if readTokenExpr == "" {
				return "SELECT 1", nil
			}
			return "SELECT 1, " + readTokenExpr, nil
		},
		func(row pgx.Row, replayToken *string) error {
			if replayToken != nil {
				return row.Scan(replayToken)
			}
			return row.Scan(new(int))
		},
	)
	if err != nil {
		t.Fatalf("QueryRead: %v", err)
	}
	if route != RouteLeader {
		t.Fatalf("route: got %v want leader", route)
	}
	if target != defaultWriteTarget {
		t.Fatalf("target: got %q want %q", target, defaultWriteTarget)
	}
}

func TestQueryRead_leaderRouteUsesPrimaryTarget(t *testing.T) {
	t.Parallel()

	leader := stubPool{row: stubRow{}}
	r := &ReadRouter{
		engine:      PostgresEngine{},
		policy:      PolicyLeaderOnly,
		clients:     make(map[string]*clientState),
		maxSize:     defaultMaxClients,
		clientTTL:   defaultClientTTL,
		writePool:   leader,
		writeTarget: defaultWriteTarget,
	}

	route, target, err := r.QueryRead(context.Background(), "client-a",
		func(string) (string, []any) { return "SELECT 1", nil },
		func(row pgx.Row, replayToken *string) error { return row.Scan(new(int)) },
	)
	if err != nil {
		t.Fatalf("QueryRead: %v", err)
	}
	if route != RouteLeader || target != defaultWriteTarget {
		t.Fatalf("route=%v target=%q", route, target)
	}
}
