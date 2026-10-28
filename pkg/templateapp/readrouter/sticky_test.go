package readrouter

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestStickyIndex_Stable(t *testing.T) {
	t.Parallel()

	const n = 2
	first := stickyIndex("client-42", n)
	for i := 0; i < 50; i++ {
		if got := stickyIndex("client-42", n); got != first {
			t.Fatalf("stickyIndex unstable: got %d want %d", got, first)
		}
	}

	counts := make([]int, n)
	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("user-%d", i)
		counts[stickyIndex(id, n)]++
	}
	for _, c := range counts {
		pct := float64(c) / 10.0
		if pct < 40 || pct > 60 {
			t.Fatalf("distribution skewed: counts=%v", counts)
		}
	}
}

func TestRoute_StickyPinsClient(t *testing.T) {
	t.Parallel()

	replicaA := stubPool{row: stubRow{}}
	replicaB := stubPool{row: stubRow{}}
	leader := stubPool{row: stubRow{}}

	r := &ReadRouter{
		engine:      PostgresEngine{},
		policy:      PolicySticky,
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
		return "SELECT 1", nil
	}
	scan := func(row pgx.Row, replayToken *string) error {
		return row.Scan()
	}

	_, target1, err := r.QueryRead(context.Background(), "client-pin", build, scan)
	if err != nil {
		t.Fatalf("first QueryRead: %v", err)
	}
	_, target2, err := r.QueryRead(context.Background(), "client-pin", build, scan)
	if err != nil {
		t.Fatalf("second QueryRead: %v", err)
	}
	if target1 != target2 {
		t.Fatalf("targets differ: %q vs %q", target1, target2)
	}
}

func TestRoute_StickyUnhealthyFallsToNext(t *testing.T) {
	t.Parallel()

	connErr := fmt.Errorf("connection refused")
	replicaA := stubPool{row: stubRow{err: connErr}}
	replicaB := stubPool{row: stubRow{}}
	leader := stubPool{row: stubRow{}}

	r := &ReadRouter{
		engine:      PostgresEngine{},
		policy:      PolicySticky,
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
		return "SELECT 1", nil
	}
	scan := func(row pgx.Row, replayToken *string) error {
		return row.Scan()
	}

	clientID := pinnedClientID(0, 2)
	if clientID == "" {
		t.Fatal("no client id pinned to replica index 0")
	}

	_, target, err := r.QueryRead(context.Background(), clientID, build, scan)
	if err != nil {
		t.Fatalf("QueryRead: %v", err)
	}
	if target != "postgres-replica-1" {
		t.Fatalf("target: got %q want postgres-replica-1", target)
	}
}

func TestRoute_ReplicaUnchanged(t *testing.T) {
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
		return "SELECT 1", nil
	}
	scan := func(row pgx.Row, replayToken *string) error {
		return row.Scan()
	}

	_, target1, err := r.QueryRead(context.Background(), "client-a", build, scan)
	if err != nil {
		t.Fatalf("first QueryRead: %v", err)
	}
	_, target2, err := r.QueryRead(context.Background(), "client-a", build, scan)
	if err != nil {
		t.Fatalf("second QueryRead: %v", err)
	}
	if target1 == target2 {
		t.Fatalf("replica policy should round-robin: both got %q", target1)
	}
}

func pinnedClientID(wantIndex, n int) string {
	for i := 0; i < 10_000; i++ {
		id := fmt.Sprintf("pin-%d", i)
		if stickyIndex(id, n) == wantIndex {
			return id
		}
	}
	return ""
}
