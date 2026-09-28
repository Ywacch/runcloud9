package readrouter

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type stubRow struct {
	err       error
	tokenText string
}

func (s stubRow) Scan(dest ...any) error {
	if s.err != nil {
		return s.err
	}
	for _, d := range dest {
		if p, ok := d.(*string); ok && s.tokenText != "" {
			*p = s.tokenText
		}
	}
	return nil
}

type stubPool struct {
	row      stubRow
	queryErr error
}

func (s stubPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return s.row
}

func (s stubPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if s.queryErr != nil {
		return nil, s.queryErr
	}
	return emptyRows{}, nil
}

type emptyRows struct{}

func (emptyRows) Close()                                       {}
func (emptyRows) Err() error                                   { return nil }
func (emptyRows) Next() bool                                   { return false }
func (emptyRows) Scan(dest ...any) error                       { return nil }
func (emptyRows) Values() ([]any, error)                       { return nil, nil }
func (emptyRows) RawValues() [][]byte                          { return nil }
func (emptyRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (emptyRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (emptyRows) Conn() *pgx.Conn                              { return nil }

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

func TestQueryRead_incrementsDBReadByReplicaHost(t *testing.T) {
	t.Parallel()

	// Unique replica label so parallel tests do not race on the global db_read_total vec.
	const metricHost = "test-increment-db-read-by-replica-host"

	replica := stubPool{row: stubRow{}}
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
			{pool: replica, host: metricHost},
		},
	}
	build := func(readTokenExpr string) (string, []any) {
		return "SELECT 1", nil
	}
	scan := func(row pgx.Row, replayToken *string) error {
		return row.Scan(new(int))
	}
	before := testutil.ToFloat64(dbReadTotal.WithLabelValues(metricHost))
	_, target, err := r.QueryRead(context.Background(), "client-metrics", build, scan)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if target != metricHost {
		t.Fatalf("target: %q", target)
	}
	after := testutil.ToFloat64(dbReadTotal.WithLabelValues(metricHost))
	if after-before != 1 {
		t.Fatalf("db_read_total %s: before %v after %v", metricHost, before, after)
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

func TestQueryRead_RYW_staleReplicaRedirectsLeader(t *testing.T) {
	replica := stubPool{row: stubRow{tokenText: "0/1"}}
	leader := stubPool{row: stubRow{}}
	r := &ReadRouter{
		engine:      PostgresEngine{},
		policy:      PolicyRYW,
		clients:     make(map[string]*clientState),
		maxSize:     defaultMaxClients,
		clientTTL:   defaultClientTTL,
		writePool:   leader,
		writeTarget: defaultWriteTarget,
		readPools:   []replicaEntry{{pool: replica, host: "postgres-replica-0"}},
	}
	writeTok, err := r.engine.ParseToken("0/100")
	if err != nil {
		t.Fatal(err)
	}
	r.AfterWrite("client-a", writeTok)

	rywBefore := counterTotal(rywViolationsTotal)
	redirectBefore := counterTotal(rywLeaderRedirectsTotal)

	route, target, err := r.QueryRead(context.Background(), "client-a",
		func(readTokenExpr string) (string, []any) {
			return "SELECT 1, " + readTokenExpr, nil
		},
		func(row pgx.Row, replayToken *string) error {
			if replayToken != nil {
				return row.Scan(new(int), replayToken)
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
	if counterTotal(rywViolationsTotal)-rywBefore != 0 {
		t.Fatalf("ryw violations should not increment on redirect")
	}
	if counterTotal(rywLeaderRedirectsTotal)-redirectBefore != 1 {
		t.Fatalf("expected one leader redirect")
	}
}

func TestQueryRead_RYW_replicaCaughtUp(t *testing.T) {
	t.Parallel()

	replica := stubPool{row: stubRow{tokenText: "0/100"}}
	leader := stubPool{row: stubRow{}}
	r := &ReadRouter{
		engine:      PostgresEngine{},
		policy:      PolicyRYW,
		clients:     make(map[string]*clientState),
		maxSize:     defaultMaxClients,
		clientTTL:   defaultClientTTL,
		writePool:   leader,
		writeTarget: defaultWriteTarget,
		readPools:   []replicaEntry{{pool: replica, host: "postgres-replica-0"}},
	}
	writeTok, err := r.engine.ParseToken("0/100")
	if err != nil {
		t.Fatal(err)
	}
	r.AfterWrite("client-a", writeTok)

	route, target, err := r.QueryRead(context.Background(), "client-a",
		func(readTokenExpr string) (string, []any) {
			return "SELECT 1, " + readTokenExpr, nil
		},
		func(row pgx.Row, replayToken *string) error {
			return row.Scan(new(int), replayToken)
		},
	)
	if err != nil {
		t.Fatalf("QueryRead: %v", err)
	}
	if route != RouteReplica {
		t.Fatalf("route: got %v want replica", route)
	}
	if target != "postgres-replica-0" {
		t.Fatalf("target: got %q", target)
	}
}

func TestQueryRead_RYW_errNoRowsAfterWriteRedirectsLeader(t *testing.T) {
	// Serial: asserts a delta on global rywLeaderRedirectsTotal (parallel RYW tests would flake).
	replica := stubPool{row: stubRow{err: pgx.ErrNoRows}}
	leader := stubPool{row: stubRow{}}
	r := &ReadRouter{
		engine:      PostgresEngine{},
		policy:      PolicyRYW,
		clients:     make(map[string]*clientState),
		maxSize:     defaultMaxClients,
		clientTTL:   defaultClientTTL,
		writePool:   leader,
		writeTarget: defaultWriteTarget,
		readPools:   []replicaEntry{{pool: replica, host: "postgres-replica-0"}},
	}
	writeTok, err := r.engine.ParseToken("0/100")
	if err != nil {
		t.Fatal(err)
	}
	r.AfterWrite("client-a", writeTok)

	redirectBefore := counterTotal(rywLeaderRedirectsTotal)

	route, target, err := r.QueryRead(context.Background(), "client-a",
		func(readTokenExpr string) (string, []any) {
			return "SELECT 1, " + readTokenExpr, nil
		},
		func(row pgx.Row, replayToken *string) error {
			if replayToken != nil {
				return row.Scan(new(int), replayToken)
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
	if counterTotal(rywLeaderRedirectsTotal)-redirectBefore != 1 {
		t.Fatalf("expected one leader redirect after ErrNoRows")
	}
}

func TestQueryRead_replicaPolicy_staleTokenServed(t *testing.T) {
	replica := stubPool{row: stubRow{tokenText: "0/1"}}
	leader := stubPool{row: stubRow{}}
	r := &ReadRouter{
		engine:      PostgresEngine{},
		policy:      PolicyReplica,
		clients:     make(map[string]*clientState),
		maxSize:     defaultMaxClients,
		clientTTL:   defaultClientTTL,
		writePool:   leader,
		writeTarget: defaultWriteTarget,
		readPools:   []replicaEntry{{pool: replica, host: "postgres-replica-0"}},
	}
	writeTok, err := r.engine.ParseToken("0/100")
	if err != nil {
		t.Fatal(err)
	}
	r.AfterWrite("client-a", writeTok)

	staleBefore := counterTotal(replicaStaleReadsTotal)
	rywBefore := counterTotal(rywViolationsTotal)

	route, target, err := r.QueryRead(context.Background(), "client-a",
		func(readTokenExpr string) (string, []any) {
			return "SELECT 1, " + readTokenExpr, nil
		},
		func(row pgx.Row, replayToken *string) error {
			return row.Scan(new(int), replayToken)
		},
	)
	if err != nil {
		t.Fatalf("QueryRead: %v", err)
	}
	if route != RouteReplica {
		t.Fatalf("route: got %v want replica", route)
	}
	if target != "postgres-replica-0" {
		t.Fatalf("target: got %q", target)
	}
	if counterTotal(replicaStaleReadsTotal)-staleBefore != 1 {
		t.Fatalf("expected replica stale counter increment")
	}
	if counterTotal(rywViolationsTotal)-rywBefore != 0 {
		t.Fatalf("ryw violations should not increment under replica policy")
	}
}

func counterTotal(c prometheus.Counter) float64 {
	return testutil.ToFloat64(c)
}

func TestQueryRows_replicaRoundRobinRotatesTargets(t *testing.T) {
	t.Parallel()

	replicaA := stubPool{row: stubRow{tokenText: "0/1"}}
	replicaB := stubPool{row: stubRow{tokenText: "0/1"}}
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

	run := func(context.Context, Querier) error { return nil }

	route1, target1, err := r.QueryRows(context.Background(), "client-a", run)
	if err != nil {
		t.Fatalf("first read: %v", err)
	}
	route2, target2, err := r.QueryRows(context.Background(), "client-a", run)
	if err != nil {
		t.Fatalf("second read: %v", err)
	}
	if route1 != RouteReplica || route2 != RouteReplica {
		t.Fatalf("routes: %v %v want replica", route1, route2)
	}
	if target1 == target2 {
		t.Fatalf("expected rotation between replica targets, got %q twice", target1)
	}
}

func TestQueryRows_failFirstReplicaUsesSecond(t *testing.T) {
	t.Parallel()

	replicaA := stubPool{row: stubRow{err: errors.New("connection reset")}}
	replicaB := stubPool{row: stubRow{tokenText: "0/1"}}
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

	route, target, err := r.QueryRows(context.Background(), "client-a", func(context.Context, Querier) error {
		return nil
	})
	if err != nil {
		t.Fatalf("QueryRows: %v", err)
	}
	if route != RouteReplica {
		t.Fatalf("route: got %v want replica", route)
	}
	if target != "postgres-replica-1" {
		t.Fatalf("target: got %q want postgres-replica-1", target)
	}
}

func TestQueryRows_failBothReplicasUsesLeader(t *testing.T) {
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

	route, target, err := r.QueryRows(context.Background(), "client-a", func(context.Context, Querier) error {
		return nil
	})
	if err != nil {
		t.Fatalf("QueryRows: %v", err)
	}
	if route != RouteLeader {
		t.Fatalf("route: got %v want leader", route)
	}
	if target != defaultWriteTarget {
		t.Fatalf("target: got %q want %q", target, defaultWriteTarget)
	}
}

func TestQueryRows_RYW_staleReplicaRedirectsLeader(t *testing.T) {
	replica := stubPool{row: stubRow{tokenText: "0/1"}}
	leader := stubPool{row: stubRow{}}
	r := &ReadRouter{
		engine:      PostgresEngine{},
		policy:      PolicyRYW,
		clients:     make(map[string]*clientState),
		maxSize:     defaultMaxClients,
		clientTTL:   defaultClientTTL,
		writePool:   leader,
		writeTarget: defaultWriteTarget,
		readPools:   []replicaEntry{{pool: replica, host: "postgres-replica-0"}},
	}
	writeTok, err := r.engine.ParseToken("0/100")
	if err != nil {
		t.Fatal(err)
	}
	r.AfterWrite("client-a", writeTok)

	rywBefore := counterTotal(rywViolationsTotal)
	redirectBefore := counterTotal(rywLeaderRedirectsTotal)
	replicaRun := false

	route, target, err := r.QueryRows(context.Background(), "client-a", func(_ context.Context, q Querier) error {
		if q == replica {
			replicaRun = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("QueryRows: %v", err)
	}
	if replicaRun {
		t.Fatal("run should not execute on replica when peek is behind lastWrite")
	}
	if route != RouteLeader {
		t.Fatalf("route: got %v want leader", route)
	}
	if target != defaultWriteTarget {
		t.Fatalf("target: got %q want %q", target, defaultWriteTarget)
	}
	if counterTotal(rywViolationsTotal)-rywBefore != 0 {
		t.Fatalf("ryw violations should not increment on redirect")
	}
	if counterTotal(rywLeaderRedirectsTotal)-redirectBefore != 1 {
		t.Fatalf("expected one leader redirect")
	}
}

func TestQueryRows_RYW_replicaCaughtUp(t *testing.T) {
	t.Parallel()

	replica := stubPool{row: stubRow{tokenText: "0/100"}}
	leader := stubPool{row: stubRow{}}
	r := &ReadRouter{
		engine:      PostgresEngine{},
		policy:      PolicyRYW,
		clients:     make(map[string]*clientState),
		maxSize:     defaultMaxClients,
		clientTTL:   defaultClientTTL,
		writePool:   leader,
		writeTarget: defaultWriteTarget,
		readPools:   []replicaEntry{{pool: replica, host: "postgres-replica-0"}},
	}
	writeTok, err := r.engine.ParseToken("0/100")
	if err != nil {
		t.Fatal(err)
	}
	r.AfterWrite("client-a", writeTok)

	route, target, err := r.QueryRows(context.Background(), "client-a", func(context.Context, Querier) error {
		return nil
	})
	if err != nil {
		t.Fatalf("QueryRows: %v", err)
	}
	if route != RouteReplica {
		t.Fatalf("route: got %v want replica", route)
	}
	if target != "postgres-replica-0" {
		t.Fatalf("target: got %q", target)
	}
}

func TestQueryRows_RYW_emptyResultAfterWriteServesReplica(t *testing.T) {
	t.Parallel()

	replica := stubPool{row: stubRow{tokenText: "0/100"}}
	leader := stubPool{row: stubRow{}}
	r := &ReadRouter{
		engine:      PostgresEngine{},
		policy:      PolicyRYW,
		clients:     make(map[string]*clientState),
		maxSize:     defaultMaxClients,
		clientTTL:   defaultClientTTL,
		writePool:   leader,
		writeTarget: defaultWriteTarget,
		readPools:   []replicaEntry{{pool: replica, host: "postgres-replica-0"}},
	}
	writeTok, err := r.engine.ParseToken("0/100")
	if err != nil {
		t.Fatal(err)
	}
	r.AfterWrite("client-a", writeTok)

	route, target, err := r.QueryRows(context.Background(), "client-a", func(context.Context, Querier) error {
		return nil
	})
	if err != nil {
		t.Fatalf("QueryRows: %v", err)
	}
	if route != RouteReplica {
		t.Fatalf("route: got %v want replica", route)
	}
	if target != "postgres-replica-0" {
		t.Fatalf("target: got %q", target)
	}
}

func TestQueryRows_replicaPolicy_stalePeekServed(t *testing.T) {
	replica := stubPool{row: stubRow{tokenText: "0/1"}}
	leader := stubPool{row: stubRow{}}
	r := &ReadRouter{
		engine:      PostgresEngine{},
		policy:      PolicyReplica,
		clients:     make(map[string]*clientState),
		maxSize:     defaultMaxClients,
		clientTTL:   defaultClientTTL,
		writePool:   leader,
		writeTarget: defaultWriteTarget,
		readPools:   []replicaEntry{{pool: replica, host: "postgres-replica-0"}},
	}
	writeTok, err := r.engine.ParseToken("0/100")
	if err != nil {
		t.Fatal(err)
	}
	r.AfterWrite("client-a", writeTok)

	staleBefore := counterTotal(replicaStaleReadsTotal)
	rywBefore := counterTotal(rywViolationsTotal)

	route, target, err := r.QueryRows(context.Background(), "client-a", func(context.Context, Querier) error {
		return nil
	})
	if err != nil {
		t.Fatalf("QueryRows: %v", err)
	}
	if route != RouteReplica {
		t.Fatalf("route: got %v want replica", route)
	}
	if target != "postgres-replica-0" {
		t.Fatalf("target: got %q", target)
	}
	if counterTotal(replicaStaleReadsTotal)-staleBefore != 1 {
		t.Fatalf("expected replica stale counter increment")
	}
	if counterTotal(rywViolationsTotal)-rywBefore != 0 {
		t.Fatalf("ryw violations should not increment under replica policy")
	}
}

func TestTokenBehindBaseline(t *testing.T) {
	t.Parallel()

	engine := PostgresEngine{}
	tok100, err := engine.ParseToken("0/100")
	if err != nil {
		t.Fatal(err)
	}
	tok90, err := engine.ParseToken("0/90")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name          string
		token         ConsistencyToken
		writeBaseline ConsistencyToken
		want          bool
	}{
		{name: "nil write baseline", token: tok100, writeBaseline: nil, want: false},
		{name: "nil token", token: nil, writeBaseline: tok100, want: false},
		{name: "caught up", token: tok100, writeBaseline: tok100, want: false},
		{name: "behind", token: tok90, writeBaseline: tok100, want: true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tokenBehindBaseline(tt.token, tt.writeBaseline); got != tt.want {
				t.Fatalf("tokenBehindBaseline() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueryRows_RYW_concurrentWriteDuringRunNotViolation(t *testing.T) {
	replica := stubPool{row: stubRow{tokenText: "0/100"}}
	leader := stubPool{row: stubRow{}}
	r := &ReadRouter{
		engine:      PostgresEngine{},
		policy:      PolicyRYW,
		clients:     make(map[string]*clientState),
		maxSize:     defaultMaxClients,
		clientTTL:   defaultClientTTL,
		writePool:   leader,
		writeTarget: defaultWriteTarget,
		readPools:   []replicaEntry{{pool: replica, host: "postgres-replica-0"}},
	}
	writeTok, err := r.engine.ParseToken("0/100")
	if err != nil {
		t.Fatal(err)
	}
	r.AfterWrite("client-a", writeTok)
	concurrentWrite, err := r.engine.ParseToken("0/110")
	if err != nil {
		t.Fatal(err)
	}

	rywBefore := counterTotal(rywViolationsTotal)
	redirectBefore := counterTotal(rywLeaderRedirectsTotal)

	route, target, err := r.QueryRows(context.Background(), "client-a", func(_ context.Context, q Querier) error {
		if q == replica {
			r.AfterWrite("client-a", concurrentWrite)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("QueryRows: %v", err)
	}
	if route != RouteReplica {
		t.Fatalf("route: got %v want replica", route)
	}
	if target != "postgres-replica-0" {
		t.Fatalf("target: got %q", target)
	}
	if counterTotal(rywViolationsTotal)-rywBefore != 0 {
		t.Fatalf("concurrent write during run must not count as RYW violation")
	}
	if counterTotal(rywLeaderRedirectsTotal)-redirectBefore != 0 {
		t.Fatalf("expected no leader redirect when peek was caught up at baseline")
	}
}

func TestQueryRows_replicaPolicy_concurrentWriteDuringRunNotStale(t *testing.T) {
	replica := stubPool{row: stubRow{tokenText: "0/100"}}
	leader := stubPool{row: stubRow{}}
	r := &ReadRouter{
		engine:      PostgresEngine{},
		policy:      PolicyReplica,
		clients:     make(map[string]*clientState),
		maxSize:     defaultMaxClients,
		clientTTL:   defaultClientTTL,
		writePool:   leader,
		writeTarget: defaultWriteTarget,
		readPools:   []replicaEntry{{pool: replica, host: "postgres-replica-0"}},
	}
	writeTok, err := r.engine.ParseToken("0/100")
	if err != nil {
		t.Fatal(err)
	}
	r.AfterWrite("client-a", writeTok)
	concurrentWrite, err := r.engine.ParseToken("0/110")
	if err != nil {
		t.Fatal(err)
	}

	staleBefore := counterTotal(replicaStaleReadsTotal)

	route, target, err := r.QueryRows(context.Background(), "client-a", func(_ context.Context, q Querier) error {
		if q == replica {
			r.AfterWrite("client-a", concurrentWrite)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("QueryRows: %v", err)
	}
	if route != RouteReplica {
		t.Fatalf("route: got %v want replica", route)
	}
	if target != "postgres-replica-0" {
		t.Fatalf("target: got %q", target)
	}
	if counterTotal(replicaStaleReadsTotal)-staleBefore != 0 {
		t.Fatalf("concurrent write during run must not count as replica stale read")
	}
}

func TestQueryRead_RYW_concurrentWriteDuringScanNotViolation(t *testing.T) {
	replica := stubPool{row: stubRow{tokenText: "0/100"}}
	leader := stubPool{row: stubRow{}}
	r := &ReadRouter{
		engine:      PostgresEngine{},
		policy:      PolicyRYW,
		clients:     make(map[string]*clientState),
		maxSize:     defaultMaxClients,
		clientTTL:   defaultClientTTL,
		writePool:   leader,
		writeTarget: defaultWriteTarget,
		readPools:   []replicaEntry{{pool: replica, host: "postgres-replica-0"}},
	}
	writeTok, err := r.engine.ParseToken("0/100")
	if err != nil {
		t.Fatal(err)
	}
	r.AfterWrite("client-a", writeTok)
	concurrentWrite, err := r.engine.ParseToken("0/110")
	if err != nil {
		t.Fatal(err)
	}

	rywBefore := counterTotal(rywViolationsTotal)
	redirectBefore := counterTotal(rywLeaderRedirectsTotal)

	route, target, err := r.QueryRead(context.Background(), "client-a",
		func(readTokenExpr string) (string, []any) {
			return "SELECT 1, " + readTokenExpr, nil
		},
		func(row pgx.Row, replayToken *string) error {
			if err := row.Scan(new(int), replayToken); err != nil {
				return err
			}
			r.AfterWrite("client-a", concurrentWrite)
			return nil
		},
	)
	if err != nil {
		t.Fatalf("QueryRead: %v", err)
	}
	if route != RouteReplica {
		t.Fatalf("route: got %v want replica", route)
	}
	if target != "postgres-replica-0" {
		t.Fatalf("target: got %q", target)
	}
	if counterTotal(rywViolationsTotal)-rywBefore != 0 {
		t.Fatalf("concurrent write during scan must not count as RYW violation")
	}
	if counterTotal(rywLeaderRedirectsTotal)-redirectBefore != 0 {
		t.Fatalf("expected no leader redirect when replica was caught up at baseline")
	}
}

func TestQueryRead_replicaPolicy_concurrentWriteDuringScanNotStale(t *testing.T) {
	replica := stubPool{row: stubRow{tokenText: "0/100"}}
	leader := stubPool{row: stubRow{}}
	r := &ReadRouter{
		engine:      PostgresEngine{},
		policy:      PolicyReplica,
		clients:     make(map[string]*clientState),
		maxSize:     defaultMaxClients,
		clientTTL:   defaultClientTTL,
		writePool:   leader,
		writeTarget: defaultWriteTarget,
		readPools:   []replicaEntry{{pool: replica, host: "postgres-replica-0"}},
	}
	writeTok, err := r.engine.ParseToken("0/100")
	if err != nil {
		t.Fatal(err)
	}
	r.AfterWrite("client-a", writeTok)
	concurrentWrite, err := r.engine.ParseToken("0/110")
	if err != nil {
		t.Fatal(err)
	}

	staleBefore := counterTotal(replicaStaleReadsTotal)

	route, target, err := r.QueryRead(context.Background(), "client-a",
		func(readTokenExpr string) (string, []any) {
			return "SELECT 1, " + readTokenExpr, nil
		},
		func(row pgx.Row, replayToken *string) error {
			if err := row.Scan(new(int), replayToken); err != nil {
				return err
			}
			r.AfterWrite("client-a", concurrentWrite)
			return nil
		},
	)
	if err != nil {
		t.Fatalf("QueryRead: %v", err)
	}
	if route != RouteReplica {
		t.Fatalf("route: got %v want replica", route)
	}
	if target != "postgres-replica-0" {
		t.Fatalf("target: got %q", target)
	}
	if counterTotal(replicaStaleReadsTotal)-staleBefore != 0 {
		t.Fatalf("concurrent write during scan must not count as replica stale read")
	}
}
