package main

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ywacch/cloud9/pkg/templateapp/readrouter"
)

type noopRows struct{}

func (noopRows) Close()                                       {}
func (noopRows) Err() error                                   { return nil }
func (noopRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (noopRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (noopRows) Next() bool                                   { return false }
func (noopRows) Scan(dest ...any) error                       { return nil }
func (noopRows) Values() ([]any, error)                       { return nil, nil }
func (noopRows) RawValues() [][]byte                          { return nil }
func (noopRows) Conn() *pgx.Conn                              { return nil }

type stubQuerier struct {
	lastSQL string
}

func (q *stubQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	q.lastSQL = strings.TrimSpace(sql)
	return noopRows{}, nil
}

func (q *stubQuerier) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return nil
}

type hostRecordingRouter struct {
	hosts []string
	calls int
	sqls  []string
}

func (r *hostRecordingRouter) QueryRead(
	ctx context.Context,
	clientID string,
	build func(readTokenExpr string) (sql string, args []any),
	scan func(row pgx.Row, replayToken *string) error,
) (readrouter.Route, string, error) {
	return readrouter.RouteLeader, "postgres", nil
}

func (r *hostRecordingRouter) QueryRows(
	ctx context.Context,
	clientID string,
	run func(ctx context.Context, q readrouter.Querier) error,
) (readrouter.Route, string, error) {
	q := &stubQuerier{}
	host := r.hosts[r.calls%len(r.hosts)]
	r.calls++
	err := run(ctx, q)
	r.sqls = append(r.sqls, q.lastSQL)
	return readrouter.RouteReplica, host, err
}

func (r *hostRecordingRouter) AfterWrite(clientID string, t readrouter.ConsistencyToken) {}

func TestGetThread_twoReadRouterQueriesAndHosts(t *testing.T) {
	t.Parallel()

	router := &hostRecordingRouter{hosts: []string{"postgres-replica-0", "postgres-replica-1"}}
	db := newDB(nil, nil, router)

	thread, err := db.GetThread(context.Background(), 7)
	if err != nil {
		t.Fatalf("GetThread: %v", err)
	}
	if router.calls != 2 {
		t.Fatalf("queryRows calls: got %d want 2", router.calls)
	}
	if thread.ServedBy.Replies != "postgres-replica-0" {
		t.Fatalf("replies host: got %q", thread.ServedBy.Replies)
	}
	if thread.ServedBy.Parents != "postgres-replica-1" {
		t.Fatalf("parents host: got %q", thread.ServedBy.Parents)
	}
	if thread.RootID != 7 {
		t.Fatalf("rootId: got %d", thread.RootID)
	}
	if len(router.sqls) != 2 {
		t.Fatalf("sqls: %+v", router.sqls)
	}
	if !strings.Contains(router.sqls[0], "thread_root_id") || !strings.Contains(router.sqls[0], "reply_to_id IS NOT NULL") {
		t.Fatalf("replies SQL: %q", router.sqls[0])
	}
	if !strings.Contains(router.sqls[1], "FROM posts WHERE id = $1") {
		t.Fatalf("parents SQL (empty replies): %q", router.sqls[1])
	}
}
