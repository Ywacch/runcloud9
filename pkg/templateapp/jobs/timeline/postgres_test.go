package timeline_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ywacch/cloud9/pkg/templateapp/jobs/timeline"
)

type execRecorder struct {
	stmts []string
	args  [][]any
}

func (e *execRecorder) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	e.stmts = append(e.stmts, sql)
	e.args = append(e.args, append([]any(nil), args...))
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (e *execRecorder) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, fmt.Errorf("execRecorder: unexpected Query")
}

func TestPostgresStore_AppendUsesBatchInsertAndConflict(t *testing.T) {
	t.Parallel()
	rec := &execRecorder{}
	store, err := timeline.NewPostgres(rec, rec, timeline.PostgresOptions{MaxEntries: 100})
	if err != nil {
		t.Fatal(err)
	}
	item := timeline.Item{
		PostID:    1,
		AuthorID:  2,
		Payload:   []byte(`{"post_id":1}`),
		CreatedAt: time.Unix(100, 0).UTC(),
	}
	userIDs := make([]int64, 600)
	for i := range userIDs {
		userIDs[i] = int64(i + 1)
	}
	if err := store.Append(context.Background(), userIDs, item); err != nil {
		t.Fatal(err)
	}
	if len(rec.stmts) < 3 {
		t.Fatalf("want insert batches + trim, got %d execs", len(rec.stmts))
	}
	if !strings.Contains(rec.stmts[0], "ON CONFLICT (user_id, post_id) DO NOTHING") {
		t.Fatalf("first insert missing conflict clause: %q", rec.stmts[0])
	}
	if !strings.Contains(rec.stmts[1], "ON CONFLICT (user_id, post_id) DO NOTHING") {
		t.Fatalf("second insert missing conflict clause: %q", rec.stmts[1])
	}
	if !strings.Contains(rec.stmts[len(rec.stmts)-1], "row_number() OVER") {
		t.Fatalf("trim missing window: %q", rec.stmts[len(rec.stmts)-1])
	}
}

func TestInitPostgresSchema_ExecsEmbeddedSQL(t *testing.T) {
	t.Parallel()
	rec := &execRecorder{}
	if err := timeline.InitPostgresSchema(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	if len(rec.stmts) != 1 || !strings.Contains(rec.stmts[0], "CREATE TABLE IF NOT EXISTS timelines") {
		t.Fatalf("schema sql: %q", rec.stmts)
	}
}

func TestPostgresStore_Engine(t *testing.T) {
	t.Parallel()
	store, err := timeline.NewPostgres(&execRecorder{}, &execRecorder{}, timeline.PostgresOptions{MaxEntries: 10})
	if err != nil {
		t.Fatal(err)
	}
	if store.Engine() != "postgresql" {
		t.Fatalf("engine: %q", store.Engine())
	}
}
