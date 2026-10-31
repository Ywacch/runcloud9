//go:build integration

package timeline_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ywacch/cloud9/pkg/templateapp/jobs/timeline"
	"github.com/ywacch/cloud9/pkg/templateapp/jobs/timeline/timelinetest"
)

func TestPostgresStore_ConformanceIntegration(t *testing.T) {
	dsn := os.Getenv("TIMELINE_PG_DSN")
	if dsn == "" {
		t.Skip("TIMELINE_PG_DSN not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := timeline.InitPostgresSchema(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE timelines`); err != nil {
		t.Fatal(err)
	}

	timelinetest.RunConformance(t, func(t *testing.T) (timeline.Store, func()) {
		t.Helper()
		if _, err := pool.Exec(context.Background(), `TRUNCATE timelines`); err != nil {
			t.Fatal(err)
		}
		s, err := timeline.NewPostgres(pool, pool, timeline.PostgresOptions{MaxEntries: 1000})
		if err != nil {
			t.Fatal(err)
		}
		return s, func() {}
	})
}

func TestPostgresStore_ReconnectSurvivalIntegration(t *testing.T) {
	dsn := os.Getenv("TIMELINE_PG_DSN")
	if dsn == "" {
		t.Skip("TIMELINE_PG_DSN not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := timeline.InitPostgresSchema(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store, err := timeline.NewPostgres(pool, pool, timeline.PostgresOptions{MaxEntries: 100})
	if err != nil {
		t.Fatal(err)
	}
	when := time.Unix(1_700_002_000, 0).UTC()
	if err := store.Append(ctx, []int64{42}, timeline.Item{PostID: 9, CreatedAt: when}); err != nil {
		t.Fatal(err)
	}
	pool.Close()
	pool2, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool2.Close)
	store2, err := timeline.NewPostgres(pool2, pool2, timeline.PostgresOptions{MaxEntries: 100})
	if err != nil {
		t.Fatal(err)
	}
	got, err := store2.Range(ctx, 42, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].PostID != 9 {
		t.Fatalf("range: %+v", got)
	}
}
