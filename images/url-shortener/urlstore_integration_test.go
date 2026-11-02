//go:build integration

package main

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ywacch/cloud9/images/url-shortener/storetest"
	"github.com/ywacch/cloud9/images/url-shortener/urlstore"
)

func TestPostgresURLStore_ConformanceIntegration(t *testing.T) {
	dsn := os.Getenv("URLSHORT_PG_DSN")
	if dsn == "" {
		t.Skip("URLSHORT_PG_DSN not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	truncate := func() error {
		_, err := pool.Exec(ctx, `TRUNCATE urls`)
		return err
	}
	if err := truncate(); err != nil {
		t.Fatal(err)
	}
	db := newDB(pool, nil, nil)
	if err := db.InitSchema(ctx); err != nil {
		t.Fatal(err)
	}

	storetest.RunConformance(t, func(t *testing.T) (urlstore.Store, func()) {
		t.Helper()
		if err := truncate(); err != nil {
			t.Fatal(err)
		}
		return newDB(pool, nil, nil), func() {}
	})
}
