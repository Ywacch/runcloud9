//go:build integration

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ywacch/cloud9/images/social-feed/socialstore"
	"github.com/ywacch/cloud9/images/social-feed/storetest"
	"github.com/ywacch/cloud9/pkg/templateapp/readrouter"
	"github.com/ywacch/cloud9/pkg/templateapp/trace"
)

func truncateSocialTables(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `TRUNCATE follows, posts, users RESTART IDENTITY CASCADE`)
	return err
}

func TestPostgresSocialStore_ConformanceIntegration(t *testing.T) {
	dsn := os.Getenv("SOCIAL_PG_DSN")
	if dsn == "" {
		t.Skip("SOCIAL_PG_DSN not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := truncateSocialTables(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err := newDB(pool, nil, nil).InitSchema(ctx); err != nil {
		t.Fatal(err)
	}

	storetest.RunConformance(t, func(t *testing.T) (socialstore.Store, func()) {
		t.Helper()
		if err := truncateSocialTables(context.Background(), pool); err != nil {
			t.Fatal(err)
		}
		return newDB(pool, nil, nil), func() {}
	})
}

func TestPostgresSocialStore_ReadYourWritesIntegration(t *testing.T) {
	dsn := os.Getenv("SOCIAL_PG_DSN")
	if dsn == "" {
		t.Skip("SOCIAL_PG_DSN not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := truncateSocialTables(ctx, pool); err != nil {
		t.Fatal(err)
	}
	readPools := []*pgxpool.Pool{pool}
	router := readrouter.NewReadRouter(pool, []readrouter.ReplicaPool{
		{Pool: pool, Host: "postgres-replica-0"},
	}, readrouter.PostgresEngine{}, readrouter.PolicyReplica)
	db := newDB(pool, readPools, router)
	if err := db.InitSchema(ctx); err != nil {
		t.Fatal(err)
	}

	clientCtx := func(base context.Context) context.Context {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req = req.WithContext(base)
		req.Header.Set(trace.HeaderClientID, "conform-ryw-client")
		return trace.ContextFromRequest(req).Context()
	}
	storetest.RunReadYourWrites(t, func(t *testing.T) (socialstore.Store, func()) {
		t.Helper()
		if err := truncateSocialTables(context.Background(), pool); err != nil {
			t.Fatal(err)
		}
		return newDB(pool, readPools, router), func() {}
	}, clientCtx)
}

func TestPostgresSocialStore_ReconnectSurvivalIntegration(t *testing.T) {
	dsn := os.Getenv("SOCIAL_PG_DSN")
	if dsn == "" {
		t.Skip("SOCIAL_PG_DSN not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := truncateSocialTables(ctx, pool); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	db := newDB(pool, nil, nil)
	if err := db.InitSchema(ctx); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	u, err := db.CreateUser(ctx, "reconnect_pg")
	if err != nil {
		pool.Close()
		t.Fatal(err)
	}
	pool.Close()

	pool2, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool2.Close)
	db2 := newDB(pool2, nil, nil)
	got, err := db2.GetUser(ctx, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Username != u.Username {
		t.Fatalf("after reconnect: %+v", got)
	}
}
