package storetest

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/ywacch/cloud9/images/url-shortener/urlstore"
)

// Factory returns a fresh store and a cleanup func.
type Factory func(t *testing.T) (urlstore.Store, func())

// RunConformance runs shared url-shortener store tests against newStore.
func RunConformance(t *testing.T, newStore Factory) {
	t.Helper()
	ctx := context.Background()

	t.Run("insert_and_get", func(t *testing.T) {
		s, cleanup := newStore(t)
		defer cleanup()
		inserted, err := s.InsertURL(ctx, "abc12345", "https://example.com/a")
		if err != nil || !inserted {
			t.Fatalf("InsertURL: inserted=%v err=%v", inserted, err)
		}
		rec, err := s.GetURL(ctx, "abc12345")
		if err != nil {
			t.Fatalf("GetURL: %v", err)
		}
		if rec.Code != "abc12345" || rec.Original != "https://example.com/a" {
			t.Fatalf("record: %+v", rec)
		}
	})

	t.Run("insert_conflict_idempotent", func(t *testing.T) {
		s, cleanup := newStore(t)
		defer cleanup()
		if _, err := s.InsertURL(ctx, "dup00001", "https://example.com/one"); err != nil {
			t.Fatalf("first insert: %v", err)
		}
		inserted, err := s.InsertURL(ctx, "dup00001", "https://example.com/two")
		if err != nil {
			t.Fatalf("second insert: %v", err)
		}
		if inserted {
			t.Fatal("want inserted=false on conflict")
		}
		rec, err := s.GetURL(ctx, "dup00001")
		if err != nil {
			t.Fatalf("GetURL: %v", err)
		}
		if rec.Original != "https://example.com/one" {
			t.Fatalf("original: %q", rec.Original)
		}
	})

	t.Run("get_missing", func(t *testing.T) {
		s, cleanup := newStore(t)
		defer cleanup()
		_, err := s.GetURL(ctx, "missing1")
		if err == nil || !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("want pgx.ErrNoRows, got %v", err)
		}
	})

	t.Run("url_exists", func(t *testing.T) {
		s, cleanup := newStore(t)
		defer cleanup()
		ok, err := s.URLExists(ctx, "nope0001")
		if err != nil || ok {
			t.Fatalf("exists missing: ok=%v err=%v", ok, err)
		}
		if _, err := s.InsertURL(ctx, "yes00001", "https://example.com/y"); err != nil {
			t.Fatalf("insert: %v", err)
		}
		ok, err = s.URLExists(ctx, "yes00001")
		if err != nil || !ok {
			t.Fatalf("exists present: ok=%v err=%v", ok, err)
		}
	})

	t.Run("list_codes", func(t *testing.T) {
		s, cleanup := newStore(t)
		defer cleanup()
		for _, pair := range []struct{ code, url string }{
			{"code0001", "https://example.com/1"},
			{"code0002", "https://example.com/2"},
		} {
			if _, err := s.InsertURL(ctx, pair.code, pair.url); err != nil {
				t.Fatalf("insert %s: %v", pair.code, err)
			}
		}
		codes, err := s.ListCodes(ctx, 10)
		if err != nil {
			t.Fatalf("ListCodes: %v", err)
		}
		if len(codes) < 2 {
			t.Fatalf("codes: %v", codes)
		}
	})

	t.Run("ping", func(t *testing.T) {
		s, cleanup := newStore(t)
		defer cleanup()
		if err := s.Ping(ctx); err != nil {
			t.Fatalf("Ping: %v", err)
		}
	})
}
