package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

type fakeUserPoolFetcher struct {
	ids   []string
	err   error
	calls atomic.Int32
}

func (f *fakeUserPoolFetcher) FetchUserPool(context.Context, string) ([]string, error) {
	f.calls.Add(1)
	if f.err != nil {
		return nil, f.err
	}
	return append([]string(nil), f.ids...), nil
}

func TestActAsValidator_KnownUnknownAndTTL(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	clock := now
	fetch := &fakeUserPoolFetcher{ids: []string{"7", "42"}}
	v := newActAsValidator(fetch, func() time.Time { return clock }, time.Minute)

	if err := v.Validate(context.Background(), "ns-1", "7"); err != nil {
		t.Fatalf("known user: %v", err)
	}
	if fetch.calls.Load() != 1 {
		t.Fatalf("fetch calls: got %d want 1", fetch.calls.Load())
	}
	if err := v.Validate(context.Background(), "ns-1", "99"); !errors.Is(err, errUnknownActAs) {
		t.Fatalf("unknown user: %v", err)
	}
	if fetch.calls.Load() != 1 {
		t.Fatalf("cache hit expected, fetch calls: %d", fetch.calls.Load())
	}

	clock = clock.Add(2 * time.Minute)
	fetch.ids = []string{"99"}
	if err := v.Validate(context.Background(), "ns-1", "99"); err != nil {
		t.Fatalf("after ttl: %v", err)
	}
	if fetch.calls.Load() != 2 {
		t.Fatalf("refetch after ttl: got %d want 2", fetch.calls.Load())
	}
}

func TestBrowserActAsUserID(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(headerBrowserActAs, "12345")
	id, err := browserActAsUserID(req)
	if err != nil || id != "12345" {
		t.Fatalf("got %q err=%v", id, err)
	}
	emptyReq := httptest.NewRequest(http.MethodGet, "/", nil)
	empty, err := browserActAsUserID(emptyReq)
	if err != nil || empty != "" {
		t.Fatalf("empty: %q err=%v", empty, err)
	}
	bad := httptest.NewRequest(http.MethodGet, "/", nil)
	bad.Header.Set(headerBrowserActAs, "abc")
	_, err = browserActAsUserID(bad)
	if !errors.Is(err, errUnknownActAs) {
		t.Fatalf("want unknown act-as, got %v", err)
	}
}

func TestParseUserPoolIDs(t *testing.T) {
	t.Parallel()
	ids, err := parseUserPoolIDs([]byte(`{"userIds":["1",2,3]}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 3 || ids[0] != "1" || ids[1] != "2" || ids[2] != "3" {
		t.Fatalf("got %v", ids)
	}
}
