package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type memCache struct {
	mu   sync.Mutex
	data map[string]string
	// Optional hooks for failure injection.
	setErr error
	delErr error
	sets   int
	dels   int
}

func newMemCache() *memCache {
	return &memCache{data: make(map[string]string)}
}

func (m *memCache) Set(_ context.Context, key, value string, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sets++
	if m.setErr != nil {
		return m.setErr
	}
	m.data[key] = value
	return nil
}

func (m *memCache) Del(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dels++
	if m.delErr != nil {
		return m.delErr
	}
	delete(m.data, key)
	return nil
}

func (m *memCache) get(key string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[key]
	return v, ok
}

type fakeURLStore struct {
	err     error
	saves   int
	lastCode string
	lastVal  string
}

func (f *fakeURLStore) SaveURL(_ context.Context, code, original string) error {
	f.saves++
	f.lastCode = code
	f.lastVal = original
	return f.err
}

func TestWriteThrough(t *testing.T) {
	t.Parallel()

	const (
		code  = "abc123"
		value = "https://example.com/path"
		ttl   = 60 * time.Second
	)
	setFail := errors.New("redis unavailable")
	dbFail := errors.New("postgres unavailable")

	cases := []struct {
		name        string
		setErr      error
		dbErr       error
		delErr      error
		wantErrIs   error
		wantKey     bool
		wantSaves   int
		wantSets    int
		wantDels    int
	}{
		{
			name:      "success sets cache then db",
			wantKey:   true,
			wantSaves: 1,
			wantSets:  1,
			wantDels:  0,
		},
		{
			name:      "redis set failure skips db and leaves no key",
			setErr:    setFail,
			wantErrIs: errWriteThroughCache,
			wantKey:   false,
			wantSaves: 0,
			wantSets:  1,
			wantDels:  0,
		},
		{
			name:      "db failure rolls back cache key",
			dbErr:     dbFail,
			wantErrIs: errWriteThroughDB,
			wantKey:   false,
			wantSaves: 1,
			wantSets:  1,
			wantDels:  1,
		},
		{
			name:      "db failure still reported when rollback del fails",
			dbErr:     dbFail,
			delErr:    errors.New("del failed"),
			wantErrIs: errWriteThroughDB,
			wantKey:   true, // del failed; key remains — error path logged, caller still sees db error
			wantSaves: 1,
			wantSets:  1,
			wantDels:  1,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cache := newMemCache()
			cache.setErr = tc.setErr
			cache.delErr = tc.delErr
			store := &fakeURLStore{err: tc.dbErr}

			err := writeThrough(context.Background(), cache, store, code, value, ttl)

			if tc.wantErrIs == nil {
				if err != nil {
					t.Fatalf("writeThrough: unexpected error: %v", err)
				}
			} else if !errors.Is(err, tc.wantErrIs) {
				t.Fatalf("writeThrough: error = %v, want Is(%v)", err, tc.wantErrIs)
			}

			_, ok := cache.get(code)
			if ok != tc.wantKey {
				t.Fatalf("cache key present=%v want=%v", ok, tc.wantKey)
			}
			if store.saves != tc.wantSaves {
				t.Fatalf("SaveURL calls=%d want=%d", store.saves, tc.wantSaves)
			}
			if cache.sets != tc.wantSets {
				t.Fatalf("Set calls=%d want=%d", cache.sets, tc.wantSets)
			}
			if cache.dels != tc.wantDels {
				t.Fatalf("Del calls=%d want=%d", cache.dels, tc.wantDels)
			}
			if tc.wantSaves > 0 && tc.dbErr == nil {
				if store.lastCode != code || store.lastVal != value {
					t.Fatalf("SaveURL args code=%q val=%q", store.lastCode, store.lastVal)
				}
			}
		})
	}
}

func TestWriteThrough_OrderIsCacheThenDB(t *testing.T) {
	t.Parallel()

	var order []string
	cache := &orderedCache{inner: newMemCache(), log: &order}
	store := &orderedStore{inner: &fakeURLStore{}, log: &order}

	if err := writeThrough(context.Background(), cache, store, "c1", "https://ok.example", time.Minute); err != nil {
		t.Fatalf("writeThrough: %v", err)
	}
	want := []string{"cache.Set", "db.SaveURL"}
	if len(order) != len(want) {
		t.Fatalf("order=%v want=%v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order[%d]=%q want %q (full=%v)", i, order[i], want[i], order)
		}
	}
}

type orderedCache struct {
	inner *memCache
	log   *[]string
}

func (o *orderedCache) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	*o.log = append(*o.log, "cache.Set")
	return o.inner.Set(ctx, key, value, ttl)
}

func (o *orderedCache) Del(ctx context.Context, key string) error {
	*o.log = append(*o.log, "cache.Del")
	return o.inner.Del(ctx, key)
}

type orderedStore struct {
	inner *fakeURLStore
	log   *[]string
}

func (o *orderedStore) SaveURL(ctx context.Context, code, original string) error {
	*o.log = append(*o.log, "db.SaveURL")
	return o.inner.SaveURL(ctx, code, original)
}
