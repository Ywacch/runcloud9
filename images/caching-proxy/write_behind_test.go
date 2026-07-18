package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestParseWriteBehindIntervalSeconds(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want int
	}{
		{name: "absent uses default", raw: "", want: 30},
		{name: "valid value", raw: "45", want: 45},
		{name: "min bound", raw: "1", want: 1},
		{name: "max bound", raw: "300", want: 300},
		{name: "zero falls back", raw: "0", want: 30},
		{name: "negative falls back", raw: "-1", want: 30},
		{name: "over max clamps", raw: "999", want: 300},
		{name: "malformed falls back", raw: "soon", want: 30},
		{name: "whitespace trimmed", raw: " 90 ", want: 90},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseWriteBehindIntervalSeconds(tt.raw); got != tt.want {
				t.Fatalf("parseWriteBehindIntervalSeconds(%q) = %d, want %d", tt.raw, got, tt.want)
			}
		})
	}
}

func TestWriteBehindInterval_PanicGuard(t *testing.T) {
	if got := writeBehindInterval(0); got != 30*time.Second {
		t.Fatalf("writeBehindInterval(0) = %v, want 30s", got)
	}
	if got := writeBehindInterval(-time.Second); got != 30*time.Second {
		t.Fatalf("writeBehindInterval(-1s) = %v, want 30s", got)
	}
	if got := writeBehindInterval(5 * time.Second); got != 5*time.Second {
		t.Fatalf("writeBehindInterval(5s) = %v, want 5s", got)
	}
}

func TestShouldStartWriteBehindFlusher(t *testing.T) {
	tests := []struct {
		strategy string
		want     bool
	}{
		{strategy: "write-behind", want: true},
		{strategy: "write-through", want: false},
		{strategy: "cache-aside", want: false},
		{strategy: "", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.strategy, func(t *testing.T) {
			if got := shouldStartWriteBehindFlusher(tt.strategy); got != tt.want {
				t.Fatalf("shouldStartWriteBehindFlusher(%q) = %v, want %v", tt.strategy, got, tt.want)
			}
		})
	}
}

func TestStorePendingWrite_NoTTL(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)

	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	ctx := context.Background()
	code := "abc123"
	value := "https://example.com"

	// Cache entry with a short TTL (simulates user-facing cache TTL).
	if err := rdb.Set(ctx, code, value, 50*time.Millisecond).Err(); err != nil {
		t.Fatalf("cache SET: %v", err)
	}
	if err := storePendingWrite(ctx, rdb, code, value); err != nil {
		t.Fatalf("storePendingWrite: %v", err)
	}

	wbKey := pendingWriteKey(code)
	if !srv.Exists(wbKey) {
		t.Fatalf("expected %s to exist", wbKey)
	}
	if got := srv.TTL(wbKey); got != 0 {
		t.Fatalf("TTL(%s) = %v, want 0 (no expiry)", wbKey, got)
	}

	// Advance past the cache-entry TTL; pending write must survive.
	srv.FastForward(100 * time.Millisecond)
	if srv.Exists(code) {
		t.Fatalf("expected cache key %s to expire", code)
	}
	if !srv.Exists(wbKey) {
		t.Fatalf("pending write %s must survive past cache-entry TTL", wbKey)
	}
}

func TestFlushPendingWrites_SuccessDeletesKey(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)

	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	ctx := context.Background()
	code := "flush1"
	value := "https://flush.example"
	if err := storePendingWrite(ctx, rdb, code, value); err != nil {
		t.Fatal(err)
	}

	store := &fakeURLStore{}
	flushPendingWrites(ctx, rdb, store)

	if store.saves != 1 || store.lastCode != code || store.lastVal != value {
		t.Fatalf("SaveURL calls=%d code=%q val=%q", store.saves, store.lastCode, store.lastVal)
	}
	if srv.Exists(pendingWriteKey(code)) {
		t.Fatalf("pending key should be deleted after successful flush")
	}
}

func TestFlushPendingWrites_DBFailureKeepsKey(t *testing.T) {
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)

	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	ctx := context.Background()
	code := "keepme"
	if err := storePendingWrite(ctx, rdb, code, "https://keep.example"); err != nil {
		t.Fatal(err)
	}

	store := &fakeURLStore{err: errors.New("postgres down")}
	flushPendingWrites(ctx, rdb, store)

	if store.saves != 1 {
		t.Fatalf("SaveURL calls=%d, want 1", store.saves)
	}
	if !srv.Exists(pendingWriteKey(code)) {
		t.Fatalf("pending key must remain when DB flush fails")
	}
}

func TestRunWriteBehindFlusher_FirstTickAndCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var ticks atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		runWriteBehindFlusher(ctx, 40*time.Millisecond, func(context.Context) {
			ticks.Add(1)
		})
	}()

	deadline := time.After(500 * time.Millisecond)
	for {
		if ticks.Load() >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("expected at least one flush tick within 500ms, got %d", ticks.Load())
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("flusher did not exit after cancel")
	}
}

func TestRunWriteBehindFlusher_ZeroIntervalDoesNotPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		// Zero would panic time.NewTicker without the guard.
		runWriteBehindFlusher(ctx, 0, func(context.Context) {})
	}()

	// Allow the goroutine to reach NewTicker with the clamped interval.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("flusher with zero interval did not start or exit")
	}
}
