package postgres

import (
	"context"
	"errors"
	"testing"
)

func TestObserve_PropagatesError(t *testing.T) {
	t.Parallel()
	want := errors.New("query failed")
	err := Observe(context.Background(), "test_op", func() error {
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("want %v, got %v", want, err)
	}
}

func TestObserve_RunsFn(t *testing.T) {
	t.Parallel()
	called := false
	if err := Observe(context.Background(), "ok_op", func() error {
		called = true
		return nil
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("fn not called")
	}
}
