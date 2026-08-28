package postgres

import (
	"context"
	"errors"
	"testing"
)

func TestObserve_PropagatesError(t *testing.T) {
	t.Parallel()
	want := errors.New("query failed")
	err := Observe(context.Background(), "test_op", "postgres-replica-0", func() error {
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("want %v, got %v", want, err)
	}
}

func TestObserve_RunsFn(t *testing.T) {
	t.Parallel()
	called := false
	if err := Observe(context.Background(), "ok_op", "postgres", func() error {
		called = true
		return nil
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("fn not called")
	}
}

func TestNormalizeObserveTarget(t *testing.T) {
	t.Parallel()
	if got := NormalizeObserveTarget(""); got != "postgres" {
		t.Fatalf("empty: got %q want postgres", got)
	}
	if got := NormalizeObserveTarget("postgres-replica-0"); got != "postgres-replica-0" {
		t.Fatalf("replica: got %q", got)
	}
}

func TestObserveTarget_recordsDynamicTarget(t *testing.T) {
	t.Parallel()
	if err := ObserveTarget(context.Background(), "get_url", func() (string, error) {
		return "postgres-replica-1", nil
	}); err != nil {
		t.Fatalf("ObserveTarget: %v", err)
	}
	if _, err := dbQueryDuration.GetMetricWithLabelValues("get_url", "postgres-replica-1"); err != nil {
		t.Fatalf("expected histogram series for dynamic target: %v", err)
	}
}

func TestObserve_recordsTargetLabel(t *testing.T) {
	t.Parallel()
	if err := Observe(context.Background(), "select_url", "postgres-replica-0", func() error {
		return nil
	}); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if _, err := dbQueryDuration.GetMetricWithLabelValues("select_url", "postgres-replica-0"); err != nil {
		t.Fatalf("expected histogram series for replica target: %v", err)
	}
	if err := Observe(context.Background(), "select_url", "", func() error { return nil }); err != nil {
		t.Fatalf("Observe primary: %v", err)
	}
	if _, err := dbQueryDuration.GetMetricWithLabelValues("select_url", "postgres"); err != nil {
		t.Fatalf("expected histogram series for primary target: %v", err)
	}
}

func TestSplitPoolMaxConns(t *testing.T) {
	t.Setenv("DB_POOL_MAX_CONNS", "25")

	write, replica := SplitPoolMaxConns(0)
	if write != 25 || replica != 25 {
		t.Fatalf("zero replicas: write=%d replica=%d want 25/25", write, replica)
	}

	write, replica = SplitPoolMaxConns(2)
	if write != 8 || replica != 8 {
		t.Fatalf("two replicas: write=%d replica=%d want 8/8", write, replica)
	}

	t.Setenv("DB_POOL_MAX_CONNS", "2")
	write, replica = SplitPoolMaxConns(3)
	if write != 1 || replica != 1 {
		t.Fatalf("min 1 each: write=%d replica=%d want 1/1", write, replica)
	}
}

func TestPoolMaxConnsFromEnv_defaultWhenUnset(t *testing.T) {
	t.Setenv("DB_POOL_MAX_CONNS", "")
	if got := poolMaxConnsFromEnv(); got != 25 {
		t.Fatalf("default max conns: got %d want 25", got)
	}
}
