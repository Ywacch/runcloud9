package api

import (
	"context"
	"errors"
	"testing"
	"time"

	"golang.org/x/sync/semaphore"
)

func TestProvisionOrchestrationTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		deployerEnv string
		want        time.Duration
	}{
		{name: "local default", deployerEnv: "", want: 2 * time.Minute},
		{name: "local explicit", deployerEnv: "local", want: 2 * time.Minute},
		{name: "k8s", deployerEnv: "k8s", want: 5 * time.Minute},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := provisionOrchestrationTimeout(tt.deployerEnv)
			if got != tt.want {
				t.Fatalf("provisionOrchestrationTimeout(%q)=%s want %s", tt.deployerEnv, got, tt.want)
			}
		})
	}
}

func TestBeginProvisionWork_DeadlineStartsAfterAcquire(t *testing.T) {
	t.Parallel()

	sem := semaphore.NewWeighted(1)
	if err := sem.Acquire(context.Background(), 1); err != nil {
		t.Fatalf("hold door: %v", err)
	}

	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	defer lifeCancel()

	type outcome struct {
		waited    time.Duration
		remaining time.Duration
		err       error
	}
	done := make(chan outcome, 1)

	go func() {
		start := time.Now()
		workCtx, cancel, release, err := beginProvisionWork(lifeCtx, sem, 200*time.Millisecond, nil)
		waited := time.Since(start)
		if err != nil {
			done <- outcome{waited: waited, err: err}
			return
		}
		defer release()
		defer cancel()

		dl, ok := workCtx.Deadline()
		if !ok {
			done <- outcome{waited: waited, err: errors.New("missing deadline")}
			return
		}
		done <- outcome{waited: waited, remaining: time.Until(dl)}
	}()

	select {
	case <-done:
		t.Fatal("beginProvisionWork returned before door was released")
	case <-time.After(120 * time.Millisecond):
	}

	sem.Release(1)

	var got outcome
	select {
	case got = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for beginProvisionWork")
	}

	if got.err != nil {
		t.Fatalf("beginProvisionWork: %v", got.err)
	}
	if got.waited < 100*time.Millisecond {
		t.Fatalf("expected door wait, waited %s", got.waited)
	}
	// Deadline must be ~200ms from acquire, not from when we started waiting.
	if got.remaining < 100*time.Millisecond || got.remaining > 250*time.Millisecond {
		t.Fatalf("remaining deadline %s want ~200ms after acquire (queue wait must not burn timeout)", got.remaining)
	}
}

func TestBeginProvisionWork_CancelDuringQueue(t *testing.T) {
	t.Parallel()

	sem := semaphore.NewWeighted(1)
	if err := sem.Acquire(context.Background(), 1); err != nil {
		t.Fatalf("hold door: %v", err)
	}
	defer sem.Release(1)

	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, cancel, release, err := beginProvisionWork(lifeCtx, sem, time.Minute, nil)
		if err == nil {
			release()
			cancel()
		}
		done <- err
	}()

	time.Sleep(50 * time.Millisecond)
	lifeCancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for canceled acquire")
	}
}

func TestBeginProvisionWork_CallsOnWaitingWhenDoorFull(t *testing.T) {
	t.Parallel()

	sem := semaphore.NewWeighted(1)
	if err := sem.Acquire(context.Background(), 1); err != nil {
		t.Fatalf("hold door: %v", err)
	}

	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	defer lifeCancel()

	waiting := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		_, cancel, release, err := beginProvisionWork(lifeCtx, sem, time.Minute, func() {
			select {
			case waiting <- struct{}{}:
			default:
			}
		})
		if err == nil {
			release()
			cancel()
		}
		done <- err
	}()

	select {
	case <-waiting:
	case <-time.After(2 * time.Second):
		t.Fatal("onWaiting was not called while door was full")
	}

	sem.Release(1)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("beginProvisionWork: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for acquire after release")
	}
}

func TestBeginProvisionWork_SkipsOnWaitingWhenDoorFree(t *testing.T) {
	t.Parallel()

	sem := semaphore.NewWeighted(1)
	called := false
	_, cancel, release, err := beginProvisionWork(context.Background(), sem, time.Minute, func() {
		called = true
	})
	if err != nil {
		t.Fatalf("beginProvisionWork: %v", err)
	}
	release()
	cancel()
	if called {
		t.Fatal("onWaiting must not run when TryAcquire succeeds")
	}
}

func TestBeginProvisionWork_NilSemaphore(t *testing.T) {
	t.Parallel()

	lifeCtx := context.Background()
	workCtx, cancel, release, err := beginProvisionWork(lifeCtx, nil, 150*time.Millisecond, nil)
	if err != nil {
		t.Fatalf("beginProvisionWork: %v", err)
	}
	defer release()
	defer cancel()

	dl, ok := workCtx.Deadline()
	if !ok {
		t.Fatal("expected deadline")
	}
	remaining := time.Until(dl)
	if remaining < 100*time.Millisecond || remaining > 200*time.Millisecond {
		t.Fatalf("remaining %s want ~150ms", remaining)
	}
}
