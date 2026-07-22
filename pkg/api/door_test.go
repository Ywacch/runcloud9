package api

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestProvisionDoor_PositionsAndEstimate(t *testing.T) {
	t.Parallel()

	door := newProvisionDoor(5, 100) // fallback unused once we Observe
	door.holdETA.Observe(100 * time.Second)

	door.enterWait("a")
	door.enterWait("b")
	door.enterWait("c")

	pos, eta, ok := door.Estimate("b")
	if !ok || pos != 2 {
		t.Fatalf("b: pos=%d ok=%v want pos=2", pos, ok)
	}
	// ceil(2 * 100 / 5) = 40
	if eta != 40 {
		t.Fatalf("b eta=%d want 40", eta)
	}

	door.leaveWait("a")
	pos, ok = door.Position("b")
	if !ok || pos != 1 {
		t.Fatalf("b after leave a: pos=%d want 1", pos)
	}
	_, eta, ok = door.Estimate("b")
	if !ok || eta != 20 { // ceil(1*100/5)=20
		t.Fatalf("b eta=%d want 20", eta)
	}
}

func TestProvisionDoor_EstimateWidthOne(t *testing.T) {
	t.Parallel()

	door := newProvisionDoor(1, 240)
	door.holdETA.Observe(120 * time.Second)
	door.enterWait("only")
	pos, eta, ok := door.Estimate("only")
	if !ok || pos != 1 || eta != 120 {
		t.Fatalf("pos=%d eta=%d ok=%v want 1,120,true", pos, eta, ok)
	}
}

func TestProvisionDoor_AcquireWaitingAndDeadline(t *testing.T) {
	t.Parallel()

	door := newProvisionDoor(1, 240)
	releaseHold, err := door.Acquire(context.Background(), "holder", nil)
	if err != nil {
		t.Fatalf("hold: %v", err)
	}

	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	defer lifeCancel()

	type outcome struct {
		waited    time.Duration
		remaining time.Duration
		err       error
		posWhile  int
	}
	done := make(chan outcome, 1)

	go func() {
		start := time.Now()
		waiting := make(chan struct{}, 1)
		var posWhile int
		workCtx, cancel, release, err := beginProvisionWork(
			lifeCtx, door, "waiter", 200*time.Millisecond,
			func() {
				posWhile, _ = door.Position("waiter")
				select {
				case waiting <- struct{}{}:
				default:
				}
			},
		)
		<-waiting
		waited := time.Since(start)
		if err != nil {
			done <- outcome{waited: waited, err: err, posWhile: posWhile}
			return
		}
		defer release()
		defer cancel()
		dl, ok := workCtx.Deadline()
		if !ok {
			done <- outcome{waited: waited, err: errors.New("missing deadline"), posWhile: posWhile}
			return
		}
		done <- outcome{waited: waited, remaining: time.Until(dl), posWhile: posWhile}
	}()

	select {
	case <-done:
		t.Fatal("beginProvisionWork returned before door was released")
	case <-time.After(120 * time.Millisecond):
	}

	releaseHold()

	var got outcome
	select {
	case got = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for beginProvisionWork")
	}
	if got.err != nil {
		t.Fatalf("beginProvisionWork: %v", got.err)
	}
	if got.posWhile != 1 {
		t.Fatalf("pos while waiting=%d want 1", got.posWhile)
	}
	if got.waited < 100*time.Millisecond {
		t.Fatalf("expected door wait, waited %s", got.waited)
	}
	if got.remaining < 100*time.Millisecond || got.remaining > 250*time.Millisecond {
		t.Fatalf("remaining deadline %s want ~200ms after acquire", got.remaining)
	}
	if _, ok := door.Position("waiter"); ok {
		t.Fatal("waiter should leave FIFO after acquire")
	}
}

func TestProvisionDoor_CancelDuringWait(t *testing.T) {
	t.Parallel()

	door := newProvisionDoor(1, 240)
	hold, err := door.Acquire(context.Background(), "holder", nil)
	if err != nil {
		t.Fatalf("hold: %v", err)
	}
	defer hold()

	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, cancel, release, err := beginProvisionWork(lifeCtx, door, "waiter", time.Minute, nil)
		if err == nil {
			release()
			cancel()
		}
		done <- err
	}()

	time.Sleep(50 * time.Millisecond)
	if _, ok := door.Position("waiter"); !ok {
		t.Fatal("expected waiter registered")
	}
	lifeCancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for canceled acquire")
	}
	if _, ok := door.Position("waiter"); ok {
		t.Fatal("waiter should leave FIFO after cancel")
	}
}

func TestProvisionDoor_SkipsOnWaitingWhenFree(t *testing.T) {
	t.Parallel()

	door := newProvisionDoor(1, 240)
	called := false
	_, cancel, release, err := beginProvisionWork(context.Background(), door, "ns", time.Minute, func() {
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

func TestBeginProvisionWork_NilDoor(t *testing.T) {
	t.Parallel()

	workCtx, cancel, release, err := beginProvisionWork(context.Background(), nil, "ns", 150*time.Millisecond, nil)
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

func TestAttachWaitFields_DoorAndRoom(t *testing.T) {
	t.Setenv("MAX_CONCURRENT_PROVISIONS", "1")
	t.Setenv("CLUSTER_DEPLOY_BUDGET_CPU", "")
	t.Setenv("CLUSTER_DEPLOY_BUDGET_MEMORY", "")

	h := NewHandler(nil, newFakeDeploymentStore(), nil, nil, nil, nil, nil, nil, nil)
	if h.door == nil {
		t.Fatal("expected door from env")
	}
	h.door.enterWait("door-ns")
	h.door.holdETA.Observe(60 * time.Second)

	resp := map[string]any{}
	h.attachWaitFields(resp, context.Background(), "door-ns", statusProvisioning)
	if resp["queuePosition"] != 1 {
		t.Fatalf("door queuePosition=%v want 1", resp["queuePosition"])
	}
	if resp["estimatedWaitSeconds"] != 60 {
		t.Fatalf("door eta=%v want 60", resp["estimatedWaitSeconds"])
	}
}
