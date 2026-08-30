package api

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestDoorReserve(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		width int
		want  int
	}{
		{"width 0", 0, 0},
		{"width 1", 1, 0},
		{"width 3", 3, 1},
		{"width 5", 5, 1},
		{"width 8", 8, 1},
		{"width 10", 10, 2},
		{"width 15", 15, 3},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := doorReserve(tc.width); got != tc.want {
				t.Fatalf("doorReserve(%d) = %d, want %d", tc.width, got, tc.want)
			}
		})
	}
}

func TestNewProvisionDoor_LaneWidths(t *testing.T) {
	t.Parallel()
	d := newProvisionDoor(5, 100)
	if d == nil {
		t.Fatal("expected door")
	}
	if d.reservedWidth != 1 || d.generalWidth != 4 || d.width != 5 {
		t.Fatalf("widths reserved=%d general=%d total=%d", d.reservedWidth, d.generalWidth, d.width)
	}
	if d.semReserved == nil {
		t.Fatal("expected reserved semaphore at width 5")
	}

	one := newProvisionDoor(1, 100)
	if one.reservedWidth != 0 || one.generalWidth != 1 || one.semReserved != nil {
		t.Fatalf("width 1: reserved=%d general=%d semReserved=%v", one.reservedWidth, one.generalWidth, one.semReserved)
	}
}

func TestProvisionStagger(t *testing.T) {
	t.Parallel()
	if got := provisionStagger(true); got != 0 {
		t.Fatalf("paid stagger = %v, want 0", got)
	}
	if got := provisionStagger(false); got < 0 || got >= 2*time.Second {
		t.Fatalf("explorer stagger = %v, want [0,2s)", got)
	}
}

func TestProvisionDoor_PositionsAndEstimate(t *testing.T) {
	t.Parallel()

	door := newProvisionDoor(5, 100) // general=4, reserved=1
	door.holdETA.Observe(100 * time.Second)

	door.enterWait("a", false)
	door.enterWait("b", false)
	door.enterWait("c", false)

	pos, eta, ok := door.Estimate("b")
	if !ok || pos != 2 {
		t.Fatalf("b: pos=%d ok=%v want pos=2", pos, ok)
	}
	// ceil(2 * 100 / 4 general) = 50
	if eta != 50 {
		t.Fatalf("b eta=%d want 50", eta)
	}

	door.leaveWait("a")
	pos, ok = door.Position("b")
	if !ok || pos != 1 {
		t.Fatalf("b after leave a: pos=%d want 1", pos)
	}
	_, eta, ok = door.Estimate("b")
	if !ok || eta != 25 { // ceil(1*100/4)=25
		t.Fatalf("b eta=%d want 25", eta)
	}
}

func TestProvisionDoor_EstimatePaidLane(t *testing.T) {
	t.Parallel()

	door := newProvisionDoor(5, 100)
	door.holdETA.Observe(100 * time.Second)
	door.enterWait("paid-a", true)
	door.enterWait("paid-b", true)

	_, eta, ok := door.Estimate("paid-b")
	if !ok || eta != 40 { // ceil(2*100/5)=40
		t.Fatalf("paid-b eta=%d ok=%v want 40", eta, ok)
	}
}

func TestProvisionDoor_EstimateWidthOne(t *testing.T) {
	t.Parallel()

	door := newProvisionDoor(1, 240)
	door.holdETA.Observe(120 * time.Second)
	door.enterWait("only", false)
	pos, eta, ok := door.Estimate("only")
	if !ok || pos != 1 || eta != 120 {
		t.Fatalf("pos=%d eta=%d ok=%v want 1,120,true", pos, eta, ok)
	}
}

func TestProvisionDoor_AcquireWaitingAndDeadline(t *testing.T) {
	t.Parallel()

	door := newProvisionDoor(1, 240)
	releaseHold, err := door.Acquire(context.Background(), "holder", false, nil)
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
			lifeCtx, door, "waiter", 200*time.Millisecond, false,
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
	hold, err := door.Acquire(context.Background(), "holder", false, nil)
	if err != nil {
		t.Fatalf("hold: %v", err)
	}
	defer hold()

	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, cancel, release, err := beginProvisionWork(lifeCtx, door, "waiter", time.Minute, false, nil)
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
	_, cancel, release, err := beginProvisionWork(context.Background(), door, "ns", time.Minute, false, func() {
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

	workCtx, cancel, release, err := beginProvisionWork(context.Background(), nil, "ns", 150*time.Millisecond, false, nil)
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
	h.door.enterWait("door-ns", false)
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

func TestProvisionDoor_PaidTakesReservedAtWidth5(t *testing.T) {
	t.Parallel()

	door := newProvisionDoor(5, 240)
	var releases []func()
	for i := 0; i < 4; i++ {
		rel, err := door.Acquire(context.Background(), fmt.Sprintf("exp-%d", i), false, nil)
		if err != nil {
			t.Fatalf("explorer %d acquire: %v", i, err)
		}
		releases = append(releases, rel)
	}

	expBlocked := make(chan error, 1)
	go func() {
		rel, err := door.Acquire(context.Background(), "exp-blocked", false, nil)
		if err == nil {
			rel()
		}
		expBlocked <- err
	}()
	waitDoorPosition(t, door, "exp-blocked")

	paidRel, err := door.Acquire(context.Background(), "paid", true, nil)
	if err != nil {
		t.Fatalf("paid acquire: %v", err)
	}

	select {
	case err := <-expBlocked:
		t.Fatalf("explorer acquired while paid held reserved (err=%v)", err)
	case <-time.After(50 * time.Millisecond):
	}

	paidRel()
	select {
	case err := <-expBlocked:
		t.Fatalf("explorer acquired from reserved release (err=%v)", err)
	case <-time.After(50 * time.Millisecond):
	}

	for _, rel := range releases {
		rel()
	}

	select {
	case err := <-expBlocked:
		if err != nil {
			t.Fatalf("explorer after releases: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("explorer should acquire after general slots free")
	}
}

func waitDoorPosition(t *testing.T, door *provisionDoor, namespace string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := door.Position(namespace); ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("waiter %s never registered", namespace)
}

func TestProvisionDoor_PaidAcquiresWhenGeneralFullAtWidth10(t *testing.T) {
	t.Parallel()

	door := newProvisionDoor(10, 240)
	if door.reservedWidth != 2 || door.generalWidth != 8 {
		t.Fatalf("reserved=%d general=%d", door.reservedWidth, door.generalWidth)
	}
	var releases []func()
	for i := 0; i < 8; i++ {
		rel, err := door.Acquire(context.Background(), fmt.Sprintf("exp-%d", i), false, nil)
		if err != nil {
			t.Fatalf("explorer %d: %v", i, err)
		}
		releases = append(releases, rel)
	}
	paidRel, err := door.Acquire(context.Background(), "paid", true, nil)
	if err != nil {
		t.Fatalf("paid: %v", err)
	}
	paidRel()
	for _, rel := range releases {
		rel()
	}
}

func TestProvisionDoor_PaidReleaseWakesPaidBeforeExplorer(t *testing.T) {
	t.Parallel()

	door := newProvisionDoor(2, 240) // reserved=1, general=1
	holdPaid, err := door.Acquire(context.Background(), "paid-holder", true, nil)
	if err != nil {
		t.Fatalf("paid holder: %v", err)
	}
	holdExp, err := door.Acquire(context.Background(), "exp-holder", false, nil)
	if err != nil {
		t.Fatalf("exp holder: %v", err)
	}

	var order []string
	var mu sync.Mutex
	record := func(name string) {
		mu.Lock()
		order = append(order, name)
		mu.Unlock()
	}

	waitPaid := make(chan struct{})
	waitExp := make(chan struct{})
	go func() {
		rel, err := door.Acquire(context.Background(), "paid-wait", true, nil)
		if err != nil {
			t.Errorf("paid wait: %v", err)
			close(waitPaid)
			return
		}
		record("paid-wait")
		rel()
		close(waitPaid)
	}()
	go func() {
		rel, err := door.Acquire(context.Background(), "exp-wait", false, nil)
		if err != nil {
			t.Errorf("exp wait: %v", err)
			close(waitExp)
			return
		}
		record("exp-wait")
		rel()
		close(waitExp)
	}()
	waitDoorPosition(t, door, "paid-wait")
	waitDoorPosition(t, door, "exp-wait")

	holdPaid() // frees reserved → paid-wait
	<-waitPaid
	holdExp() // frees general → exp-wait (no paid waiting)

	select {
	case <-waitExp:
	case <-time.After(2 * time.Second):
		t.Fatal("exp-wait timed out")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "paid-wait" || order[1] != "exp-wait" {
		t.Fatalf("wake order = %v, want [paid-wait exp-wait]", order)
	}
}

func TestProvisionDoor_WidthOneSharesSlot(t *testing.T) {
	t.Parallel()

	door := newProvisionDoor(1, 240)
	hold, err := door.Acquire(context.Background(), "exp", false, nil)
	if err != nil {
		t.Fatalf("hold: %v", err)
	}

	acquired := make(chan struct{})
	go func() {
		rel, err := door.Acquire(context.Background(), "paid", true, nil)
		if err != nil {
			t.Errorf("paid wait: %v", err)
			close(acquired)
			return
		}
		rel()
		close(acquired)
	}()
	time.Sleep(30 * time.Millisecond)
	select {
	case <-acquired:
		t.Fatal("paid should block on single shared slot")
	default:
	}
	hold()
	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("paid should acquire after release")
	}
}
