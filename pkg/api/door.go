package api

import (
	"context"
	"math"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

// provisionDoor bounds concurrent orchestrations and tracks waiters for ETA.
// A nil *provisionDoor means the doorway is off (unlimited orchestrations).
type provisionDoor struct {
	sem     *semaphore.Weighted
	width   int
	holdETA *provisionETA

	mu      sync.Mutex
	waiters []string // FIFO namespaces waiting on Acquire
}

func newProvisionDoor(width int, holdFallbackSec float64) *provisionDoor {
	if width <= 0 {
		return nil
	}
	return &provisionDoor{
		sem:     semaphore.NewWeighted(int64(width)),
		width:   width,
		holdETA: newProvisionETA(holdFallbackSec),
	}
}

// Acquire takes one doorway slot. If the door is full, namespace is registered
// in the waiter FIFO, then onWaiting runs once before blocking. Caller must
// invoke the returned release when finished.
func (d *provisionDoor) Acquire(ctx context.Context, namespace string, onWaiting func()) (release func(), err error) {
	release = func() {}
	if d == nil || d.sem == nil {
		return release, nil
	}

	if d.sem.TryAcquire(1) {
		return func() { d.sem.Release(1) }, nil
	}

	d.enterWait(namespace)
	defer d.leaveWait(namespace)
	if onWaiting != nil {
		onWaiting()
	}

	if err := d.sem.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	return func() { d.sem.Release(1) }, nil
}

func (d *provisionDoor) enterWait(namespace string) {
	if d == nil || namespace == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, ns := range d.waiters {
		if ns == namespace {
			return
		}
	}
	d.waiters = append(d.waiters, namespace)
}

func (d *provisionDoor) leaveWait(namespace string) {
	if d == nil || namespace == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	out := d.waiters[:0]
	for _, ns := range d.waiters {
		if ns != namespace {
			out = append(out, ns)
		}
	}
	d.waiters = out
}

// Position returns the 1-based FIFO place of namespace among door waiters.
func (d *provisionDoor) Position(namespace string) (int, bool) {
	if d == nil || namespace == "" {
		return 0, false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for i, ns := range d.waiters {
		if ns == namespace {
			return i + 1, true
		}
	}
	return 0, false
}

// Estimate returns position and wait seconds: ceil(pos * holdAvg / width).
func (d *provisionDoor) Estimate(namespace string) (pos, etaSec int, ok bool) {
	pos, ok = d.Position(namespace)
	if !ok || d == nil || d.width <= 0 {
		return 0, 0, false
	}
	avg := d.holdETA.Seconds()
	eta := int(math.Ceil(float64(pos) * avg / float64(d.width)))
	if eta < 1 {
		eta = 1
	}
	return pos, eta, true
}

// ObserveHold records time spent holding a doorway slot (acquire → release).
func (d *provisionDoor) ObserveHold(dur time.Duration) {
	if d == nil {
		return
	}
	d.holdETA.Observe(dur)
}

// beginProvisionWork acquires one doorway slot (if door != nil) under lifeCtx,
// then returns a child context whose deadline starts only after the slot is held.
// If the door is full, onWaiting is invoked once before blocking on Acquire.
// lifeCtx cancellation aborts both the wait and the work context.
// Caller must invoke cancel and release (in any order) when finished.
func beginProvisionWork(
	lifeCtx context.Context,
	door *provisionDoor,
	namespace string,
	timeout time.Duration,
	onWaiting func(),
) (workCtx context.Context, cancel context.CancelFunc, release func(), err error) {
	release = func() {}
	if door != nil {
		release, err = door.Acquire(lifeCtx, namespace, onWaiting)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	workCtx, cancel = context.WithTimeout(lifeCtx, timeout)
	return workCtx, cancel, release, nil
}
