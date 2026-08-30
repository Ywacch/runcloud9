package api

import (
	"context"
	"math"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

type doorPool int

const (
	doorPoolGeneral doorPool = iota
	doorPoolReserved
)

type doorWaiter struct {
	namespace string
	paid      bool
	done      chan struct{}
	release   func()
}

// provisionDoor bounds concurrent orchestrations and tracks waiters for ETA.
// A nil *provisionDoor means the doorway is off (unlimited orchestrations).
type provisionDoor struct {
	semGeneral    *semaphore.Weighted
	semReserved   *semaphore.Weighted // nil when reservedWidth == 0
	width         int
	generalWidth  int
	reservedWidth int
	holdETA       *provisionETA

	mu              sync.Mutex
	waitersPaid     []string
	waitersExplorer []string
	pendingWaiters  map[string]*doorWaiter
}

// doorReserve returns paid-first reserved slots for a doorway of the given width.
func doorReserve(width int) int {
	if width <= 1 {
		return 0
	}
	if width <= 5 {
		return 1
	}
	reserved := width / 5
	if reserved < 1 {
		reserved = 1
	}
	if width-reserved < 1 {
		return width - 1
	}
	return reserved
}

func newProvisionDoor(width int, holdFallbackSec float64) *provisionDoor {
	if width <= 0 {
		return nil
	}
	reserved := doorReserve(width)
	general := width - reserved
	d := &provisionDoor{
		semGeneral:     semaphore.NewWeighted(int64(general)),
		width:          width,
		generalWidth:   general,
		reservedWidth:  reserved,
		holdETA:        newProvisionETA(holdFallbackSec),
		pendingWaiters: make(map[string]*doorWaiter),
	}
	if reserved > 0 {
		d.semReserved = semaphore.NewWeighted(int64(reserved))
	}
	return d
}

// Acquire takes one doorway slot. Paid users try reserved then general; explorers
// use general only. If the door is full, namespace is registered in the lane FIFO,
// then onWaiting runs once before blocking. Caller must invoke the returned release.
func (d *provisionDoor) Acquire(ctx context.Context, namespace string, paid bool, onWaiting func()) (release func(), err error) {
	release = func() {}
	if d == nil || d.semGeneral == nil {
		return release, nil
	}

	if rel, ok := d.tryAcquireImmediate(paid); ok {
		return rel, nil
	}

	w := &doorWaiter{
		namespace: namespace,
		paid:      paid,
		done:      make(chan struct{}),
	}

	d.mu.Lock()
	if rel, ok := d.tryAcquireImmediateLocked(paid); ok {
		d.mu.Unlock()
		return rel, nil
	}
	d.registerWaiterLocked(namespace, paid, w)
	d.mu.Unlock()

	if onWaiting != nil {
		onWaiting()
	}

	select {
	case <-w.done:
		if w.release == nil {
			return nil, context.Canceled
		}
		return w.release, nil
	case <-ctx.Done():
		d.mu.Lock()
		if _, waiting := d.pendingWaiters[namespace]; !waiting {
			d.mu.Unlock()
			if w.release == nil {
				return nil, ctx.Err()
			}
			return w.release, nil
		}
		d.unregisterWaiterLocked(namespace)
		d.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (d *provisionDoor) tryAcquireImmediate(paid bool) (func(), bool) {
	if paid && d.semReserved != nil && d.semReserved.TryAcquire(1) {
		return d.makeRelease(doorPoolReserved), true
	}
	if d.semGeneral.TryAcquire(1) {
		return d.makeRelease(doorPoolGeneral), true
	}
	return nil, false
}

func (d *provisionDoor) tryAcquireImmediateLocked(paid bool) (func(), bool) {
	return d.tryAcquireImmediate(paid)
}

func (d *provisionDoor) makeRelease(pool doorPool) func() {
	return func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		switch pool {
		case doorPoolReserved:
			if d.handoffPaidLocked(doorPoolReserved) {
				return
			}
			if d.semReserved != nil {
				d.semReserved.Release(1)
			}
		case doorPoolGeneral:
			if d.handoffPaidLocked(doorPoolGeneral) {
				return
			}
			if d.handoffExplorerLocked() {
				return
			}
			d.semGeneral.Release(1)
		}
	}
}

func (d *provisionDoor) registerWaiterLocked(namespace string, paid bool, w *doorWaiter) {
	if namespace == "" {
		return
	}
	for _, ns := range d.waitersPaid {
		if ns == namespace {
			return
		}
	}
	for _, ns := range d.waitersExplorer {
		if ns == namespace {
			return
		}
	}
	if paid {
		d.waitersPaid = append(d.waitersPaid, namespace)
	} else {
		d.waitersExplorer = append(d.waitersExplorer, namespace)
	}
	d.pendingWaiters[namespace] = w
}

func (d *provisionDoor) unregisterWaiterLocked(namespace string) {
	w := d.pendingWaiters[namespace]
	if w == nil {
		return
	}
	delete(d.pendingWaiters, namespace)
	if w.paid {
		d.waitersPaid = removeNamespace(d.waitersPaid, namespace)
	} else {
		d.waitersExplorer = removeNamespace(d.waitersExplorer, namespace)
	}
}

func removeNamespace(list []string, namespace string) []string {
	out := list[:0]
	for _, ns := range list {
		if ns != namespace {
			out = append(out, ns)
		}
	}
	return out
}

func (d *provisionDoor) handoffPaidLocked(pool doorPool) bool {
	for len(d.waitersPaid) > 0 {
		ns := d.waitersPaid[0]
		d.waitersPaid = d.waitersPaid[1:]
		w := d.pendingWaiters[ns]
		if w == nil {
			continue
		}
		delete(d.pendingWaiters, ns)
		w.release = d.makeRelease(pool)
		close(w.done)
		return true
	}
	return false
}

func (d *provisionDoor) handoffExplorerLocked() bool {
	for len(d.waitersExplorer) > 0 {
		ns := d.waitersExplorer[0]
		d.waitersExplorer = d.waitersExplorer[1:]
		w := d.pendingWaiters[ns]
		if w == nil {
			continue
		}
		delete(d.pendingWaiters, ns)
		w.release = d.makeRelease(doorPoolGeneral)
		close(w.done)
		return true
	}
	return false
}

func (d *provisionDoor) enterWait(namespace string, paid bool) {
	if d == nil || namespace == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, ns := range d.waitersPaid {
		if ns == namespace {
			return
		}
	}
	for _, ns := range d.waitersExplorer {
		if ns == namespace {
			return
		}
	}
	if paid {
		d.waitersPaid = append(d.waitersPaid, namespace)
	} else {
		d.waitersExplorer = append(d.waitersExplorer, namespace)
	}
}

func (d *provisionDoor) leaveWait(namespace string) {
	if d == nil || namespace == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.waitersPaid = removeNamespace(d.waitersPaid, namespace)
	d.waitersExplorer = removeNamespace(d.waitersExplorer, namespace)
}

// Position returns the 1-based FIFO place of namespace among door waiters in its lane.
func (d *provisionDoor) Position(namespace string) (int, bool) {
	pos, _, ok := d.positionAndLane(namespace)
	return pos, ok
}

func (d *provisionDoor) positionAndLane(namespace string) (pos int, paid bool, ok bool) {
	if d == nil || namespace == "" {
		return 0, false, false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for i, ns := range d.waitersPaid {
		if ns == namespace {
			return i + 1, true, true
		}
	}
	for i, ns := range d.waitersExplorer {
		if ns == namespace {
			return i + 1, false, true
		}
	}
	return 0, false, false
}

// Estimate returns position and wait seconds for the waiter's lane.
// Paid: ceil(pos * holdAvg / width). Explorer: ceil(pos * holdAvg / generalWidth).
func (d *provisionDoor) Estimate(namespace string) (pos, etaSec int, ok bool) {
	pos, paid, ok := d.positionAndLane(namespace)
	if !ok || d == nil || d.width <= 0 {
		return 0, 0, false
	}
	divisor := d.generalWidth
	if paid {
		divisor = d.width
	}
	if divisor <= 0 {
		divisor = 1
	}
	avg := d.holdETA.Seconds()
	eta := int(math.Ceil(float64(pos) * avg / float64(divisor)))
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
	paid bool,
	onWaiting func(),
) (workCtx context.Context, cancel context.CancelFunc, release func(), err error) {
	release = func() {}
	if door != nil {
		release, err = door.Acquire(lifeCtx, namespace, paid, onWaiting)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	workCtx, cancel = context.WithTimeout(lifeCtx, timeout)
	return workCtx, cancel, release, nil
}
