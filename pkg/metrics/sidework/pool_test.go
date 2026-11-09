package sidework

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPool_CapsInflight(t *testing.T) {
	t.Parallel()
	const slots = 32
	p := New(slots, time.Now)
	var peak atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, ok := p.Acquire(context.Background(), 500*time.Millisecond)
			if !ok {
				return
			}
			defer release()
			n := int32(p.Inflight())
			for {
				old := peak.Load()
				if n <= old {
					break
				}
				if peak.CompareAndSwap(old, n) {
					break
				}
			}
			time.Sleep(2 * time.Millisecond)
		}()
	}
	wg.Wait()
	if got := peak.Load(); got > slots {
		t.Fatalf("peak inflight %d exceeds slots %d", got, slots)
	}
	if p.Inflight() != 0 {
		t.Fatalf("leaked inflight %d", p.Inflight())
	}
}

func TestPool_SkipAfterWait(t *testing.T) {
	t.Parallel()
	p := New(1, time.Now)
	release, ok := p.Acquire(context.Background(), 0)
	if !ok {
		t.Fatal("expected first acquire")
	}
	defer release()
	_, ok = p.Acquire(context.Background(), 20*time.Millisecond)
	if ok {
		t.Fatal("expected acquire to time out while slot held")
	}
}
