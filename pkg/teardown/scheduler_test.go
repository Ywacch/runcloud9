package teardown

import (
	"sync"
	"testing"
	"time"
)

func TestScheduler_FiresAfterDuration(t *testing.T) {
	ch := make(chan string, 1)
	fn := func(namespace string) { ch <- namespace }
	s := NewScheduler(fn)

	s.Schedule("ns-1", time.Now().Add(50*time.Millisecond))

	select {
	case ns := <-ch:
		if ns != "ns-1" {
			t.Errorf("expected ns-1, got %s", ns)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected callback to fire within 200ms")
	}
}

func TestScheduler_Cancel(t *testing.T) {
	ch := make(chan string, 1)
	fn := func(namespace string) { ch <- namespace }
	s := NewScheduler(fn)

	s.Schedule("ns-cancel", time.Now().Add(100*time.Millisecond))
	s.Cancel("ns-cancel")

	select {
	case <-ch:
		t.Fatal("callback should not have fired after Cancel")
	case <-time.After(200 * time.Millisecond):
		// expected: no event
	}
}

func TestScheduler_ReplaceExisting(t *testing.T) {
	var count int
	var mu sync.Mutex
	ch := make(chan string, 2)
	fn := func(namespace string) {
		mu.Lock()
		count++
		mu.Unlock()
		ch <- namespace
	}
	s := NewScheduler(fn)

	// Schedule once for 30ms, then replace with 50ms
	s.Schedule("ns-replace", time.Now().Add(30*time.Millisecond))
	s.Schedule("ns-replace", time.Now().Add(50*time.Millisecond))

	// Only one callback should fire (the second timer)
	select {
	case ns := <-ch:
		if ns != "ns-replace" {
			t.Errorf("expected ns-replace, got %s", ns)
		}
		mu.Lock()
		c := count
		mu.Unlock()
		if c != 1 {
			t.Errorf("expected exactly 1 callback, got %d", c)
		}
	case <-time.After(150 * time.Millisecond):
		t.Fatal("expected one callback within 150ms")
	}

	// No second callback
	select {
	case <-ch:
		t.Fatal("expected only one callback")
	case <-time.After(50 * time.Millisecond):
	}
}
