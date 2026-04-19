package metrics

import (
	"testing"
	"time"
)

func TestEventBus_EmitDrainCleanup(t *testing.T) {
	t.Parallel()
	eb := NewEventBus()
	ns := "ns-a"

	for i := 0; i < 5; i++ {
		eb.Emit(ns, Event{Timestamp: time.Now(), Type: "action", Source: "loadgen", Title: "t", Description: "d"})
	}
	got := eb.Drain(ns)
	if len(got) != 5 {
		t.Fatalf("first drain: want 5 events, got %d", len(got))
	}
	got2 := eb.Drain(ns)
	if len(got2) != 0 {
		t.Fatalf("second drain on empty: want 0, got %d", len(got2))
	}

	eb.Emit(ns, Event{Title: "a"})
	eb.Emit(ns, Event{Title: "b"})
	got3 := eb.Drain(ns)
	if len(got3) != 2 {
		t.Fatalf("after re-emit: want 2, got %d", len(got3))
	}

	eb.Cleanup(ns)
	eb.Emit(ns, Event{Title: "solo"})
	got4 := eb.Drain(ns)
	if len(got4) != 1 || got4[0].Title != "solo" {
		t.Fatalf("after cleanup: want 1 event 'solo', got %+v", got4)
	}
}

func TestEventBus_NilSafe(t *testing.T) {
	t.Parallel()
	var eb *EventBus
	eb.Emit("x", Event{})
	eb.Drain("x")
	eb.Cleanup("x")
}
