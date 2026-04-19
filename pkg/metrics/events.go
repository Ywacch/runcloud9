package metrics

import "sync"

// EventBus buffers handler-emitted events per namespace until the next Collect drain.
type EventBus struct {
	mu     sync.Mutex
	events map[string][]Event // namespace → pending (FIFO within a tick)
}

// NewEventBus creates an empty event bus.
func NewEventBus() *EventBus {
	return &EventBus{events: make(map[string][]Event)}
}

// Emit appends an event for a namespace. Safe for concurrent calls from HTTP handlers.
func (eb *EventBus) Emit(namespace string, e Event) {
	if eb == nil || namespace == "" {
		return
	}
	eb.mu.Lock()
	defer eb.mu.Unlock()
	eb.events[namespace] = append(eb.events[namespace], e)
}

// Drain removes and returns all pending events for a namespace. Called once per Collect tick.
func (eb *EventBus) Drain(namespace string) []Event {
	if eb == nil || namespace == "" {
		return nil
	}
	eb.mu.Lock()
	defer eb.mu.Unlock()
	out := eb.events[namespace]
	eb.events[namespace] = nil
	return out
}

// Cleanup drops all state for a namespace (call on deployment teardown).
func (eb *EventBus) Cleanup(namespace string) {
	if eb == nil || namespace == "" {
		return
	}
	eb.mu.Lock()
	defer eb.mu.Unlock()
	delete(eb.events, namespace)
}
