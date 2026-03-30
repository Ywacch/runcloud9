package teardown

import (
	"sync"
	"time"
)

// Scheduler manages a 9-minute auto-teardown timer per deployment.
// one goroutine-backed timer per active deployment.
// time.AfterFunc fires the callback in a new goroutine after the duration.
// We keep a map so we can cancel timers if teardown is requested early (DELETE endpoint).
type Scheduler struct {
	mu         sync.Mutex
	timers     map[string]*time.Timer
	teardownFn func(namespace string)
}

// NewScheduler creates a scheduler that will call teardownFn(namespace) when each timer fires.
func NewScheduler(teardownFn func(namespace string)) *Scheduler {
	return &Scheduler{
		timers:     make(map[string]*time.Timer),
		teardownFn: teardownFn,
	}
}

// Schedule registers a timer that fires teardownFn(namespace) at fireAt.
// If a timer already exists for this namespace, it is replaced.
// UNDERSTAND: time.AfterFunc does not block — it returns immediately.
// The callback runs in a separate goroutine after the duration.
func (s *Scheduler) Schedule(namespace string, fireAt time.Time) {
	remaining := time.Until(fireAt)
	if remaining <= 0 {
		go s.teardownFn(namespace)
		return
	}
	s.mu.Lock()
	if old, ok := s.timers[namespace]; ok {
		old.Stop()
	}
	t := time.AfterFunc(remaining, func() {
		s.teardownFn(namespace)
		s.mu.Lock()
		delete(s.timers, namespace)
		s.mu.Unlock()
	})
	s.timers[namespace] = t
	s.mu.Unlock()
}

// Cancel stops and removes the timer for this namespace.
// Called when DELETE /deployments/{id} is received so teardown doesn't fire twice.
func (s *Scheduler) Cancel(namespace string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.timers[namespace]; ok {
		t.Stop()
		delete(s.timers, namespace)
	}
}
