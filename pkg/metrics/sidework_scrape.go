package metrics

import (
	"context"
	"time"

	"github.com/ywacch/cloud9/pkg/metrics/sidework"
)

// SideWorkPool returns the process-wide side-work pool (may be nil on incomplete collectors).
func (c *MetricsCollector) SideWorkPool() *sidework.Pool {
	if c == nil {
		return nil
	}
	return c.sideWork
}

// SetSideWorkInflightObserver wires cloud9_sidework_inflight (or tests).
func (c *MetricsCollector) SetSideWorkInflightObserver(fn func(int)) {
	if c == nil || c.sideWork == nil || fn == nil {
		return
	}
	c.sideWork.SetInflightObserver(fn)
}

func (c *MetricsCollector) hasLivePublisher(namespace string) bool {
	if c == nil || namespace == "" {
		return false
	}
	c.publishersMu.Lock()
	_, ok := c.publishers[namespace]
	c.publishersMu.Unlock()
	return ok
}

// beginSideWorkScrape gates direct HTTP scrapes: live publisher, cadence, and pool slot (non-blocking).
func (c *MetricsCollector) beginSideWorkScrape(ctx context.Context, namespace string) (release func(), ok bool) {
	noop := func() {}
	if c == nil {
		return noop, false
	}
	if c.sideWork == nil {
		return noop, true
	}
	if !c.hasLivePublisher(namespace) {
		return noop, false
	}
	now := time.Now()
	c.sideWorkMu.Lock()
	if c.sideWorkLast == nil {
		c.sideWorkLast = make(map[string]time.Time)
	}
	last := c.sideWorkLast[namespace]
	cadence := c.sideWorkCadence
	c.sideWorkMu.Unlock()
	if cadence > 0 && now.Sub(last) < cadence {
		return noop, false
	}
	release, ok = c.sideWork.Acquire(ctx, 0)
	if !ok {
		return noop, false
	}
	c.sideWorkMu.Lock()
	c.sideWorkLast[namespace] = now
	c.sideWorkMu.Unlock()
	return release, true
}

func initSideWork(c *MetricsCollector) {
	if c == nil {
		return
	}
	c.sideWork = sidework.New(sidework.SlotsFromEnv(), nil)
	c.sideWorkCadence = sidework.CadenceFromEnv()
	c.sideWorkLast = make(map[string]time.Time)
}
