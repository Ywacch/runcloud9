package metrics

import (
	"context"
	"time"
)

// pgSessionStableInterval is the Prom poll gap while waiting for seed counters
// to stop changing. Tests may shorten this.
var pgSessionStableInterval = 2 * time.Second

// pgSessionStableTimeout bounds WaitAndRebaselinePostgresSession so provision
// does not stall if the exporter never publishes seed totals.
var pgSessionStableTimeout = 45 * time.Second

type pgEngineTotals struct {
	read, write     float64
	readOK, writeOK bool
}

func pgEngineTotalsStable(prev, cur pgEngineTotals) bool {
	if !prev.writeOK || !cur.writeOK || cur.write <= 0 {
		return false
	}
	if prev.write != cur.write {
		return false
	}
	if !prev.readOK || !cur.readOK {
		return false
	}
	return prev.read == cur.read
}

// HoldPostgresSession suppresses Postgres engine session totals (tiles stay 0)
// until OpenPostgresSession or WaitAndRebaselinePostgresSession. Call at the
// start of provision when a seeder will load a database dump.
func (c *MetricsCollector) HoldPostgresSession(namespace string) {
	if c == nil || namespace == "" {
		return
	}
	c.pgHoldMu.Lock()
	defer c.pgHoldMu.Unlock()
	if c.pgHold == nil {
		c.pgHold = make(map[string]bool)
	}
	c.pgHold[namespace] = true
}

// OpenPostgresSession allows ObserveDeltas for Postgres engine session totals.
// The next scrape becomes the baseline when last was never snapshotted.
func (c *MetricsCollector) OpenPostgresSession(namespace string) {
	if c == nil || namespace == "" {
		return
	}
	c.pgHoldMu.Lock()
	defer c.pgHoldMu.Unlock()
	delete(c.pgHold, namespace)
}

func (c *MetricsCollector) postgresSessionHeld(namespace string) bool {
	if c == nil || namespace == "" {
		return false
	}
	c.pgHoldMu.Lock()
	defer c.pgHoldMu.Unlock()
	return c.pgHold[namespace]
}

func (c *MetricsCollector) queryPostgresEngineTotals(ctx context.Context, ns string) pgEngineTotals {
	var t pgEngineTotals
	if c == nil {
		return t
	}
	if v, ok, err := c.queryScalar(ctx, postgresUserTableReadTotal9mQuery(ns)); err == nil && ok {
		t.read = v
		t.readOK = true
	}
	if v, ok, err := c.queryScalar(ctx, postgresUserTableWriteTotal9mQuery(ns)); err == nil && ok {
		t.write = v
		t.writeOK = true
	}
	return t
}

func (c *MetricsCollector) snapshotPostgresSession(ns string, t pgEngineTotals) {
	if c == nil || c.sessionAcc == nil {
		return
	}
	readByPod := map[string]float64{}
	writeByPod := map[string]float64{}
	if t.readOK {
		readByPod[postgresClusterTotalKey] = t.read
	}
	if t.writeOK && t.write > 0 {
		writeByPod[postgresClusterTotalKey] = t.write
	}
	c.sessionAcc.rebaseline(ns, seriesPgRead, readByPod)
	c.sessionAcc.rebaseline(ns, seriesPgWrite, writeByPod)
}

// RebaselinePostgresSession snapshots current primary user-table counters and
// zeros Postgres engine session sums. Missing PromQL results clear last so the
// next ObserveDeltas scrape becomes the baseline.
func (c *MetricsCollector) RebaselinePostgresSession(ctx context.Context, ns string) {
	if c == nil || ns == "" || c.sessionAcc == nil {
		return
	}
	c.snapshotPostgresSession(ns, c.queryPostgresEngineTotals(ctx, ns))
}

// WaitAndRebaselinePostgresSession polls Prometheus until read and write user-table
// totals are stable (two equal samples, writes > 0), snapshots that baseline, then
// opens session accumulation. On timeout it snapshots the last sample (or clears
// last when writes are still 0) and opens so tiles are not stuck.
func (c *MetricsCollector) WaitAndRebaselinePostgresSession(ctx context.Context, ns string) {
	if c == nil || ns == "" {
		return
	}
	defer c.OpenPostgresSession(ns)
	if c.sessionAcc == nil {
		return
	}

	waitCtx, cancel := context.WithTimeout(ctx, pgSessionStableTimeout)
	defer cancel()

	interval := pgSessionStableInterval
	if interval <= 0 {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var prev pgEngineTotals
	havePrev := false
	for {
		cur := c.queryPostgresEngineTotals(waitCtx, ns)
		if havePrev && pgEngineTotalsStable(prev, cur) {
			c.snapshotPostgresSession(ns, cur)
			return
		}
		prev = cur
		havePrev = true

		select {
		case <-waitCtx.Done():
			c.snapshotPostgresSession(ns, cur)
			return
		case <-ticker.C:
		}
	}
}
