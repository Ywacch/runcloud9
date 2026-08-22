package probes

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type scriptedProbe struct {
	mu       sync.Mutex
	writeVer int64
	readVer  int64
	readOK   bool
	readErr  error
	writeErr error
	closed   bool
}

func (p *scriptedProbe) EnsureSchema(context.Context) error { return nil }

func (p *scriptedProbe) WriteVersion(context.Context) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.writeErr != nil {
		return 0, p.writeErr
	}
	p.writeVer++
	return p.writeVer, nil
}

func (p *scriptedProbe) ReadVersion(context.Context) (int64, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.readErr != nil {
		return 0, false, p.readErr
	}
	return p.readVer, p.readOK, nil
}

func (p *scriptedProbe) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	return nil
}

func TestObserver_StartStop_idempotent(t *testing.T) {
	t.Parallel()
	primary := &scriptedProbe{readOK: true}
	replica := &scriptedProbe{readOK: true, readVer: 1}
	var dialCount atomic.Int32
	o := NewObserver(
		WithTickInterval(50*time.Millisecond),
		WithProbeDial(func(ctx context.Context, host string) (ConsistencyProbe, error) {
			dialCount.Add(1)
			if host == primaryHost("ns-idem") {
				return primary, nil
			}
			return replica, nil
		}),
	)

	o.Start("ns-idem")
	o.Start("ns-idem")
	waitForProbe(t, o, "ns-idem", func(r ProbeResult) bool {
		return r.WrittenVersion > 0
	})
	if dialCount.Load() != 2 {
		t.Fatalf("dial count = %d, want 2 (primary+replica once)", dialCount.Load())
	}

	o.Stop("ns-idem")
	o.Stop("ns-idem")

	if !primary.closed || !replica.closed {
		t.Fatalf("probes not closed: primary=%v replica=%v", primary.closed, replica.closed)
	}
}

func TestObserver_loopUpdatesLatest(t *testing.T) {
	t.Parallel()
	primary := &scriptedProbe{}
	replica := &scriptedProbe{readOK: true}
	o := NewObserver(
		WithTickInterval(20*time.Millisecond),
		WithProbeDial(func(ctx context.Context, host string) (ConsistencyProbe, error) {
			if host == primaryHost("ns-loop") {
				return primary, nil
			}
			return replica, nil
		}),
	)

	o.Start("ns-loop")
	t.Cleanup(func() { o.Stop("ns-loop") })

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if result, ok := o.Latest("ns-loop"); ok && result.WrittenVersion > 0 {
			if result.ReplicaVersion != replica.readVer {
				t.Fatalf("replica version: got %d want %d", result.ReplicaVersion, replica.readVer)
			}
			if result.RYWStalenessVersions != StalenessVersions(result.WrittenVersion, result.ReplicaVersion) {
				t.Fatalf("staleness: %+v", result)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for probe result")
}

func TestObserver_monotonicViolation(t *testing.T) {
	t.Parallel()
	primary := &scriptedProbe{}
	replica := &scriptedProbe{readOK: true, readVer: 5}
	o := NewObserver(
		WithTickInterval(15*time.Millisecond),
		WithProbeDial(func(ctx context.Context, host string) (ConsistencyProbe, error) {
			if host == primaryHost("ns-mono") {
				return primary, nil
			}
			return replica, nil
		}),
	)

	o.Start("ns-mono")
	t.Cleanup(func() { o.Stop("ns-mono") })

	waitForProbe(t, o, "ns-mono", func(r ProbeResult) bool {
		return r.WrittenVersion > 0 && !r.MonotonicViolation
	})

	replica.readVer = 3
	waitForProbe(t, o, "ns-mono", func(r ProbeResult) bool {
		return r.MonotonicViolation
	})
}

func TestObserver_probeErrorRecorded(t *testing.T) {
	t.Parallel()
	primary := &scriptedProbe{writeErr: fmt.Errorf("write failed")}
	replica := &scriptedProbe{readOK: true}
	o := NewObserver(
		WithTickInterval(15*time.Millisecond),
		WithProbeDial(func(ctx context.Context, host string) (ConsistencyProbe, error) {
			if host == primaryHost("ns-err") {
				return primary, nil
			}
			return replica, nil
		}),
	)

	o.Start("ns-err")
	t.Cleanup(func() { o.Stop("ns-err") })

	waitForProbe(t, o, "ns-err", func(r ProbeResult) bool {
		return r.LastErr != ""
	})
}

func waitForProbe(t *testing.T, o *Observer, namespace string, ok func(ProbeResult) bool) {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if result, has := o.Latest(namespace); has && ok(result) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for probe condition")
}

func TestPrimaryReplicaHost(t *testing.T) {
	t.Parallel()
	if got := primaryHost("ns1"); got != "db.ns1.svc.cluster.local" {
		t.Fatalf("primaryHost: %q", got)
	}
	if got := replicaHost("ns1"); got != "db-replica.ns1.svc.cluster.local" {
		t.Fatalf("replicaHost: %q", got)
	}
}
