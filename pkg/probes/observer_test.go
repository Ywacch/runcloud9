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

func (p *scriptedProbe) setReadVer(v int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.readVer = v
}

func (p *scriptedProbe) readVersion() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.readVer
}

func (p *scriptedProbe) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
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

	o.Start("ns-idem", ObserverConfig{NeedReplica: true})
	o.Start("ns-idem", ObserverConfig{NeedReplica: true})
	waitForProbe(t, o, "ns-idem", func(r ProbeResult) bool {
		return r.WrittenVersion > 0
	})
	if dialCount.Load() != 2 {
		t.Fatalf("dial count = %d, want 2 (primary+replica once)", dialCount.Load())
	}

	o.Stop("ns-idem")
	o.Stop("ns-idem")

	if !primary.isClosed() || !replica.isClosed() {
		t.Fatalf("probes not closed: primary=%v replica=%v", primary.isClosed(), replica.isClosed())
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

	o.Start("ns-loop", ObserverConfig{NeedReplica: true})
	t.Cleanup(func() { o.Stop("ns-loop") })

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if result, ok := o.Latest("ns-loop"); ok && result.WrittenVersion > 0 {
			if result.ReplicaVersion != replica.readVersion() {
				t.Fatalf("replica version: got %d want %d", result.ReplicaVersion, replica.readVersion())
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

	o.Start("ns-mono", ObserverConfig{NeedReplica: true})
	t.Cleanup(func() { o.Stop("ns-mono") })

	waitForProbe(t, o, "ns-mono", func(r ProbeResult) bool {
		return r.WrittenVersion > 0 && !r.MonotonicViolation
	})

	replica.setReadVer(3)
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

	o.Start("ns-err", ObserverConfig{NeedReplica: true})
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

type scriptedCacheProbe struct {
	mu       sync.Mutex
	stored   ConsistencyToken
	hit      bool
	getErr   error
	setErr   error
	closed   bool
	setCalls int
}

func (p *scriptedCacheProbe) Get(context.Context, string) (ConsistencyToken, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.getErr != nil {
		return nil, false, p.getErr
	}
	return p.stored, p.hit, nil
}

func (p *scriptedCacheProbe) Set(_ context.Context, _ string, tok ConsistencyToken, _ time.Duration) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.setCalls++
	if p.setErr != nil {
		return p.setErr
	}
	p.stored = tok
	p.hit = true
	return nil
}

func (p *scriptedCacheProbe) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	return nil
}

func (p *scriptedCacheProbe) setCallsCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.setCalls
}

func newCacheObserverTest(t *testing.T, primary *scriptedProbe, cache *scriptedCacheProbe) *Observer {
	t.Helper()
	o := NewObserver(
		WithTickInterval(50*time.Millisecond),
		WithProbeDial(func(ctx context.Context, host string) (ConsistencyProbe, error) {
			if host == primaryHost("ns-cache") {
				return primary, nil
			}
			return nil, fmt.Errorf("unexpected host %q", host)
		}),
		WithCacheDial(func(ctx context.Context, namespace string, opts CacheDialOptions) (CacheProbe, error) {
			if namespace != "ns-cache" {
				return nil, fmt.Errorf("unexpected namespace %q", namespace)
			}
			return cache, nil
		}),
	)
	return o
}

func TestObserver_cacheMissWarmsWithoutStale(t *testing.T) {
	t.Parallel()
	primary := &scriptedProbe{readOK: true}
	cache := &scriptedCacheProbe{}
	o := newCacheObserverTest(t, primary, cache)
	o.Start("ns-cache", ObserverConfig{NeedCache: true})
	t.Cleanup(func() { o.Stop("ns-cache") })

	waitForProbe(t, o, "ns-cache", func(r ProbeResult) bool {
		return r.WrittenVersion > 0 && !r.CacheDbStale
	})
	if cache.setCallsCount() != 1 {
		t.Fatalf("set calls = %d, want 1 on miss warm", cache.setCallsCount())
	}
}

func TestObserver_cacheStaleHit(t *testing.T) {
	t.Parallel()
	primary := &scriptedProbe{readOK: true}
	cache := &scriptedCacheProbe{
		hit:    true,
		stored: Int64Token(0),
	}
	o := newCacheObserverTest(t, primary, cache)
	o.Start("ns-cache", ObserverConfig{NeedCache: true})
	t.Cleanup(func() { o.Stop("ns-cache") })

	waitForProbe(t, o, "ns-cache", func(r ProbeResult) bool {
		return r.CacheDbStale && r.CacheHit
	})
}

func TestObserver_cacheHitFresh(t *testing.T) {
	t.Parallel()
	primary := &scriptedProbe{readOK: true}
	cache := &scriptedCacheProbe{
		hit:    true,
		stored: Int64Token(100),
	}
	o := newCacheObserverTest(t, primary, cache)
	o.Start("ns-cache", ObserverConfig{NeedCache: true})
	t.Cleanup(func() { o.Stop("ns-cache") })

	waitForProbe(t, o, "ns-cache", func(r ProbeResult) bool {
		return r.WrittenVersion > 0 && r.CacheHit && !r.CacheDbStale
	})
}
