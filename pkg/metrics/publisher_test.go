package metrics

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestEnsurePublisher_SecondCallDoesNotStartSecondLoop(t *testing.T) {
	t.Parallel()

	var collectCount atomic.Int32
	c := &MetricsCollector{
		publisherCollectHook: func(ctx context.Context, namespace, template string, topo TopologyFlags) (*DeploymentMetrics, error) {
			collectCount.Add(1)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(50 * time.Millisecond):
			}
			return &DeploymentMetrics{
				Timestamp:  time.Now(),
				Namespace:  namespace,
				Components: map[string]ComponentMetrics{},
			}, nil
		},
	}

	c.EnsurePublisher("ns-a", "url-shortener", TopologyFlags{})
	c.EnsurePublisher("ns-a", "social-feed", TopologyFlags{HasDB: true})

	c.publishersMu.Lock()
	count := len(c.publishers)
	c.publishersMu.Unlock()
	if count != 1 {
		t.Fatalf("expected one publisher, got %d", count)
	}

	time.Sleep(150 * time.Millisecond)
	first := collectCount.Load()
	if first == 0 {
		t.Fatal("expected at least one collect")
	}

	time.Sleep(150 * time.Millisecond)
	second := collectCount.Load()
	if second-first > first+1 {
		t.Fatalf("expected a single collect loop, collect count jumped from %d to %d", first, second)
	}

	c.StopPublisher("ns-a")
}

func TestStopPublisher_CancelsInFlightCollect(t *testing.T) {
	t.Parallel()

	collectStarted := make(chan struct{})
	collectDone := make(chan struct{})

	c := &MetricsCollector{
		publisherCollectHook: func(ctx context.Context, namespace, template string, topo TopologyFlags) (*DeploymentMetrics, error) {
			select {
			case <-collectStarted:
			default:
				close(collectStarted)
			}
			<-ctx.Done()
			close(collectDone)
			return nil, ctx.Err()
		},
	}

	c.EnsurePublisher("ns-stop", "url-shortener", TopologyFlags{})

	select {
	case <-collectStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("collect did not start")
	}

	c.StopPublisher("ns-stop")

	select {
	case <-collectDone:
	case <-time.After(3 * time.Second):
		t.Fatal("in-flight collect was not cancelled")
	}
}

func TestLatest_PendingBeforeFirstCollect(t *testing.T) {
	t.Parallel()

	c := &MetricsCollector{
		publisherCollectHook: func(ctx context.Context, namespace, template string, topo TopologyFlags) (*DeploymentMetrics, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(2 * time.Second):
			}
			return &DeploymentMetrics{
				Timestamp:  time.Now(),
				Namespace:  namespace,
				Components: map[string]ComponentMetrics{},
			}, nil
		},
	}

	c.EnsurePublisher("ns-pending", "url-shortener", TopologyFlags{})
	dm := c.Latest("ns-pending")
	if dm == nil {
		t.Fatal("expected pending skeleton")
	}
	if !dm.CollectPending {
		t.Fatal("expected collectPending true")
	}
	if dm.SnapshotAgeMs != 0 {
		t.Fatalf("expected snapshotAgeMs 0, got %d", dm.SnapshotAgeMs)
	}
	if dm.Components == nil {
		t.Fatal("expected non-nil components map")
	}
	c.StopPublisher("ns-pending")
}

func TestLatest_AfterStoreCollectPendingFalseAndAgeGrows(t *testing.T) {
	t.Parallel()

	c := &MetricsCollector{}
	c.publisherCollectHook = func(ctx context.Context, namespace, template string, topo TopologyFlags) (*DeploymentMetrics, error) {
		return &DeploymentMetrics{
			Timestamp:  time.Now(),
			Namespace:  namespace,
			Components: map[string]ComponentMetrics{},
		}, nil
	}

	c.EnsurePublisher("ns-age", "url-shortener", TopologyFlags{})
	deadline := time.Now().Add(2 * time.Second)
	var first *DeploymentMetrics
	for time.Now().Before(deadline) {
		first = c.Latest("ns-age")
		if first != nil && !first.CollectPending {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if first == nil || first.CollectPending {
		t.Fatalf("expected stored snapshot, got %+v", first)
	}

	time.Sleep(50 * time.Millisecond)
	second := c.Latest("ns-age")
	if second == nil || second.CollectPending {
		t.Fatalf("expected stored snapshot after wait, got %+v", second)
	}
	if second.SnapshotAgeMs <= first.SnapshotAgeMs {
		t.Fatalf("expected snapshotAgeMs to grow, first=%d second=%d", first.SnapshotAgeMs, second.SnapshotAgeMs)
	}
	c.StopPublisher("ns-age")
}

func TestPublisher_SurvivesStreamCancel(t *testing.T) {
	t.Parallel()

	var collectCount atomic.Int32
	c := &MetricsCollector{
		publisherCollectHook: func(ctx context.Context, namespace, template string, topo TopologyFlags) (*DeploymentMetrics, error) {
			collectCount.Add(1)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(30 * time.Millisecond):
			}
			return &DeploymentMetrics{
				Timestamp:  time.Now(),
				Namespace:  namespace,
				Components: map[string]ComponentMetrics{},
			}, nil
		},
	}

	c.EnsurePublisher("ns-survive", "url-shortener", TopologyFlags{})

	waitCollect := func(min int32, timeout time.Duration) int32 {
		t.Helper()
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			n := collectCount.Load()
			if n >= min {
				return n
			}
			time.Sleep(20 * time.Millisecond)
		}
		return collectCount.Load()
	}

	if waitCollect(1, 2*time.Second) < 1 {
		t.Fatal("publisher did not collect")
	}

	_, wsCancel := context.WithCancel(context.Background())
	wsCancel()

	c.publishersMu.Lock()
	_, ok := c.publishers["ns-survive"]
	c.publishersMu.Unlock()
	if !ok {
		t.Fatal("publisher map entry gone after unrelated ctx cancel")
	}
	if dm := c.Latest("ns-survive"); dm == nil {
		t.Fatal("Latest returned nil after unrelated ctx cancel")
	}

	before := collectCount.Load()
	after := waitCollect(before+1, publisherCollectInterval+time.Second)
	if after <= before {
		t.Fatalf("publisher stopped after unrelated ctx cancel: before=%d after=%d", before, after)
	}

	c.StopPublisher("ns-survive")
}
