package metrics

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

const publisherCollectInterval = 2 * time.Second

type sessionPublisher struct {
	cancel      context.CancelFunc
	done        chan struct{}
	mu          sync.Mutex
	template    string
	topo        TopologyFlags
	collectedAt time.Time
	latest      *DeploymentMetrics
}

// EnsurePublisher starts or refreshes the per-namespace Collect loop for a deployment session.
// The loop uses context.Background() and is not tied to any WebSocket lifetime.
func (c *MetricsCollector) EnsurePublisher(namespace, template string, topo TopologyFlags) {
	if c == nil || namespace == "" {
		return
	}
	c.publishersMu.Lock()
	if c.publishers == nil {
		c.publishers = make(map[string]*sessionPublisher)
	}
	pub := c.publishers[namespace]
	if pub != nil {
		pub.mu.Lock()
		pub.template = template
		pub.topo = topo
		pub.mu.Unlock()
		c.publishersMu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	pub = &sessionPublisher{
		cancel:   cancel,
		done:     make(chan struct{}),
		template: template,
		topo:     topo,
	}
	c.publishers[namespace] = pub
	c.publishersMu.Unlock()
	go pub.loop(ctx, c, namespace)
}

// Latest returns the most recent snapshot for a namespace, or a pending skeleton
// before the first Collect completes. Returns nil when no publisher exists.
func (c *MetricsCollector) Latest(namespace string) *DeploymentMetrics {
	if c == nil || namespace == "" {
		return nil
	}
	c.publishersMu.Lock()
	pub := c.publishers[namespace]
	c.publishersMu.Unlock()
	if pub == nil {
		return nil
	}
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if pub.latest == nil {
		return &DeploymentMetrics{
			Timestamp:      time.Now(),
			Namespace:      namespace,
			Components:     map[string]ComponentMetrics{},
			CollectPending: true,
			SnapshotAgeMs:  0,
		}
	}
	out := *pub.latest
	out.SnapshotAgeMs = time.Since(pub.collectedAt).Milliseconds()
	out.CollectPending = false
	return &out
}

// StopPublisher cancels the session Collect loop and waits for it to exit.
func (c *MetricsCollector) StopPublisher(namespace string) {
	if c == nil || namespace == "" {
		return
	}
	c.publishersMu.Lock()
	pub := c.publishers[namespace]
	if pub != nil {
		delete(c.publishers, namespace)
	}
	c.publishersMu.Unlock()
	if pub == nil {
		return
	}
	pub.cancel()
	select {
	case <-pub.done:
	case <-time.After(5 * time.Second):
		slog.Warn("metrics publisher stop timed out", "namespace", namespace)
	}
}

func (pub *sessionPublisher) loop(ctx context.Context, c *MetricsCollector, namespace string) {
	defer close(pub.done)
	for {
		pub.mu.Lock()
		template := pub.template
		topo := pub.topo
		pub.mu.Unlock()

		var dm *DeploymentMetrics
		var err error
		if hook := c.publisherCollectHook; hook != nil {
			dm, err = hook(ctx, namespace, template, topo)
		} else {
			dm, err = c.Collect(ctx, namespace, template, topo)
		}
		if dm != nil {
			pub.mu.Lock()
			pub.latest = dm
			pub.collectedAt = time.Now()
			pub.mu.Unlock()
		}
		if err != nil {
			slog.Warn("metrics publisher collect failed", "namespace", namespace, "error", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(publisherCollectInterval):
		}
	}
}
