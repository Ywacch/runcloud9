package probes

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
)

const defaultObserverInterval = 10 * time.Second

// ProbeResult is the latest primary write vs replica read snapshot for one namespace.
type ProbeResult struct {
	WrittenVersion       int64
	ReplicaVersion       int64
	RYWStalenessVersions int64
	MonotonicViolation   bool
	LastErr              string
	At                   time.Time
}

type probeDialFunc func(ctx context.Context, host string) (ConsistencyProbe, error)

type observerSession struct {
	cancel             context.CancelFunc
	done               chan struct{}
	closeOnce          sync.Once
	result             ProbeResult
	hasResult          bool
	lastReplicaVersion int64
	schemaEnsured      bool
	primary            ConsistencyProbe
	replica            ConsistencyProbe
}

// Observer runs a slow background loop per deployment namespace when read replicas exist.
type Observer struct {
	mu           sync.Mutex
	sessions     map[string]*observerSession
	dial         probeDialFunc
	tickInterval time.Duration
}

// ObserverOption configures a new Observer.
type ObserverOption func(*Observer)

// WithProbeDial overrides the default Postgres dialer (tests).
func WithProbeDial(fn probeDialFunc) ObserverOption {
	return func(o *Observer) {
		if fn != nil {
			o.dial = fn
		}
	}
}

// WithTickInterval overrides the default probe cadence (tests).
func WithTickInterval(d time.Duration) ObserverOption {
	return func(o *Observer) {
		if d > 0 {
			o.tickInterval = d
		}
	}
}

// NewObserver constructs an observer with optional test hooks.
func NewObserver(opts ...ObserverOption) *Observer {
	o := &Observer{
		sessions:     make(map[string]*observerSession),
		dial:         defaultProbeDial,
		tickInterval: defaultObserverInterval,
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

func defaultProbeDial(ctx context.Context, host string) (ConsistencyProbe, error) {
	return NewPostgresProbe(ctx, host)
}

func primaryHost(namespace string) string {
	return fmt.Sprintf("db.%s.svc.cluster.local", namespace)
}

func replicaHost(namespace string) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local", deployer.DBReplicaServiceName, namespace)
}

// Start launches the observer loop for namespace. Idempotent.
func (o *Observer) Start(namespace string) {
	if o == nil || namespace == "" {
		return
	}
	o.mu.Lock()
	if _, exists := o.sessions[namespace]; exists {
		o.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	sess := &observerSession{
		cancel: cancel,
		done:   make(chan struct{}),
	}
	o.sessions[namespace] = sess
	o.mu.Unlock()

	go o.runLoop(ctx, namespace, sess)
}

// Stop cancels the observer loop and closes probe connections for namespace.
func (o *Observer) Stop(namespace string) {
	if o == nil || namespace == "" {
		return
	}
	o.mu.Lock()
	sess, ok := o.sessions[namespace]
	if !ok {
		o.mu.Unlock()
		return
	}
	delete(o.sessions, namespace)
	cancel := sess.cancel
	o.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	<-sess.done
}

// Latest returns the most recent probe result for namespace.
func (o *Observer) Latest(namespace string) (ProbeResult, bool) {
	if o == nil || namespace == "" {
		return ProbeResult{}, false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	sess, ok := o.sessions[namespace]
	if !ok || !sess.hasResult {
		return ProbeResult{}, false
	}
	return sess.result, true
}

func (o *Observer) runLoop(ctx context.Context, namespace string, sess *observerSession) {
	defer close(sess.done)
	defer closeSessionProbes(sess)

	ticker := time.NewTicker(o.tickInterval)
	defer ticker.Stop()

	o.probeOnce(ctx, namespace, sess)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.probeOnce(ctx, namespace, sess)
		}
	}
}

func (o *Observer) probeOnce(parent context.Context, namespace string, sess *observerSession) {
	timeout := o.tickInterval - time.Millisecond
	if timeout < time.Second {
		timeout = time.Second
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	result, err := o.runProbeTick(ctx, namespace, sess)
	result.At = time.Now()
	if err != nil {
		result.LastErr = err.Error()
	}
	o.storeResult(sess, result)
}

func (o *Observer) runProbeTick(ctx context.Context, namespace string, sess *observerSession) (ProbeResult, error) {
	if err := o.ensureProbes(ctx, namespace, sess); err != nil {
		return ProbeResult{}, err
	}
	if !sess.schemaEnsured {
		if err := sess.primary.EnsureSchema(ctx); err != nil {
			return ProbeResult{}, err
		}
		sess.schemaEnsured = true
	}

	written, err := sess.primary.WriteVersion(ctx)
	if err != nil {
		return ProbeResult{}, err
	}

	replicaVer, ok, err := sess.replica.ReadVersion(ctx)
	if err != nil {
		return ProbeResult{WrittenVersion: written}, err
	}

	out := ProbeResult{WrittenVersion: written}
	if ok {
		out.ReplicaVersion = replicaVer
		out.RYWStalenessVersions = StalenessVersions(written, replicaVer)
		if sess.lastReplicaVersion > 0 && MonotonicViolation(sess.lastReplicaVersion, replicaVer) {
			out.MonotonicViolation = true
		}
		sess.lastReplicaVersion = replicaVer
	} else {
		out.RYWStalenessVersions = StalenessVersions(written, 0)
	}
	return out, nil
}

func (o *Observer) ensureProbes(ctx context.Context, namespace string, sess *observerSession) error {
	if sess.primary == nil {
		primary, err := o.dial(ctx, primaryHost(namespace))
		if err != nil {
			return fmt.Errorf("dial primary: %w", err)
		}
		sess.primary = primary
	}
	if sess.replica == nil {
		replica, err := o.dial(ctx, replicaHost(namespace))
		if err != nil {
			return fmt.Errorf("dial replica: %w", err)
		}
		sess.replica = replica
	}
	return nil
}

func (o *Observer) storeResult(sess *observerSession, result ProbeResult) {
	o.mu.Lock()
	defer o.mu.Unlock()
	sess.result = result
	sess.hasResult = true
}

func closeSessionProbes(sess *observerSession) {
	if sess == nil {
		return
	}
	sess.closeOnce.Do(func() {
		if sess.primary != nil {
			_ = sess.primary.Close()
			sess.primary = nil
		}
		if sess.replica != nil {
			_ = sess.replica.Close()
			sess.replica = nil
		}
	})
}
