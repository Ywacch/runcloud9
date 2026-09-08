package probes

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
)

const defaultObserverInterval = 10 * time.Second

// ObserverConfig selects which probe legs run for a namespace session.
type ObserverConfig struct {
	NeedReplica bool
	NeedCache   bool
	Cache       CacheDialOptions
}

// ProbeResult is the latest primary write vs replica read and optional cache read snapshot.
type ProbeResult struct {
	WrittenVersion       int64
	ReplicaVersion       int64
	RYWStalenessVersions int64
	MonotonicViolation   bool
	CacheHit             bool
	CacheDbStale         bool
	CacheErr             string
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
	needReplica        bool
	needCache          bool
	cacheDisabled      bool
	cacheOpts          CacheDialOptions
	cacheDialErr       error
	primary            ConsistencyProbe
	replica            ConsistencyProbe
	cache              CacheProbe
}

// Observer runs a slow background loop per deployment namespace.
// Results are a lag/reachability canary for ConsistencyStatus (replicaStalenessVersions,
// LastProbeError). Student-facing ryw_violation / monotonic_violation events are sourced
// from app readrouter Prometheus counters, not from this loop.
type Observer struct {
	mu           sync.Mutex
	sessions     map[string]*observerSession
	dial         probeDialFunc
	dialCache    cacheDialFunc
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

// WithCacheDial overrides the default cache dialer (tests).
func WithCacheDial(fn cacheDialFunc) ObserverOption {
	return func(o *Observer) {
		if fn != nil {
			o.dialCache = fn
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
		dialCache:    defaultCacheDial,
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
func (o *Observer) Start(namespace string, cfg ObserverConfig) {
	if o == nil || namespace == "" {
		return
	}
	if !cfg.NeedReplica && !cfg.NeedCache {
		return
	}
	o.mu.Lock()
	if _, exists := o.sessions[namespace]; exists {
		o.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	sess := &observerSession{
		cancel:      cancel,
		done:        make(chan struct{}),
		needReplica: cfg.NeedReplica,
		needCache:   cfg.NeedCache,
		cacheOpts:   cfg.Cache,
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

	out := ProbeResult{WrittenVersion: written}

	if sess.needReplica && sess.replica != nil {
		replicaVer, ok, err := sess.replica.ReadVersion(ctx)
		if err != nil {
			o.probeCache(ctx, sess, written, &out)
			return out, err
		}
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
	}

	o.probeCache(ctx, sess, written, &out)
	return out, nil
}

func (o *Observer) probeCache(ctx context.Context, sess *observerSession, written int64, out *ProbeResult) {
	if !sess.needCache || sess.cacheDisabled {
		return
	}
	if sess.cache == nil {
		if sess.cacheDialErr != nil {
			out.CacheErr = sess.cacheDialErr.Error()
		}
		return
	}

	writtenTok := Int64Token(written)
	cached, hit, err := sess.cache.Get(ctx, CacheProbeKey)
	if err != nil {
		out.CacheErr = err.Error()
		return
	}
	if !hit {
		ttl := sess.cacheOpts.ProbeTTL
		if ttl <= 0 {
			ttl = defaultCacheProbeTTL
		}
		if err := sess.cache.Set(ctx, CacheProbeKey, writtenTok, ttl); err != nil {
			out.CacheErr = err.Error()
		}
		return
	}

	out.CacheHit = true
	if TokenBehind(writtenTok, cached) {
		out.CacheDbStale = true
	}
}

func (o *Observer) ensureProbes(ctx context.Context, namespace string, sess *observerSession) error {
	if sess.primary == nil {
		primary, err := o.dial(ctx, primaryHost(namespace))
		if err != nil {
			return fmt.Errorf("dial primary: %w", err)
		}
		sess.primary = primary
	}
	if sess.needReplica && sess.replica == nil {
		replica, err := o.dial(ctx, replicaHost(namespace))
		if err != nil {
			return fmt.Errorf("dial replica: %w", err)
		}
		sess.replica = replica
	}
	if sess.needCache && sess.cache == nil && !sess.cacheDisabled {
		cache, err := o.dialCache(ctx, namespace, sess.cacheOpts)
		if err != nil {
			if errors.Is(err, ErrCacheProbeUnsupported) {
				sess.cacheDisabled = true
			} else {
				sess.cacheDialErr = err
			}
		} else {
			sess.cache = cache
			sess.cacheDialErr = nil
		}
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
		if sess.cache != nil {
			_ = sess.cache.Close()
			sess.cache = nil
		}
	})
}
