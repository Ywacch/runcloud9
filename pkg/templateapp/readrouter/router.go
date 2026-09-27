// Package readrouter routes SQL reads between postgres primary and replicas with
// optional read-your-writes and monotonic consistency policies.
package readrouter

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ywacch/cloud9/pkg/templateapp/consistency"
)

const (
	defaultMaxClients  = 10_000
	defaultClientTTL   = time.Hour
	defaultWriteTarget = "postgres"
)

// ConsistencyToken compares replication positions for routing and violation detection.
type ConsistencyToken interface {
	GTE(other ConsistencyToken) bool
	String() string
}

type lsnToken uint64

func (t lsnToken) GTE(other ConsistencyToken) bool {
	o, ok := other.(lsnToken)
	if !ok {
		return false
	}
	return uint64(t) >= uint64(o)
}

func (t lsnToken) String() string {
	high := uint64(t) >> 32
	low := uint64(t) & 0xffffffff
	return fmt.Sprintf("%X/%X", high, low)
}

// parseLSN parses a postgres LSN string "X/Y" (hex) into a comparable token.
func parseLSN(raw string) (uint64, error) {
	s := strings.TrimSpace(raw)
	parts := strings.Split(s, "/")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid lsn %q", raw)
	}
	high, err := strconv.ParseUint(parts[0], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("parse lsn high %q: %w", parts[0], err)
	}
	low, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("parse lsn low %q: %w", parts[1], err)
	}
	return (high << 32) | low, nil
}

// RoutingPolicy selects how reads are routed between leader and replica pools.
type RoutingPolicy string

const (
	PolicyLeaderOnly   RoutingPolicy = "leader-only"
	PolicyReplica      RoutingPolicy = "replica"
	PolicyRYW          RoutingPolicy = "ryw"
	PolicyMonotonic    RoutingPolicy = "monotonic"
	PolicyRYWMonotonic RoutingPolicy = "ryw+monotonic"
	PolicyBoundedStale RoutingPolicy = "bounded-staleness" // reserved, unimplemented
	PolicyQuorum       RoutingPolicy = "quorum"            // reserved, unimplemented
)

// Route is the pool chosen for a read.
type Route int

const (
	RouteLeader Route = iota
	RouteReplica
)

func (r Route) String() string {
	switch r {
	case RouteReplica:
		return "replica"
	default:
		return "leader"
	}
}

// Engine maps a storage backend to token expressions and parsing.
type Engine interface {
	WriteTokenExpr() string
	ReadTokenExpr() string
	ParseToken(raw string) (ConsistencyToken, error)
}

// EnforcementPoint decides read routes and records write/read tokens per client.
type EnforcementPoint interface {
	RouteRead(clientID string, pol RoutingPolicy) Route
	AfterWrite(clientID string, t ConsistencyToken)
	RecordRead(clientID string, t ConsistencyToken) (rywViolation, monotonicViolation bool)
}

type clientState struct {
	lastWrite ConsistencyToken
	lastRead  ConsistencyToken
	seenAt    time.Time
}

// Querier executes SQL reads against a routed pool.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type queryPool = Querier

// ReplicaPool binds one replica pgx pool to its stable DNS hostname for Observe targets.
type ReplicaPool struct {
	Pool *pgxpool.Pool
	Host string
}

type replicaEntry struct {
	pool queryPool
	host string
}

// ReadRouter tracks per-client tokens, applies a routing policy, and optionally
// executes reads against leader/replica pools.
type ReadRouter struct {
	mu          sync.Mutex
	engine      Engine
	policy      RoutingPolicy
	clients     map[string]*clientState
	maxSize     int
	clientTTL   time.Duration
	writePool   queryPool
	writeTarget string
	readPools   []replicaEntry
	rrIndex     atomic.Uint64

	sampleMu         sync.RWMutex
	violationSamples []consistency.ReplicaSample
}

// NewReadRouter builds a router. Pools may be nil when only testing enforcement.
func NewReadRouter(writePool *pgxpool.Pool, replicas []ReplicaPool, eng Engine, pol RoutingPolicy) *ReadRouter {
	if eng == nil {
		eng = PostgresEngine{}
	}
	entries := make([]replicaEntry, 0, len(replicas))
	for _, rp := range replicas {
		if rp.Pool == nil || strings.TrimSpace(rp.Host) == "" {
			continue
		}
		entries = append(entries, replicaEntry{pool: rp.Pool, host: rp.Host})
	}
	var write queryPool
	if writePool != nil {
		write = writePool
	}
	return &ReadRouter{
		engine:      eng,
		policy:      pol,
		clients:     make(map[string]*clientState),
		maxSize:     defaultMaxClients,
		clientTTL:   defaultClientTTL,
		writePool:   write,
		writeTarget: defaultWriteTarget,
		readPools:   entries,
	}
}

// Policy returns the default routing policy for this router.
func (r *ReadRouter) Policy() RoutingPolicy {
	return r.policy
}

// RouteRead picks leader or replica for clientID under pol.
func (r *ReadRouter) RouteRead(clientID string, pol RoutingPolicy) Route {
	if pol == "" {
		pol = r.policy
	}
	switch pol {
	case PolicyLeaderOnly, PolicyBoundedStale, PolicyQuorum:
		return RouteLeader
	case PolicyReplica, PolicyMonotonic:
		return RouteReplica
	case PolicyRYW, PolicyRYWMonotonic:
		if r.needsLeaderForRYW(clientID) {
			rywLeaderRedirectsTotal.Inc()
			return RouteLeader
		}
		return RouteReplica
	default:
		return RouteLeader
	}
}

// snapshotLastWrite returns the client's lastWrite at call time for QueryRead
// and QueryRows routing baselines. Writes recorded after the snapshot are
// concurrent with the in-flight read, not causally prior to it.
func (r *ReadRouter) snapshotLastWrite(clientID string) ConsistencyToken {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.clients[clientID]
	if st == nil || st.lastWrite == nil {
		return nil
	}
	return st.lastWrite
}

// tokenBehindBaseline reports whether token trails the frozen write baseline.
func tokenBehindBaseline(token, writeBaseline ConsistencyToken) bool {
	if token == nil || writeBaseline == nil {
		return false
	}
	return !token.GTE(writeBaseline)
}

func (r *ReadRouter) needsLeaderForRYW(clientID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.clients[clientID]
	if st == nil || st.lastWrite == nil {
		return false
	}
	if st.lastRead == nil {
		return true
	}
	return !st.lastRead.GTE(st.lastWrite)
}

// AfterWrite records the write token observed after a successful write.
func (r *ReadRouter) AfterWrite(clientID string, t ConsistencyToken) {
	if clientID == "" || t == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.getOrCreateLocked(clientID)
	st.lastWrite = t
	st.seenAt = time.Now()
}

// RecordRead updates read token state and returns violation flags for the policy.
func (r *ReadRouter) RecordRead(clientID string, t ConsistencyToken) (rywViolation, monotonicViolation bool) {
	if clientID == "" || t == nil {
		return false, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.getOrCreateLocked(clientID)
	defer func() {
		st.lastRead = t
		st.seenAt = time.Now()
	}()

	switch r.policy {
	case PolicyRYW, PolicyRYWMonotonic:
		if st.lastWrite != nil && !t.GTE(st.lastWrite) {
			rywViolation = true
			rywViolationsTotal.Inc()
			r.recordViolationSample(context.Background(), "ryw", clientID, "", "", st.lastWrite, t)
		}
	case PolicyReplica:
		if st.lastWrite != nil && !t.GTE(st.lastWrite) {
			replicaStaleReadsTotal.Inc()
			r.recordViolationSample(context.Background(), "replica_stale", clientID, "", "", st.lastWrite, t)
		}
	}

	switch r.policy {
	case PolicyMonotonic, PolicyRYWMonotonic:
		if st.lastRead != nil && !t.GTE(st.lastRead) {
			monotonicViolation = true
			monotonicViolationsTotal.Inc()
			r.recordViolationSample(context.Background(), "monotonic", clientID, "", "", st.lastRead, t)
		}
	}
	return rywViolation, monotonicViolation
}

// recordReadAgainst updates read token state using a frozen write baseline from
// the start of QueryRows. RYW and replica-stale checks compare against
// writeBaseline, not live lastWrite, so concurrent writes during run() do not
// count as violations.
func (r *ReadRouter) recordReadAgainst(
	ctx context.Context,
	clientID, replicaHost, tableOrKey string,
	t, writeBaseline ConsistencyToken,
) (rywViolation, monotonicViolation bool) {
	if clientID == "" || t == nil {
		return false, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.getOrCreateLocked(clientID)
	defer func() {
		st.lastRead = t
		st.seenAt = time.Now()
	}()

	switch r.policy {
	case PolicyRYW, PolicyRYWMonotonic:
		if tokenBehindBaseline(t, writeBaseline) {
			rywViolation = true
			rywViolationsTotal.Inc()
			r.recordViolationSample(ctx, "ryw", clientID, replicaHost, tableOrKey, writeBaseline, t)
		}
	case PolicyReplica:
		if tokenBehindBaseline(t, writeBaseline) {
			replicaStaleReadsTotal.Inc()
			r.recordViolationSample(ctx, "replica_stale", clientID, replicaHost, tableOrKey, writeBaseline, t)
		}
	}

	switch r.policy {
	case PolicyMonotonic, PolicyRYWMonotonic:
		if st.lastRead != nil && !t.GTE(st.lastRead) {
			monotonicViolation = true
			monotonicViolationsTotal.Inc()
			r.recordViolationSample(ctx, "monotonic", clientID, replicaHost, tableOrKey, st.lastRead, t)
		}
	}
	return rywViolation, monotonicViolation
}

func (r *ReadRouter) getOrCreateLocked(clientID string) *clientState {
	r.pruneLocked()
	st, ok := r.clients[clientID]
	if ok {
		return st
	}
	st = &clientState{}
	r.clients[clientID] = st
	return st
}

func (r *ReadRouter) pruneLocked() {
	now := time.Now()
	for id, st := range r.clients {
		if now.Sub(st.seenAt) > r.clientTTL {
			delete(r.clients, id)
		}
	}
	if len(r.clients) <= r.maxSize {
		return
	}
	for id := range r.clients {
		delete(r.clients, id)
		if len(r.clients) <= r.maxSize {
			break
		}
	}
}

func (r *ReadRouter) leaderTarget() string {
	if strings.TrimSpace(r.writeTarget) == "" {
		return defaultWriteTarget
	}
	return r.writeTarget
}

func (r *ReadRouter) nextReplicaStart() int {
	n := len(r.readPools)
	if n == 0 {
		return 0
	}
	return int(r.rrIndex.Add(1)-1) % n
}

// ReadTarget picks leader or a replica pool for multi-row reads that do not use QueryRead.
func (r *ReadRouter) ReadTarget(clientID string) (queryPool, string) {
	if r == nil || r.writePool == nil {
		return nil, defaultWriteTarget
	}
	route := r.RouteRead(clientID, r.policy)
	if route == RouteReplica && len(r.readPools) > 0 {
		idx := r.nextReplicaStart()
		entry := r.readPools[idx]
		incDBRead(entry.host)
		return entry.pool, entry.host
	}
	incDBRead(DBReadLeaderLabel)
	return r.writePool, r.leaderTarget()
}
