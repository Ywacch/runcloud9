package metrics

import (
	"fmt"
	"sync"
	"time"

	"github.com/ywacch/cloud9/pkg/probes"
	"github.com/ywacch/cloud9/pkg/rules"
	"github.com/ywacch/cloud9/pkg/templates"
)

const (
	ruleRYWViolation       = rules.IDRYWViolation
	ruleMonotonicViolation = rules.IDMonotonicViolation
	ruleCacheDbStale       = rules.IDCacheDBStale

	guaranteeHeldByNone = "none"
	guaranteeHeldByApp  = "app"
	guaranteeHeldByDB   = "db"
)

// cacheStaleSample is the newest stale-read evidence row scraped from app-server.
type cacheStaleSample struct {
	Key, Served, Truth, RequestID string
}

// appTrafficConsistency is scraped readrouter Prometheus state for one Collect tick.
type appTrafficConsistency struct {
	RYWTotal          float64
	MonoTotal         float64
	LeaderReads       int64
	ReplicaReads      int64
	ReadRateByReplica map[string]float64
	LeaderRedirects   float64
	ReplicaStaleTotal float64
}

// ConsistencyDetector turns observer probe snapshots into L1 invariant events and db consistency counters.
type ConsistencyDetector struct {
	mu         sync.Mutex
	reader     consistencyProbeReader
	suppressed map[string]map[string]bool
	violations map[string]map[string]int64

	appStaleTotal map[string]float64
	appStaleSeen  map[string]float64
	lastSample    map[string]cacheStaleSample

	appTraffic  map[string]appTrafficConsistency
	appRYWSeen  map[string]float64
	appMonoSeen map[string]float64

	postgresSyncActive map[string]bool

	activePartition    map[string]PartitionContext
	partitionRYWSeen   map[string]float64
	partitionStaleSeen map[string]float64

	replicaReady              map[string]map[string]bool
	replicaConnectedOnPrimary map[string]map[string]bool
}

// NewConsistencyDetector creates a detector backed by the given probe reader.
func NewConsistencyDetector(reader consistencyProbeReader) *ConsistencyDetector {
	return &ConsistencyDetector{
		reader:                    reader,
		suppressed:                make(map[string]map[string]bool),
		violations:                make(map[string]map[string]int64),
		appStaleTotal:             make(map[string]float64),
		appStaleSeen:              make(map[string]float64),
		lastSample:                make(map[string]cacheStaleSample),
		appTraffic:                make(map[string]appTrafficConsistency),
		appRYWSeen:                make(map[string]float64),
		appMonoSeen:               make(map[string]float64),
		postgresSyncActive:        make(map[string]bool),
		activePartition:           make(map[string]PartitionContext),
		partitionRYWSeen:          make(map[string]float64),
		partitionStaleSeen:        make(map[string]float64),
		replicaReady:              make(map[string]map[string]bool),
		replicaConnectedOnPrimary: make(map[string]map[string]bool),
	}
}

// NoteAppCacheStale records the scraped app-server stale-read counter and optional evidence sample.
// Call once per Collect tick before Eval.
func (cd *ConsistencyDetector) NoteAppCacheStale(namespace string, total float64, sample cacheStaleSample) {
	if cd == nil || namespace == "" {
		return
	}
	cd.mu.Lock()
	defer cd.mu.Unlock()
	cd.appStaleTotal[namespace] = total
	if sample.Key != "" {
		cd.lastSample[namespace] = sample
	}
}

// NotePostgresSyncActive records whether live pg_settings match the design synchronous replication GUCs.
// Call once per Collect tick before Eval when the topology expects semi-sync or sync.
func (cd *ConsistencyDetector) NotePostgresSyncActive(namespace string, active bool) {
	if cd == nil || namespace == "" {
		return
	}
	cd.mu.Lock()
	defer cd.mu.Unlock()
	cd.postgresSyncActive[namespace] = active
}

// NoteAppTrafficConsistency records scraped readrouter counters for RYW/monotonic and read offload.
// Call once per Collect tick before Eval. Student RYW/monotonic events use these totals, not the probe.
func (cd *ConsistencyDetector) NoteAppTrafficConsistency(namespace string, traffic appTrafficConsistency) {
	if cd == nil || namespace == "" {
		return
	}
	cd.mu.Lock()
	defer cd.mu.Unlock()
	cd.appTraffic[namespace] = traffic
}

// Eval updates db.Consistency and emits invariant events on transitions.
// Student-facing RYW/monotonic come from app Prometheus counters (Phase 2.5 PR9).
// Student-facing cache_db_stale comes from app counters (E4). The _c9_probe observer
// remains a lag/reachability canary (replicaStalenessVersions, LastProbeError only).
func (cd *ConsistencyDetector) Eval(namespace string, dm *DeploymentMetrics, topo TopologyFlags, _ *templates.TemplateSpec) []Event {
	if cd == nil || dm == nil || namespace == "" || !topo.HasDB {
		return nil
	}

	var out []Event
	now := time.Now()

	if topo.HasCache {
		out = append(out, cd.evalAppCacheStale(namespace, now)...)
	}
	out = append(out, cd.evalAppRYW(namespace, now)...)
	out = append(out, cd.evalAppMonotonic(namespace, now)...)
	out = append(out, cd.evalPartitionCAP(namespace, now, dm, topo, nil)...)
	out = append(out, cd.evalReplicaCannotCatchUp(namespace, now, topo)...)

	var result probes.ProbeResult
	probeOK := false
	if cd.reader != nil {
		result, probeOK = cd.reader.Latest(namespace)
	}

	if cm, exists := dm.Components["db"]; exists {
		cm.Consistency = cd.statusFor(namespace, result, probeOK, topo)
		dm.Components["db"] = cm
	}

	return out
}

// Cleanup removes latch and cumulative counter state for a namespace (deployment teardown).
func (cd *ConsistencyDetector) Cleanup(namespace string) {
	if cd == nil || namespace == "" {
		return
	}
	cd.mu.Lock()
	defer cd.mu.Unlock()
	delete(cd.suppressed, namespace)
	delete(cd.violations, namespace)
	delete(cd.appStaleTotal, namespace)
	delete(cd.appStaleSeen, namespace)
	delete(cd.lastSample, namespace)
	delete(cd.appTraffic, namespace)
	delete(cd.appRYWSeen, namespace)
	delete(cd.appMonoSeen, namespace)
	delete(cd.postgresSyncActive, namespace)
	delete(cd.activePartition, namespace)
	delete(cd.partitionRYWSeen, namespace)
	delete(cd.partitionStaleSeen, namespace)
	delete(cd.replicaReady, namespace)
	delete(cd.replicaConnectedOnPrimary, namespace)
}

func (cd *ConsistencyDetector) evalAppCacheStale(namespace string, now time.Time) []Event {
	cd.mu.Lock()
	total := cd.appStaleTotal[namespace]
	lastSeen := cd.appStaleSeen[namespace]
	sample := cd.lastSample[namespace]
	cd.appStaleSeen[namespace] = total
	cd.mu.Unlock()

	if total > lastSeen {
		if cd.fire(namespace, ruleCacheDbStale) {
			cd.incrementViolation(namespace, ruleCacheDbStale)
			return []Event{cd.cacheDbStaleEvent(now, sample)}
		}
		return nil
	}
	if total == lastSeen {
		cd.clear(namespace, ruleCacheDbStale)
	}
	return nil
}

func (cd *ConsistencyDetector) evalAppRYW(namespace string, now time.Time) []Event {
	cd.mu.Lock()
	total := cd.appTraffic[namespace].RYWTotal
	lastSeen := cd.appRYWSeen[namespace]
	cd.appRYWSeen[namespace] = total
	cd.mu.Unlock()

	if total > lastSeen {
		if cd.fire(namespace, ruleRYWViolation) {
			cd.incrementViolation(namespace, ruleRYWViolation)
			return []Event{cd.rywTrafficEvent(now, total-lastSeen)}
		}
		return nil
	}
	if total == lastSeen {
		cd.clear(namespace, ruleRYWViolation)
	}
	return nil
}

func (cd *ConsistencyDetector) evalAppMonotonic(namespace string, now time.Time) []Event {
	cd.mu.Lock()
	total := cd.appTraffic[namespace].MonoTotal
	lastSeen := cd.appMonoSeen[namespace]
	cd.appMonoSeen[namespace] = total
	cd.mu.Unlock()

	if total > lastSeen {
		if cd.fire(namespace, ruleMonotonicViolation) {
			cd.incrementViolation(namespace, ruleMonotonicViolation)
			return []Event{cd.monotonicTrafficEvent(now, total-lastSeen)}
		}
		return nil
	}
	if total == lastSeen {
		cd.clear(namespace, ruleMonotonicViolation)
	}
	return nil
}

func (cd *ConsistencyDetector) statusFor(namespace string, result probes.ProbeResult, ok bool, topo TopologyFlags) *ConsistencyStatus {
	cd.mu.Lock()
	traffic := cd.appTraffic[namespace]
	postgresSync := cd.postgresSyncActive[namespace]
	cd.mu.Unlock()

	status := &ConsistencyStatus{
		RYWViolations:        cd.violationCount(namespace, ruleRYWViolation),
		MonotonicViolations:  cd.violationCount(namespace, ruleMonotonicViolation),
		CacheDbStale:         cd.violationCount(namespace, ruleCacheDbStale),
		PartitionStaleReads:  cd.violationCount(namespace, rulePartitionStaleReads),
		PartitionWriteStall:  cd.violationCount(namespace, rulePartitionWriteStall),
		ReplicaCannotCatchUp: cd.violationCount(namespace, ruleReplicaCannotCatchUp),
		LeaderReads:          traffic.LeaderReads,
		ReplicaReads:         traffic.ReplicaReads,
		ReadRateByReplica:    cloneFloatMap(traffic.ReadRateByReplica),
		ReplicaStaleReads:    int64(traffic.ReplicaStaleTotal + 0.5),
		GuaranteeHeldBy:      guaranteeHeldByFrom(traffic, topo, postgresSync),
	}
	if sample, has := cd.sampleFor(namespace); has {
		status.LastCacheKey = sample.Key
		status.LastCacheServed = sample.Served
		status.LastCacheTruth = sample.Truth
		status.LastCacheRequestID = sample.RequestID
	}
	if !ok {
		return status
	}
	// Probe stays a lag/reachability canary — do not source student RYW/monotonic from it.
	status.ReplicaStalenessVersions = result.RYWStalenessVersions
	status.LastProbeError = result.LastErr
	return status
}

func guaranteeHeldByFrom(traffic appTrafficConsistency, topo TopologyFlags, postgresSyncActive bool) string {
	if topo.HasDB && topo.DBReadReplicas > 0 && topo.SyncCommitLevel == "remote_apply" {
		switch topo.ReplicationMode {
		case "semi-sync", "sync":
			if postgresSyncActive {
				return guaranteeHeldByDB
			}
			return guaranteeHeldByNone
		}
	}
	if traffic.LeaderRedirects > 0 {
		return guaranteeHeldByApp
	}
	return guaranteeHeldByNone
}

func (cd *ConsistencyDetector) sampleFor(namespace string) (cacheStaleSample, bool) {
	cd.mu.Lock()
	defer cd.mu.Unlock()
	sample, ok := cd.lastSample[namespace]
	return sample, ok && sample.Key != ""
}

func (cd *ConsistencyDetector) rywTrafficEvent(now time.Time, delta float64) Event {
	desc := "A replica read observed a write that has not replayed yet"
	if delta >= 1 {
		desc = fmt.Sprintf("App observed %.0f read-your-writes violation(s) on replica traffic", delta)
	}
	return Event{
		Timestamp:   now,
		Type:        "invariant",
		Source:      "detector",
		Component:   "db",
		RuleID:      ruleRYWViolation,
		Visibility:  VisibilityStudent,
		Title:       "Read-your-writes violation",
		Description: desc,
		Detail: map[string]string{
			"kind":   "ryw",
			"source": "traffic",
			"delta":  fmt.Sprintf("%.0f", delta),
		},
	}
}

func (cd *ConsistencyDetector) monotonicTrafficEvent(now time.Time, delta float64) Event {
	desc := "A later replica read observed an earlier replication position"
	if delta >= 1 {
		desc = fmt.Sprintf("App observed %.0f monotonic-read violation(s) on replica traffic", delta)
	}
	return Event{
		Timestamp:   now,
		Type:        "invariant",
		Source:      "detector",
		Component:   "db",
		RuleID:      ruleMonotonicViolation,
		Visibility:  VisibilityStudent,
		Title:       "Monotonic read violation",
		Description: desc,
		Detail: map[string]string{
			"kind":   "monotonic",
			"source": "traffic",
			"delta":  fmt.Sprintf("%.0f", delta),
		},
	}
}

func (cd *ConsistencyDetector) cacheDbStaleEvent(now time.Time, sample cacheStaleSample) Event {
	desc := "Cache served a value different from the database"
	if sample.Key != "" && sample.Served != "" && sample.Truth != "" {
		desc = fmt.Sprintf("Cache served %s for %s; database has %s", sample.Served, sample.Key, sample.Truth)
	}
	detail := map[string]string{"kind": "cache_db_stale"}
	if sample.Key != "" {
		detail["key"] = sample.Key
	}
	if sample.Served != "" {
		detail["served"] = sample.Served
	}
	if sample.Truth != "" {
		detail["truth"] = sample.Truth
	}
	if sample.RequestID != "" {
		detail["requestId"] = sample.RequestID
	}
	return Event{
		Timestamp:   now,
		Type:        "invariant",
		Source:      "detector",
		Component:   "db",
		RuleID:      ruleCacheDbStale,
		Visibility:  VisibilityStudent,
		Title:       "Cache vs database staleness",
		Description: desc,
		Detail:      detail,
	}
}

func (cd *ConsistencyDetector) fire(namespace, rule string) bool {
	cd.mu.Lock()
	defer cd.mu.Unlock()
	m := cd.suppressed[namespace]
	if m == nil {
		m = make(map[string]bool)
		cd.suppressed[namespace] = m
	}
	if m[rule] {
		return false
	}
	m[rule] = true
	return true
}

func (cd *ConsistencyDetector) clear(namespace, rule string) {
	cd.mu.Lock()
	defer cd.mu.Unlock()
	m := cd.suppressed[namespace]
	if m == nil {
		return
	}
	delete(m, rule)
	if len(m) == 0 {
		delete(cd.suppressed, namespace)
	}
}

func (cd *ConsistencyDetector) incrementViolation(namespace, rule string) {
	cd.mu.Lock()
	defer cd.mu.Unlock()
	m := cd.violations[namespace]
	if m == nil {
		m = make(map[string]int64)
		cd.violations[namespace] = m
	}
	m[rule]++
}

func (cd *ConsistencyDetector) violationCount(namespace, rule string) int64 {
	cd.mu.Lock()
	defer cd.mu.Unlock()
	m := cd.violations[namespace]
	if m == nil {
		return 0
	}
	return m[rule]
}
