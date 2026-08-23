package metrics

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/ywacch/cloud9/pkg/probes"
	"github.com/ywacch/cloud9/pkg/templates"
)

const (
	ruleRYWViolation       = "ryw_violation"
	ruleMonotonicViolation = "monotonic_violation"
	ruleCacheDbStale       = "cache_db_stale"
)

// cacheStaleSample is the newest stale-read evidence row scraped from app-server.
type cacheStaleSample struct {
	Key, Served, Truth, RequestID string
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
}

// NewConsistencyDetector creates a detector backed by the given probe reader.
func NewConsistencyDetector(reader consistencyProbeReader) *ConsistencyDetector {
	return &ConsistencyDetector{
		reader:        reader,
		suppressed:    make(map[string]map[string]bool),
		violations:    make(map[string]map[string]int64),
		appStaleTotal: make(map[string]float64),
		appStaleSeen:  make(map[string]float64),
		lastSample:    make(map[string]cacheStaleSample),
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

// Eval reads the latest probe result, updates db.Consistency, and emits invariant events on transitions.
// RYW and monotonic violations are observer-sourced until Phase 2.5 PR9. Student-facing cache_db_stale
// is sourced from app Prometheus counters (not the _c9_probe canary).
func (cd *ConsistencyDetector) Eval(namespace string, dm *DeploymentMetrics, topo TopologyFlags, _ *templates.TemplateSpec) []Event {
	if cd == nil || dm == nil || namespace == "" || !topo.HasDB {
		return nil
	}

	var out []Event
	now := time.Now()

	if topo.HasCache {
		out = append(out, cd.evalAppCacheStale(namespace, now)...)
	}

	var result probes.ProbeResult
	probeOK := false
	if cd.reader != nil {
		result, probeOK = cd.reader.Latest(namespace)
	}

	if probeOK {
		if result.RYWStalenessVersions > 0 {
			if cd.fire(namespace, ruleRYWViolation) {
				cd.incrementViolation(namespace, ruleRYWViolation)
				out = append(out, cd.rywEvent(now, result))
			}
		} else {
			cd.clear(namespace, ruleRYWViolation)
		}

		if result.MonotonicViolation {
			if cd.fire(namespace, ruleMonotonicViolation) {
				cd.incrementViolation(namespace, ruleMonotonicViolation)
				out = append(out, cd.monotonicEvent(now, result))
			}
		} else {
			cd.clear(namespace, ruleMonotonicViolation)
		}
	}

	if cm, exists := dm.Components["db"]; exists {
		cm.Consistency = cd.statusFor(namespace, result, probeOK)
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

func (cd *ConsistencyDetector) statusFor(namespace string, result probes.ProbeResult, ok bool) *ConsistencyStatus {
	status := &ConsistencyStatus{
		RYWViolations:       cd.violationCount(namespace, ruleRYWViolation),
		MonotonicViolations: cd.violationCount(namespace, ruleMonotonicViolation),
		CacheDbStale:        cd.violationCount(namespace, ruleCacheDbStale),
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
	status.ReplicaStalenessVersions = result.RYWStalenessVersions
	status.LastProbeError = result.LastErr
	return status
}

func (cd *ConsistencyDetector) sampleFor(namespace string) (cacheStaleSample, bool) {
	cd.mu.Lock()
	defer cd.mu.Unlock()
	sample, ok := cd.lastSample[namespace]
	return sample, ok && sample.Key != ""
}

func (cd *ConsistencyDetector) rywEvent(now time.Time, result probes.ProbeResult) Event {
	return Event{
		Timestamp:   now,
		Type:        "invariant",
		Source:      "detector",
		Component:   "db",
		RuleID:      ruleRYWViolation,
		Audience:    "student",
		Title:       "Read-your-writes violation",
		Description: fmt.Sprintf("Replica is %d logical version(s) behind the latest primary write", result.RYWStalenessVersions),
		Detail: map[string]string{
			"kind":              "ryw",
			"stalenessVersions": strconv.FormatInt(result.RYWStalenessVersions, 10),
		},
	}
}

func (cd *ConsistencyDetector) monotonicEvent(now time.Time, result probes.ProbeResult) Event {
	return Event{
		Timestamp:   now,
		Type:        "invariant",
		Source:      "detector",
		Component:   "db",
		RuleID:      ruleMonotonicViolation,
		Audience:    "student",
		Title:       "Monotonic read violation",
		Description: fmt.Sprintf("Replica version regressed to %d after observing a higher version", result.ReplicaVersion),
		Detail: map[string]string{
			"kind":              "monotonic",
			"stalenessVersions": strconv.FormatInt(result.RYWStalenessVersions, 10),
			"replicaVersion":    strconv.FormatInt(result.ReplicaVersion, 10),
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
		Audience:    "student",
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
