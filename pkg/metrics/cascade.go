package metrics

import (
	"log/slog"
	"sync"
	"time"

	"github.com/ywacch/cloud9/pkg/templates"
)

// cascadeCompact holds the fields needed for correlation rules (cheap to store in a short window).
type cascadeCompact struct {
	HitRate      float64
	DBQueryRPS   float64
	QueueDepth   float64
	WorkerCPU    float64
	DBConnPct    float64
	AppErrRPS    float64
	AppP99       float64
	CacheMissPct float64
	MQSat        float64
}

func compactCascade(dm *DeploymentMetrics) cascadeCompact {
	var c cascadeCompact
	if dm == nil {
		return c
	}
	app := dm.Components["app-server"]
	cache := dm.Components["cache"]
	db := dm.Components["db"]
	mq := dm.Components["mq"]
	w := dm.Components["worker"]

	c.HitRate = lifetimeHitRate(cache)
	c.DBQueryRPS = db.DbQueryRate
	c.QueueDepth = mq.QueueDepth
	c.WorkerCPU = w.CPUUsage
	if db.MaxConnections > 0 {
		c.DBConnPct = 100 * db.DbActiveConnections / db.MaxConnections
	}
	c.AppErrRPS = app.ErrorRate
	c.AppP99 = app.P99Latency
	total := cache.CacheHitsLast9m + cache.CacheMissesLast9m
	if total > 0 {
		c.CacheMissPct = 100 * cache.CacheMissesLast9m / total
	}
	c.MQSat = mq.MQSaturationRatio
	return c
}

// CascadeDetector keeps a short history per namespace and emits correlation events (Type=cascade).
type CascadeDetector struct {
	mu sync.Mutex
	// namespace -> up to 5 recent compacts (oldest first after trim)
	history    map[string][]cascadeCompact
	suppressed map[string]map[string]bool
}

// NewCascadeDetector creates an empty detector.
func NewCascadeDetector() *CascadeDetector {
	return &CascadeDetector{
		history:    make(map[string][]cascadeCompact),
		suppressed: make(map[string]map[string]bool),
	}
}

const (
	cascadeRuleCacheDB        = "cascade_cache_db"
	cascadeRuleQueueWorker    = "cascade_queue_worker"
	cascadeRuleDBPoolErrors   = "cascade_dbpool_errors"
	cascadeRuleLatencyMiss    = "cascade_latency_miss"
	cascadeRuleMQBackpressure = "cascade_mq_backpressure"
)

// Eval compares the latest snapshot to the previous tick and returns new cascade events.
func (cd *CascadeDetector) Eval(namespace string, dm *DeploymentMetrics, topo TopologyFlags, tmpl *templates.TemplateSpec) []Event {
	if cd == nil || namespace == "" || dm == nil {
		return nil
	}
	cur := compactCascade(dm)

	cd.mu.Lock()
	defer cd.mu.Unlock()

	h := append(cd.history[namespace], cur)
	if len(h) > 5 {
		h = h[len(h)-5:]
	}
	cd.history[namespace] = h
	if len(h) < 2 {
		return nil
	}
	prev := h[len(h)-2]

	var out []Event
	now := time.Now()

	// 1. Cache hit rate drops >20% AND DB query rate increases >30%
	hitDrop := prev.HitRate > 0 && (prev.HitRate-cur.HitRate)/prev.HitRate > 0.2
	dbUp := prev.DBQueryRPS > 0 && (cur.DBQueryRPS-prev.DBQueryRPS)/prev.DBQueryRPS > 0.3
	if hitDrop && dbUp {
		if cd.fire(namespace, cascadeRuleCacheDB) {
			slog.Info("cascade detected", "namespace", namespace, "rule", cascadeRuleCacheDB)
			ev := Event{
				Timestamp:   now,
				Type:        "cascade",
				Source:      "detector",
				Component:   "cache",
				Title:       "Cache miss spike increasing DB load",
				Description: "Cache hit rate fell >20% while DB query rate rose >30% vs prior tick",
			}
			n := Narrate(cascadeRuleCacheDB, dm, topo, tmpl)
			ev.Narrative, ev.Suggestion, ev.Related = n.Narrative, n.Suggestion, n.Related
			out = append(out, ev)
		}
	} else {
		cd.clearRule(namespace, cascadeRuleCacheDB)
	}

	// 2. Queue depth +50% AND worker CPU +20%
	qUp := prev.QueueDepth > 0 && (cur.QueueDepth-prev.QueueDepth)/prev.QueueDepth > 0.5
	wUp := prev.WorkerCPU > 0 && (cur.WorkerCPU-prev.WorkerCPU)/prev.WorkerCPU > 0.2
	if qUp && wUp {
		if cd.fire(namespace, cascadeRuleQueueWorker) {
			slog.Info("cascade detected", "namespace", namespace, "rule", cascadeRuleQueueWorker)
			ev := Event{
				Timestamp:   now,
				Type:        "cascade",
				Source:      "detector",
				Component:   "mq",
				Title:       "Queue backlog driving worker CPU",
				Description: "Queue depth rose >50% while worker CPU rose >20% vs prior tick",
			}
			n := Narrate(cascadeRuleQueueWorker, dm, topo, tmpl)
			ev.Narrative, ev.Suggestion, ev.Related = n.Narrative, n.Suggestion, n.Related
			out = append(out, ev)
		}
	} else {
		cd.clearRule(namespace, cascadeRuleQueueWorker)
	}

	// 3. DB connections >80% of max AND app error rate more than doubled
	errDouble := prev.AppErrRPS > 0 && (cur.AppErrRPS-prev.AppErrRPS)/prev.AppErrRPS > 1
	if cur.DBConnPct > 80 && errDouble {
		if cd.fire(namespace, cascadeRuleDBPoolErrors) {
			slog.Info("cascade detected", "namespace", namespace, "rule", cascadeRuleDBPoolErrors)
			ev := Event{
				Timestamp:   now,
				Type:        "cascade",
				Source:      "detector",
				Component:   "db",
				Title:       "Connection pool stress and rising errors",
				Description: "DB connections >80% of max while HTTP error rate more than doubled vs prior tick",
			}
			n := Narrate(cascadeRuleDBPoolErrors, dm, topo, tmpl)
			ev.Narrative, ev.Suggestion, ev.Related = n.Narrative, n.Suggestion, n.Related
			out = append(out, ev)
		}
	} else {
		cd.clearRule(namespace, cascadeRuleDBPoolErrors)
	}

	// 4. App p99 +200% AND cache miss % +50%
	p99Up := prev.AppP99 > 0 && (cur.AppP99-prev.AppP99)/prev.AppP99 > 2
	missUp := prev.CacheMissPct > 0 && (cur.CacheMissPct-prev.CacheMissPct)/prev.CacheMissPct > 0.5
	if p99Up && missUp {
		if cd.fire(namespace, cascadeRuleLatencyMiss) {
			slog.Info("cascade detected", "namespace", namespace, "rule", cascadeRuleLatencyMiss)
			ev := Event{
				Timestamp:   now,
				Type:        "cascade",
				Source:      "detector",
				Component:   "app-server",
				Title:       "Cold cache degrading latency",
				Description: "p99 latency more than tripled while cache miss share rose >50% vs prior tick",
			}
			n := Narrate(cascadeRuleLatencyMiss, dm, topo, tmpl)
			ev.Narrative, ev.Suggestion, ev.Related = n.Narrative, n.Suggestion, n.Related
			out = append(out, ev)
		}
	} else {
		cd.clearRule(namespace, cascadeRuleLatencyMiss)
	}

	// 5. MQ saturation >2 AND queue depth >50
	if cur.MQSat > 2 && cur.QueueDepth > 50 {
		if cd.fire(namespace, cascadeRuleMQBackpressure) {
			slog.Info("cascade detected", "namespace", namespace, "rule", cascadeRuleMQBackpressure)
			ev := Event{
				Timestamp:   now,
				Type:        "cascade",
				Source:      "detector",
				Component:   "mq",
				Title:       "Producer outpacing consumer",
				Description: "MQ saturation ratio >2 with queue depth >50",
			}
			n := Narrate(cascadeRuleMQBackpressure, dm, topo, tmpl)
			ev.Narrative, ev.Suggestion, ev.Related = n.Narrative, n.Suggestion, n.Related
			out = append(out, ev)
		}
	} else {
		cd.clearRule(namespace, cascadeRuleMQBackpressure)
	}

	return out
}

// Cleanup removes history and latch state for a namespace.
func (cd *CascadeDetector) Cleanup(namespace string) {
	if cd == nil || namespace == "" {
		return
	}
	cd.mu.Lock()
	defer cd.mu.Unlock()
	delete(cd.history, namespace)
	delete(cd.suppressed, namespace)
}

func (cd *CascadeDetector) fire(namespace, rule string) bool {
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

func (cd *CascadeDetector) clearRule(namespace, rule string) {
	m := cd.suppressed[namespace]
	if m == nil {
		return
	}
	delete(m, rule)
	if len(m) == 0 {
		delete(cd.suppressed, namespace)
	}
}
