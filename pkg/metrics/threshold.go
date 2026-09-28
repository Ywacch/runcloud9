package metrics

import (
	"sync"
	"time"

	"github.com/ywacch/cloud9/pkg/rules"
	"github.com/ywacch/cloud9/pkg/templates"
)

// ThresholdDetector emits at most one "threshold" event per rule while a condition stays true;
// it resets when the condition clears so a later crossing fires again.
type ThresholdDetector struct {
	mu sync.Mutex
	// namespace -> rule id -> currently in alert (suppress repeat until recovery)
	suppressed map[string]map[string]bool
}

// NewThresholdDetector creates a detector with empty state.
func NewThresholdDetector() *ThresholdDetector {
	return &ThresholdDetector{
		suppressed: make(map[string]map[string]bool),
	}
}

const (
	ruleCacheHitLow    = rules.IDCacheHitLow
	ruleErrorRate      = rules.IDErrorRate
	ruleDBLatency      = rules.IDDBLatency
	ruleQueueDepth     = rules.IDQueueDepth
	ruleDBConnPressure = rules.IDDBConnPressure
	ruleMQBackpressure = rules.IDMQBackpressure
)

// Eval returns threshold events for conditions that just crossed into a bad state.
func (t *ThresholdDetector) Eval(namespace string, dm *DeploymentMetrics, topo TopologyFlags, tmpl *templates.TemplateSpec) []Event {
	if t == nil || dm == nil || namespace == "" {
		return nil
	}
	var out []Event
	now := time.Now()

	app := dm.Components["app-server"]
	cache := dm.Components["cache"]
	db := dm.Components["db"]
	mq := dm.Components["mq"]

	hitRate := lifetimeHitRate(cache)
	totalWindow := cache.CacheHitsLast9m + cache.CacheMissesLast9m
	if totalWindow > 0 && hitRate < 30 {
		if t.fire(namespace, ruleCacheHitLow) {
			ev := Event{
				Timestamp:   now,
				Type:        "threshold",
				Source:      "detector",
				Component:   "cache",
				RuleID:      ruleCacheHitLow,
				Title:       "Low cache hit rate",
				Description: "Rolling 9m cache hit rate dropped below 30%",
			}
			n := Narrate(ruleCacheHitLow, dm, topo, tmpl)
			ev.Narrative, ev.Suggestion, ev.Related = n.Narrative, n.Suggestion, n.Related
			out = append(out, ev)
		}
	} else {
		t.clear(namespace, ruleCacheHitLow)
	}

	if app.ErrorRate > 1 {
		if t.fire(namespace, ruleErrorRate) {
			ev := Event{
				Timestamp:   now,
				Type:        "threshold",
				Source:      "detector",
				Component:   "app-server",
				RuleID:      ruleErrorRate,
				Title:       "Elevated HTTP error rate",
				Description: "HTTP 5xx rate exceeds 1 req/s",
			}
			n := Narrate(ruleErrorRate, dm, topo, tmpl)
			ev.Narrative, ev.Suggestion, ev.Related = n.Narrative, n.Suggestion, n.Related
			out = append(out, ev)
		}
	} else {
		t.clear(namespace, ruleErrorRate)
	}

	if db.DbQueryLatency > 0.5 {
		if t.fire(namespace, ruleDBLatency) {
			ev := Event{
				Timestamp:   now,
				Type:        "threshold",
				Source:      "detector",
				Component:   "db",
				RuleID:      ruleDBLatency,
				Title:       "High DB query latency",
				Description: "Average DB query latency exceeds 500ms",
			}
			n := Narrate(ruleDBLatency, dm, topo, tmpl)
			ev.Narrative, ev.Suggestion, ev.Related = n.Narrative, n.Suggestion, n.Related
			out = append(out, ev)
		}
	} else {
		t.clear(namespace, ruleDBLatency)
	}

	if mq.QueueDepth > 100 {
		if t.fire(namespace, ruleQueueDepth) {
			ev := Event{
				Timestamp:   now,
				Type:        "threshold",
				Source:      "detector",
				Component:   "mq",
				RuleID:      ruleQueueDepth,
				Title:       "Queue depth high",
				Description: "RabbitMQ queue depth exceeds 100 messages",
			}
			n := Narrate(ruleQueueDepth, dm, topo, tmpl)
			ev.Narrative, ev.Suggestion, ev.Related = n.Narrative, n.Suggestion, n.Related
			out = append(out, ev)
		}
	} else {
		t.clear(namespace, ruleQueueDepth)
	}

	if db.MaxConnections > 0 && db.DbActiveConnections/db.MaxConnections > 0.8 {
		if t.fire(namespace, ruleDBConnPressure) {
			ev := Event{
				Timestamp:   now,
				Type:        "threshold",
				Source:      "detector",
				Component:   "db",
				RuleID:      ruleDBConnPressure,
				Title:       "Database connection pressure",
				Description: "Open connections exceed 80% of max_connections",
			}
			n := Narrate(ruleDBConnPressure, dm, topo, tmpl)
			ev.Narrative, ev.Suggestion, ev.Related = n.Narrative, n.Suggestion, n.Related
			out = append(out, ev)
		}
	} else {
		t.clear(namespace, ruleDBConnPressure)
	}

	if mq.MQSaturationRatio > 2 && mq.QueueDepth > 50 {
		if t.fire(namespace, ruleMQBackpressure) {
			ev := Event{
				Timestamp:   now,
				Type:        "threshold",
				Source:      "detector",
				Component:   "mq",
				RuleID:      ruleMQBackpressure,
				Title:       "Producer outpacing consumer",
				Description: "MQ saturation ratio > 2 with queue depth > 50",
			}
			n := Narrate(ruleMQBackpressure, dm, topo, tmpl)
			ev.Narrative, ev.Suggestion, ev.Related = n.Narrative, n.Suggestion, n.Related
			out = append(out, ev)
		}
	} else {
		t.clear(namespace, ruleMQBackpressure)
	}

	return out
}

// Cleanup removes threshold latch state for a namespace.
func (t *ThresholdDetector) Cleanup(namespace string) {
	if t == nil || namespace == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.suppressed, namespace)
}

func (t *ThresholdDetector) fire(namespace, rule string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	m := t.suppressed[namespace]
	if m == nil {
		m = make(map[string]bool)
		t.suppressed[namespace] = m
	}
	if m[rule] {
		return false
	}
	m[rule] = true
	return true
}

func (t *ThresholdDetector) clear(namespace, rule string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	m := t.suppressed[namespace]
	if m == nil {
		return
	}
	delete(m, rule)
	if len(m) == 0 {
		delete(t.suppressed, namespace)
	}
}
