package metrics

import (
	"sync"
	"time"
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
	ruleCacheHitLow      = "cache_hit_low"
	ruleErrorRate        = "error_rate"
	ruleDBLatency        = "db_latency"
	ruleQueueDepth       = "queue_depth"
	ruleDBConnPressure   = "db_conn_pressure"
	ruleMQBackpressure   = "mq_backpressure"
)

// Eval returns threshold events for conditions that just crossed into a bad state.
func (t *ThresholdDetector) Eval(namespace string, dm *DeploymentMetrics) []Event {
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
			out = append(out, Event{
				Timestamp:   now,
				Type:        "threshold",
				Source:      "detector",
				Component:   "cache",
				Title:       "Low cache hit rate",
				Description: "Rolling 9m cache hit rate dropped below 30%",
			})
		}
	} else {
		t.clear(namespace, ruleCacheHitLow)
	}

	if app.ErrorRate > 1 {
		if t.fire(namespace, ruleErrorRate) {
			out = append(out, Event{
				Timestamp:   now,
				Type:        "threshold",
				Source:      "detector",
				Component:   "app-server",
				Title:       "Elevated HTTP error rate",
				Description: "HTTP 5xx rate exceeds 1 req/s",
			})
		}
	} else {
		t.clear(namespace, ruleErrorRate)
	}

	if db.DbQueryLatency > 0.5 {
		if t.fire(namespace, ruleDBLatency) {
			out = append(out, Event{
				Timestamp:   now,
				Type:        "threshold",
				Source:      "detector",
				Component:   "db",
				Title:       "High DB query latency",
				Description: "Average DB query latency exceeds 500ms",
			})
		}
	} else {
		t.clear(namespace, ruleDBLatency)
	}

	if mq.QueueDepth > 100 {
		if t.fire(namespace, ruleQueueDepth) {
			out = append(out, Event{
				Timestamp:   now,
				Type:        "threshold",
				Source:      "detector",
				Component:   "mq",
				Title:       "Queue depth high",
				Description: "RabbitMQ queue depth exceeds 100 messages",
			})
		}
	} else {
		t.clear(namespace, ruleQueueDepth)
	}

	if db.MaxConnections > 0 && db.DbActiveConnections/db.MaxConnections > 0.8 {
		if t.fire(namespace, ruleDBConnPressure) {
			out = append(out, Event{
				Timestamp:   now,
				Type:        "threshold",
				Source:      "detector",
				Component:   "db",
				Title:       "Database connection pressure",
				Description: "Active connections exceed 80% of max_connections",
			})
		}
	} else {
		t.clear(namespace, ruleDBConnPressure)
	}

	if mq.MQSaturationRatio > 2 && mq.QueueDepth > 50 {
		if t.fire(namespace, ruleMQBackpressure) {
			out = append(out, Event{
				Timestamp:   now,
				Type:        "threshold",
				Source:      "detector",
				Component:   "mq",
				Title:       "Producer outpacing consumer",
				Description: "MQ saturation ratio > 2 with queue depth > 50",
			})
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
