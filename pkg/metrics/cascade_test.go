package metrics

import (
	"testing"
)

func TestCascadeDetector_FirstTickNoEvents(t *testing.T) {
	t.Parallel()
	cd := NewCascadeDetector()
	ns := "ns-cascade"
	dm := dmForCascade(2.5, 60, 0, 0, 0, 0, 0, 0, 0, 0)
	ev := cd.Eval(ns, dm, TopologyFlags{}, nil)
	if len(ev) != 0 {
		t.Fatalf("first tick: want no events, got %+v", ev)
	}
}

func TestCascadeDetector_MQBackpressureLatch(t *testing.T) {
	t.Parallel()
	cd := NewCascadeDetector()
	ns := "ns-mq"

	// Warm history (no events).
	cd.Eval(ns, dmForCascade(1, 10, 0, 0, 0, 0, 0, 0, 0, 0), TopologyFlags{}, nil)

	ev1 := cd.Eval(ns, dmForCascade(2.5, 60, 0, 0, 0, 0, 0, 0, 0, 0), TopologyFlags{}, nil)
	if len(ev1) != 1 || ev1[0].Type != "cascade" || ev1[0].Component != "mq" {
		t.Fatalf("second tick: want 1 mq cascade, got %+v", ev1)
	}

	ev2 := cd.Eval(ns, dmForCascade(2.5, 60, 0, 0, 0, 0, 0, 0, 0, 0), TopologyFlags{}, nil)
	if len(ev2) != 0 {
		t.Fatalf("same condition: want suppressed, got %+v", ev2)
	}

	cd.Eval(ns, dmForCascade(1, 10, 0, 0, 0, 0, 0, 0, 0, 0), TopologyFlags{}, nil) // clear latch

	ev3 := cd.Eval(ns, dmForCascade(2.5, 60, 0, 0, 0, 0, 0, 0, 0, 0), TopologyFlags{}, nil)
	if len(ev3) != 1 {
		t.Fatalf("after clear: want 1 cascade again, got %+v", ev3)
	}
}

func TestCascadeDetector_CacheDBCorrelation(t *testing.T) {
	t.Parallel()
	cd := NewCascadeDetector()
	ns := "ns-cache-db"

	// prev: high hit rate, baseline DB RPS
	prev := dmForCascade(0, 0, 80, 20, 100, 0, 0, 0, 0, 0)
	cd.Eval(ns, prev, TopologyFlags{}, nil)

	// cur: hit rate drop >20%, DB +30%
	cur := dmForCascade(0, 0, 55, 45, 140, 0, 0, 0, 0, 0)
	ev := cd.Eval(ns, cur, TopologyFlags{}, nil)
	if len(ev) != 1 || ev[0].Component != "cache" {
		t.Fatalf("want cache cascade, got %+v", ev)
	}
}

func TestCascadeDetector_Cleanup(t *testing.T) {
	t.Parallel()
	cd := NewCascadeDetector()
	ns := "ns-clean"
	cd.Eval(ns, dmForCascade(1, 10, 0, 0, 0, 0, 0, 0, 0, 0), TopologyFlags{}, nil)
	cd.Eval(ns, dmForCascade(2.5, 60, 0, 0, 0, 0, 0, 0, 0, 0), TopologyFlags{}, nil)
	cd.Cleanup(ns)

	ev := cd.Eval(ns, dmForCascade(2.5, 60, 0, 0, 0, 0, 0, 0, 0, 0), TopologyFlags{}, nil)
	if len(ev) != 0 {
		t.Fatalf("after cleanup first tick on fresh ns state: want 0, got %+v", ev)
	}
	ev2 := cd.Eval(ns, dmForCascade(2.5, 60, 0, 0, 0, 0, 0, 0, 0, 0), TopologyFlags{}, nil)
	if len(ev2) != 1 {
		t.Fatalf("after cleanup, second tick should fire once, got %+v", ev2)
	}
}

// dmForCascade builds a minimal DeploymentMetrics for cascade rules.
// mqSat, qDepth: mq component. cacheHits/MissesLast9m, dbQueryRPS: cache/db.
// Remaining fields use app-server / db for other rules when non-zero.
func dmForCascade(mqSat, qDepth, cacheHits, cacheMiss, dbQuery float64, dbConn, dbMax, appErr, appP99, workerCPU float64) *DeploymentMetrics {
	dm := &DeploymentMetrics{
		Components: map[string]ComponentMetrics{
			"app-server": {
				Component:   "app-server",
				ErrorRate:   appErr,
				P99Latency:  appP99,
				RequestRate: 1,
			},
			"cache": {
				Component:         "cache",
				CacheHitsLast9m:   cacheHits,
				CacheMissesLast9m: cacheMiss,
			},
			"db": {
				Component:           "db",
				DbQueryRate:         dbQuery,
				DbActiveConnections: dbConn,
				MaxConnections:      dbMax,
			},
			"mq": {
				Component:         "mq",
				QueueDepth:        qDepth,
				MQSaturationRatio: mqSat,
			},
			"worker": {
				Component: "worker",
				CPUUsage:  workerCPU,
			},
		},
	}
	return dm
}
