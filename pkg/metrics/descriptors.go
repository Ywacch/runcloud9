package metrics

import (
	"sort"
	"strings"

	"github.com/ywacch/cloud9/pkg/engines"
)

// SignalDescriptor explains one normalized signal for brain context. Freshness
// is resolved from component_internals tags or the existing tier tables, never
// hardcoded per descriptor row.
type SignalDescriptor struct {
	Name      string          `json:"name"`
	Component string          `json:"component"`
	Unit      string          `json:"unit"`
	Semantic  string          `json:"semantic"`
	Meaning   string          `json:"meaning"`
	Concept   string          `json:"concept,omitempty"`
	Freshness SignalFreshness `json:"freshness"`
}

type signalMeta struct {
	unit           string
	semantic       string
	meaning        string
	concept        string
	freshnessGroup string
	defaultTier    FreshnessTier
}

// signalDescriptorCatalog documents every key emitted by componentSignals,
// layerSignals, and podSignals. Add a row here when introducing a new internals
// signal or TestDescriptors_CoverEveryComponentInternalsField will fail.
var signalDescriptorCatalog = map[string]signalMeta{
	"activeConnections": {
		unit: "count", semantic: "gauge", meaning: "active client connections",
		freshnessGroup: SignalConnections,
	},
	"avgLatency": {
		unit: "ms", semantic: "latency", meaning: "mean operation latency in seconds",
		freshnessGroup: SignalThroughput,
	},
	"cacheHits": {
		unit: "rps", semantic: "rate", meaning: "cache lookups per second where the key was present",
		freshnessGroup: SignalThroughput,
	},
	"cacheHitsLast9m": {
		unit: "count", semantic: "counter", meaning: "session total cache hits",
		freshnessGroup: SignalThroughput,
	},
	"cacheMisses": {
		unit: "rps", semantic: "rate", meaning: "cache lookups per second where the key was absent",
		freshnessGroup: SignalThroughput,
	},
	"cacheMissesLast9m": {
		unit: "count", semantic: "counter", meaning: "session total cache misses",
		freshnessGroup: SignalThroughput,
	},
	"cacheErrors": {
		unit: "rps", semantic: "rate", meaning: "cache lookups per second the app could not complete",
		freshnessGroup: SignalThroughput,
	},
	"cacheErrorsLast9m": {
		unit: "count", semantic: "counter", meaning: "session total cache lookups the app could not complete",
		freshnessGroup: SignalThroughput,
	},
	"consumeRate": {
		unit: "rps", semantic: "rate", meaning: "message consume rate",
		concept: engines.ConceptFlowControl, freshnessGroup: SignalThroughput,
	},
	"cpuLimit": {
		unit: "count", semantic: "gauge", meaning: "configured CPU limit in cores",
		freshnessGroup: SignalContainerCPU,
	},
	"cpuUsage": {
		unit: "percent", semantic: "gauge", meaning: "container CPU utilization",
		freshnessGroup: SignalContainerCPU,
	},
	"dbActiveConnections": {
		unit: "count", semantic: "gauge", meaning: "open postgres backends (pg_stat_database numbackends)",
		freshnessGroup: SignalConnections,
	},
	"dbQueryLatency": {
		unit: "ms", semantic: "latency", meaning: "mean database query latency in seconds",
		freshnessGroup: SignalThroughput,
	},
	"dbQueryRate": {
		unit: "rps", semantic: "rate", meaning: "instrumented SQL queries per second",
		freshnessGroup: SignalThroughput,
	},
	"dbTotalQueries": {
		unit: "count", semantic: "counter", meaning: "session total instrumented SQL queries",
		freshnessGroup: SignalThroughput,
	},
	"errorRate": {
		unit: "rps", semantic: "rate", meaning: "HTTP or worker errors per second",
		freshnessGroup: SignalThroughput,
	},
	"evictionsPerSec": {
		unit: "rps", semantic: "rate", meaning: "cache key evictions per second",
		freshnessGroup: SignalThroughput,
	},
	"expirationsPerSec": {
		unit: "rps", semantic: "rate", meaning: "cache key expirations per second",
		freshnessGroup: SignalThroughput,
	},
	"evictionsLast9m": {
		unit: "count", semantic: "counter", meaning: "session total cache key evictions",
		freshnessGroup: SignalThroughput,
	},
	"expirationsLast9m": {
		unit: "count", semantic: "counter", meaning: "session total cache key expirations",
		freshnessGroup: SignalThroughput,
	},
	"lagBytes": {
		unit: "bytes", semantic: "gauge", meaning: "replication lag in bytes behind primary",
		freshnessGroup: SignalReplication,
	},
	"maxConnections": {
		unit: "count", semantic: "gauge", meaning: "database max_connections setting",
		freshnessGroup: SignalConnections,
	},
	"memoryLimit": {
		unit: "bytes", semantic: "gauge", meaning: "configured memory limit in bytes",
		freshnessGroup: SignalContainerMem,
	},
	"memoryUsage": {
		unit: "bytes", semantic: "gauge", meaning: "container working-set memory in bytes",
		freshnessGroup: SignalContainerMem,
	},
	"messagesReady": {
		unit: "count", semantic: "gauge", meaning: "queued messages ready for delivery",
		concept: engines.ConceptConsumerLag, freshnessGroup: SignalQueue,
	},
	"messagesUnacked": {
		unit: "count", semantic: "gauge", meaning: "messages delivered but not yet acknowledged",
		concept: engines.ConceptRedelivery, freshnessGroup: SignalQueue,
	},
	"mqSaturationRatio": {
		unit: "percent", semantic: "ratio", meaning: "produce rate divided by consume rate",
		concept: engines.ConceptFlowControl, freshnessGroup: SignalThroughput,
	},
	"p50Latency": {
		unit: "ms", semantic: "latency", meaning: "p50 request latency in seconds",
		freshnessGroup: SignalThroughput,
	},
	"p50Latency9m": {
		unit: "ms", semantic: "latency", meaning: "session p50 request latency in seconds",
		freshnessGroup: SignalThroughput,
	},
	"p95Latency": {
		unit: "ms", semantic: "latency", meaning: "p95 request latency in seconds",
		freshnessGroup: SignalThroughput,
	},
	"p95Latency9m": {
		unit: "ms", semantic: "latency", meaning: "session p95 request latency in seconds",
		freshnessGroup: SignalThroughput,
	},
	"p99Latency": {
		unit: "ms", semantic: "latency", meaning: "p99 request latency in seconds",
		freshnessGroup: SignalThroughput,
	},
	"p99Latency9m": {
		unit: "ms", semantic: "latency", meaning: "session p99 request latency in seconds",
		freshnessGroup: SignalThroughput,
	},
	"produceRate": {
		unit: "rps", semantic: "rate", meaning: "message publish rate",
		concept: engines.ConceptFlowControl, freshnessGroup: SignalThroughput,
	},
	"queueDepth": {
		unit: "count", semantic: "gauge", meaning: "broker queue depth",
		concept: engines.ConceptConsumerLag, freshnessGroup: SignalQueue,
	},
	"mqMemoryUsed": {
		unit: "bytes", semantic: "gauge", meaning: "broker resident memory",
		concept: engines.ConceptMemoryOverhead, freshnessGroup: SignalContainerMem,
	},
	"mqMemoryLimit": {
		unit: "bytes", semantic: "gauge", meaning: "broker memory limit",
		concept: engines.ConceptMemoryOverhead, freshnessGroup: SignalContainerMem,
	},
	"ready": {
		unit: "count", semantic: "gauge", meaning: "pod readiness (1 when ready)",
		defaultTier: FreshnessDirect,
	},
	"replicationLagSeconds": {
		unit: "count", semantic: "gauge", meaning: "replication lag in seconds",
		freshnessGroup: SignalReplication,
	},
	"redisOpsPerSec": {
		unit: "ops/s", semantic: "rate", meaning: "redis request-path command rate excluding SCAN",
		freshnessGroup: SignalThroughput,
	},
	"requestRate": {
		unit: "rps", semantic: "rate", meaning: "HTTP or worker request rate",
		freshnessGroup: SignalThroughput,
	},
	"restartCount": {
		unit: "count", semantic: "counter", meaning: "container restart count",
		freshnessGroup: SignalContainerCPU,
	},
	"totalErrors9m": {
		unit: "count", semantic: "counter", meaning: "session total HTTP or worker errors",
		freshnessGroup: SignalThroughput,
	},
	"totalRequests": {
		unit: "count", semantic: "counter", meaning: "session total HTTP or worker requests",
		freshnessGroup: SignalThroughput,
	},
	"timelineDeadTuples": {
		unit: "count", semantic: "gauge", meaning: "dead rows in timelines table awaiting vacuum",
		freshnessGroup: SignalThroughput,
	},
	"timelineRelationBytes": {
		unit: "bytes", semantic: "gauge", meaning: "on-disk size of the timelines fan-out table",
		freshnessGroup: SignalThroughput,
	},
	"workerSaturation": {
		unit: "percent", semantic: "ratio", meaning: "worker queue saturation ratio",
		freshnessGroup: SignalThroughput,
	},
}

// signalMetaFor returns catalog metadata for a component_internals signal name.
func signalMetaFor(signalName string) (signalMeta, bool) {
	meta, ok := signalDescriptorCatalog[signalName]
	if ok {
		return meta, true
	}
	if strings.HasPrefix(signalName, "redisCmd.") {
		cmd := strings.TrimPrefix(signalName, "redisCmd.")
		return signalMeta{
			unit:           "ops/s",
			semantic:       "rate",
			meaning:        "redis " + cmd + " command rate",
			freshnessGroup: SignalThroughput,
		}, true
	}
	return signalMeta{}, false
}

// BuildDescriptors emits one descriptor per (component role, signal name) pair
// present in component_internals.
func BuildDescriptors(internals ComponentInternals) []SignalDescriptor {
	if len(internals) == 0 {
		return nil
	}
	roles := make([]string, 0, len(internals))
	for role := range internals {
		roles = append(roles, role)
	}
	sort.Strings(roles)

	var out []SignalDescriptor
	seen := make(map[string]bool)
	for _, role := range roles {
		comp := internals[role]
		for _, inst := range comp.Instances {
			for signalName := range inst.Signals {
				key := role + "\x00" + signalName
				if seen[key] {
					continue
				}
				seen[key] = true
				meta, ok := signalMetaFor(signalName)
				if !ok {
					continue
				}
				out = append(out, SignalDescriptor{
					Name:      signalName,
					Component: role,
					Unit:      meta.unit,
					Semantic:  meta.semantic,
					Meaning:   meta.meaning,
					Concept:   meta.concept,
					Freshness: freshnessForSignal(role, signalName, inst.Freshness, meta),
				})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Component != out[j].Component {
			return out[i].Component < out[j].Component
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func freshnessForSignal(role, signalName string, instFresh map[string]SignalFreshness, meta signalMeta) SignalFreshness {
	if meta.freshnessGroup != "" && instFresh != nil {
		if f, ok := instFresh[meta.freshnessGroup]; ok {
			return f
		}
	}
	if meta.freshnessGroup != "" {
		if tiers, ok := componentSignalTiers[role]; ok {
			if tier, ok := tiers[meta.freshnessGroup]; ok {
				return signalFreshness(tier, "")
			}
		}
	}
	if meta.defaultTier != "" {
		return signalFreshness(meta.defaultTier, "")
	}
	return signalFreshness(FreshnessRate, "")
}
