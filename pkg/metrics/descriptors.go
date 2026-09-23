package metrics

import "sort"

// SignalDescriptor explains one normalized signal for brain context. Freshness
// is resolved from component_internals tags or the existing tier tables, never
// hardcoded per descriptor row.
type SignalDescriptor struct {
	Name      string          `json:"name"`
	Component string          `json:"component"`
	Unit      string          `json:"unit"`
	Semantic  string          `json:"semantic"`
	Meaning   string          `json:"meaning"`
	Freshness SignalFreshness `json:"freshness"`
}

type signalMeta struct {
	unit           string
	semantic       string
	meaning        string
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
		unit: "rps", semantic: "rate", meaning: "cache hits per second",
		freshnessGroup: SignalThroughput,
	},
	"cacheHitsLast9m": {
		unit: "count", semantic: "counter", meaning: "session total cache hits",
		freshnessGroup: SignalThroughput,
	},
	"cacheMisses": {
		unit: "rps", semantic: "rate", meaning: "cache misses per second",
		freshnessGroup: SignalThroughput,
	},
	"cacheMissesLast9m": {
		unit: "count", semantic: "counter", meaning: "session total cache misses",
		freshnessGroup: SignalThroughput,
	},
	"consumeRate": {
		unit: "rps", semantic: "rate", meaning: "message consume rate",
		freshnessGroup: SignalThroughput,
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
		unit: "count", semantic: "gauge", meaning: "open database connections",
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
		freshnessGroup: SignalQueue,
	},
	"messagesUnacked": {
		unit: "count", semantic: "gauge", meaning: "messages delivered but not yet acknowledged",
		freshnessGroup: SignalQueue,
	},
	"mqSaturationRatio": {
		unit: "percent", semantic: "ratio", meaning: "produce rate divided by consume rate",
		freshnessGroup: SignalThroughput,
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
		freshnessGroup: SignalThroughput,
	},
	"queueDepth": {
		unit: "count", semantic: "gauge", meaning: "broker queue depth",
		freshnessGroup: SignalQueue,
	},
	"ready": {
		unit: "count", semantic: "gauge", meaning: "pod readiness (1 when ready)",
		defaultTier: FreshnessDirect,
	},
	"replicationLagSeconds": {
		unit: "count", semantic: "gauge", meaning: "replication lag in seconds",
		freshnessGroup: SignalReplication,
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
	"workerSaturation": {
		unit: "percent", semantic: "ratio", meaning: "worker queue saturation ratio",
		freshnessGroup: SignalThroughput,
	},
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
				meta, ok := signalDescriptorCatalog[signalName]
				if !ok {
					continue
				}
				out = append(out, SignalDescriptor{
					Name:      signalName,
					Component: role,
					Unit:      meta.unit,
					Semantic:  meta.semantic,
					Meaning:   meta.meaning,
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
