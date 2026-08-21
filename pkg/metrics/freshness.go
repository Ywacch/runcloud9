package metrics

import "time"

// FreshnessTier classifies how quickly a signal group reflects reality, keyed by
// collection method rather than engine. New engines map their collection paths
// onto the same tiers instead of introducing per-engine freshness rules.
type FreshnessTier string

const (
	// FreshnessDirect is a live poll of process state (loadgen, nginx stub_status,
	// RabbitMQ management/prometheus plugin): ~2s behind at most.
	FreshnessDirect FreshnessTier = "direct"
	// FreshnessRate is a PromQL rate()/gauge over scraped series: bounded by
	// PromRateWindow plus the Prometheus scrape interval.
	FreshnessRate FreshnessTier = "rate"
	// FreshnessMetricsAPI is kubelet -> metrics-server (metrics.k8s.io): updates in
	// ~15-60s steps on GKE Autopilot; polling faster repeats the cached value.
	FreshnessMetricsAPI FreshnessTier = "metrics_api"
	// FreshnessEstimated means no live series existed and the value came from kube
	// resource requests; it is a static placeholder, not an observation.
	FreshnessEstimated FreshnessTier = "estimated"
)

// Signal-group keys used in ComponentMetrics.Freshness / ComponentLayerMetrics.Freshness.
const (
	SignalThroughput   = "throughput"   // request/query/hit rates, latencies, errors
	SignalContainerCPU = "containerCpu" // CPUUsage
	SignalContainerMem = "containerMem" // MemoryBytes
	SignalConnections  = "connections"  // ActiveConnections / DbActiveConnections
	SignalQueue        = "queue"        // QueueDepth / MessagesReady / MessagesUnacked
	SignalReplication  = "replication"  // ReplicationLagSeconds on Postgres replica layers
)

// SignalFreshness tags one signal group on a component or layer with its
// observation tier and nominal collection lag.
type SignalFreshness struct {
	Tier FreshnessTier `json:"tier"`
	// NominalLagSeconds is the typical worst-case delay between reality and the
	// reported value; 0 for estimated (no live observation to lag behind).
	NominalLagSeconds float64 `json:"nominalLagSeconds"`
	// Source names the collection path (e.g. prometheus_rate, cadvisor,
	// metrics_api, kube_requests, direct_poll).
	Source string `json:"source,omitempty"`
}

// prometheusScrapeSeconds mirrors scrapeInterval in k8s/helm/prometheus-values.yaml.
// PromRateWindow requires scrapeInterval <= 5s, so the rate-tier lag derives from both.
const prometheusScrapeSeconds = 5.0

// nominalLagSeconds returns the typical collection lag for a tier. The rate tier
// tracks PromRateWindow so the constant cannot drift from the live query window.
func nominalLagSeconds(t FreshnessTier) float64 {
	switch t {
	case FreshnessDirect:
		return 2
	case FreshnessRate:
		w, err := time.ParseDuration(PromRateWindow)
		if err != nil {
			return 15
		}
		return w.Seconds() + prometheusScrapeSeconds
	case FreshnessMetricsAPI:
		return 45 // midpoint of the observed 15-60s step cadence
	default:
		return 0
	}
}

// tierForResourceSource maps a container CPU/mem source tag (cadvisor | metrics_api |
// requests | empty) plus the estimated flag onto a freshness tier. Empty source with a
// live value means a Prometheus-scraped series (process_*, engine exporter) — rate tier.
func tierForResourceSource(src string, estimated bool) FreshnessTier {
	if estimated {
		return FreshnessEstimated
	}
	switch src {
	case ResourceSourceCadvisor:
		return FreshnessRate
	case ResourceSourceMetricsAPI:
		return FreshnessMetricsAPI
	case ResourceSourceRequests:
		return FreshnessEstimated
	default:
		return FreshnessRate
	}
}

// defaultSourceForTier names the collection path when the collector did not tag one.
func defaultSourceForTier(t FreshnessTier) string {
	switch t {
	case FreshnessDirect:
		return "direct_poll"
	case FreshnessRate:
		return "prometheus_rate"
	case FreshnessMetricsAPI:
		return "metrics_api"
	default:
		return "kube_requests"
	}
}

// signalFreshness builds the JSON-friendly tag; source falls back to the tier default.
func signalFreshness(tier FreshnessTier, source string) SignalFreshness {
	if source == "" {
		source = defaultSourceForTier(tier)
	}
	return SignalFreshness{
		Tier:              tier,
		NominalLagSeconds: nominalLagSeconds(tier),
		Source:            source,
	}
}

// componentSignalTiers declares the non-resource signal tiers per component. Container
// CPU/mem tiers are always derived from the live CPUSource/MemSource tags instead.
var componentSignalTiers = map[string]map[string]FreshnessTier{
	"app-server": {SignalThroughput: FreshnessRate},
	"worker":     {SignalThroughput: FreshnessRate},
	"cache":      {SignalThroughput: FreshnessRate},
	"db":         {SignalThroughput: FreshnessRate, SignalConnections: FreshnessRate, SignalReplication: FreshnessRate},
	"lb":         {SignalThroughput: FreshnessRate, SignalConnections: FreshnessDirect},
	"mq":         {SignalThroughput: FreshnessDirect, SignalQueue: FreshnessDirect},
}

// annotateFreshness fills the Freshness map on every component and cache layer of an
// assembled snapshot. Pure function over collected fields — no queries, so it must run
// after all components and layers are final (post storage profiles / layer sync).
func annotateFreshness(dm *DeploymentMetrics) {
	if dm == nil {
		return
	}
	for name, cm := range dm.Components {
		fresh := make(map[string]SignalFreshness, len(componentSignalTiers[name])+2)
		for key, tier := range componentSignalTiers[name] {
			fresh[key] = signalFreshness(tier, "")
		}
		fresh[SignalContainerCPU] = signalFreshness(
			tierForResourceSource(cm.CPUSource, cm.CPUIsEstimated), cm.CPUSource)
		fresh[SignalContainerMem] = signalFreshness(
			tierForResourceSource(cm.MemSource, cm.MemIsEstimated), cm.MemSource)
		cm.Freshness = fresh
		for i := range cm.Layers {
			layer := &cm.Layers[i]
			layerFresh := map[string]SignalFreshness{
				SignalContainerCPU: signalFreshness(
					tierForResourceSource(layer.CPUSource, layer.CPUIsEstimated), layer.CPUSource),
				SignalContainerMem: signalFreshness(
					tierForResourceSource(layer.MemSource, layer.MemIsEstimated), layer.MemSource),
			}
			if name == "db" && layer.Role == dbLayerRoleReplica {
				layerFresh[SignalReplication] = signalFreshness(FreshnessRate, "prometheus_gauge")
			}
			layer.Freshness = layerFresh
		}
		dm.Components[name] = cm
	}
}
