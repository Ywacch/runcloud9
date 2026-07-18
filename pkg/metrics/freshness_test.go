package metrics

import (
	"testing"
	"time"
)

func TestTierForResourceSource(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		src       string
		estimated bool
		want      FreshnessTier
	}{
		{"cadvisor live", ResourceSourceCadvisor, false, FreshnessRate},
		{"metrics api", ResourceSourceMetricsAPI, false, FreshnessMetricsAPI},
		{"requests source", ResourceSourceRequests, false, FreshnessEstimated},
		{"empty source live (process_/exporter series)", "", false, FreshnessRate},
		{"estimated flag wins over cadvisor", ResourceSourceCadvisor, true, FreshnessEstimated},
		{"estimated flag wins over metrics api", ResourceSourceMetricsAPI, true, FreshnessEstimated},
		{"empty source estimated", "", true, FreshnessEstimated},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tierForResourceSource(tt.src, tt.estimated); got != tt.want {
				t.Fatalf("tierForResourceSource(%q, %v) = %q, want %q", tt.src, tt.estimated, got, tt.want)
			}
		})
	}
}

func TestNominalLagSeconds(t *testing.T) {
	t.Parallel()
	w, err := time.ParseDuration(PromRateWindow)
	if err != nil {
		t.Fatalf("PromRateWindow unparsable: %v", err)
	}
	wantRate := w.Seconds() + prometheusScrapeSeconds

	tests := []struct {
		name string
		tier FreshnessTier
		want float64
	}{
		{"direct", FreshnessDirect, 2},
		{"rate tracks PromRateWindow", FreshnessRate, wantRate},
		{"metrics api", FreshnessMetricsAPI, 45},
		{"estimated has no lag", FreshnessEstimated, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := nominalLagSeconds(tt.tier); got != tt.want {
				t.Fatalf("nominalLagSeconds(%q) = %v, want %v", tt.tier, got, tt.want)
			}
		})
	}
}

func TestAnnotateFreshness_PerComponent(t *testing.T) {
	t.Parallel()
	dm := &DeploymentMetrics{
		Components: map[string]ComponentMetrics{
			"app-server": {Component: "app-server"}, // empty sources, live
			"db": {
				Component: "db",
				CPUSource: ResourceSourceMetricsAPI,
				MemSource: ResourceSourceMetricsAPI,
			},
			"lb": {
				Component:      "lb",
				CPUSource:      ResourceSourceRequests,
				CPUIsEstimated: true,
				MemSource:      ResourceSourceMetricsAPI,
			},
			"mq": {
				Component: "mq",
				CPUSource: ResourceSourceMetricsAPI,
				MemSource: ResourceSourceMetricsAPI,
			},
		},
	}
	annotateFreshness(dm)

	app := dm.Components["app-server"].Freshness
	if app[SignalThroughput].Tier != FreshnessRate {
		t.Fatalf("app throughput: %+v", app[SignalThroughput])
	}
	if app[SignalContainerCPU].Tier != FreshnessRate || app[SignalContainerCPU].Source != "prometheus_rate" {
		t.Fatalf("app containerCpu: %+v", app[SignalContainerCPU])
	}
	if _, ok := app[SignalQueue]; ok {
		t.Fatal("app should not have queue signal")
	}

	db := dm.Components["db"].Freshness
	if db[SignalThroughput].Tier != FreshnessRate {
		t.Fatalf("db throughput: %+v", db[SignalThroughput])
	}
	if db[SignalConnections].Tier != FreshnessRate {
		t.Fatalf("db connections: %+v", db[SignalConnections])
	}
	if db[SignalContainerCPU].Tier != FreshnessMetricsAPI || db[SignalContainerCPU].NominalLagSeconds != 45 {
		t.Fatalf("db containerCpu should be metrics_api tier: %+v", db[SignalContainerCPU])
	}

	lb := dm.Components["lb"].Freshness
	if lb[SignalConnections].Tier != FreshnessDirect || lb[SignalConnections].NominalLagSeconds != 2 {
		t.Fatalf("lb connections should be direct: %+v", lb[SignalConnections])
	}
	if lb[SignalThroughput].Tier != FreshnessRate {
		t.Fatalf("lb throughput: %+v", lb[SignalThroughput])
	}
	if lb[SignalContainerCPU].Tier != FreshnessEstimated || lb[SignalContainerCPU].NominalLagSeconds != 0 {
		t.Fatalf("lb containerCpu should be estimated: %+v", lb[SignalContainerCPU])
	}
	if lb[SignalContainerMem].Tier != FreshnessMetricsAPI {
		t.Fatalf("lb containerMem: %+v", lb[SignalContainerMem])
	}

	mq := dm.Components["mq"].Freshness
	if mq[SignalQueue].Tier != FreshnessDirect {
		t.Fatalf("mq queue should be direct: %+v", mq[SignalQueue])
	}
	if mq[SignalThroughput].Tier != FreshnessDirect {
		t.Fatalf("mq throughput should be direct: %+v", mq[SignalThroughput])
	}
}

func TestAnnotateFreshness_CacheLayerSplit(t *testing.T) {
	t.Parallel()
	dm := &DeploymentMetrics{
		Components: map[string]ComponentMetrics{
			"cache": {
				Component: "cache",
				Layers: []ComponentLayerMetrics{
					{
						ID:             "redis-internal",
						Role:           "backing-store",
						CPUIsEstimated: true, // cadvisor absent → kube requests
					},
					{
						ID:   "cache-proxy",
						Role: "smart-client",
						// empty source, live process_* series
					},
				},
			},
		},
	}
	annotateFreshness(dm)

	layers := dm.Components["cache"].Layers
	redis := layers[0].Freshness
	if redis[SignalContainerCPU].Tier != FreshnessEstimated {
		t.Fatalf("redis-internal cpu should be estimated: %+v", redis[SignalContainerCPU])
	}
	proxy := layers[1].Freshness
	if proxy[SignalContainerCPU].Tier != FreshnessRate {
		t.Fatalf("cache-proxy cpu should be rate: %+v", proxy[SignalContainerCPU])
	}
	if proxy[SignalContainerCPU].NominalLagSeconds != nominalLagSeconds(FreshnessRate) {
		t.Fatalf("cache-proxy lag: %+v", proxy[SignalContainerCPU])
	}
	// Component-level throughput still tagged for cache.
	if dm.Components["cache"].Freshness[SignalThroughput].Tier != FreshnessRate {
		t.Fatalf("cache throughput: %+v", dm.Components["cache"].Freshness)
	}
}

func TestAnnotateFreshness_NilAndUnknownComponent(t *testing.T) {
	t.Parallel()
	annotateFreshness(nil) // must not panic

	dm := &DeploymentMetrics{
		Components: map[string]ComponentMetrics{
			"custom-thing": {Component: "custom-thing"},
		},
	}
	annotateFreshness(dm)
	fresh := dm.Components["custom-thing"].Freshness
	if len(fresh) != 2 {
		t.Fatalf("unknown component should get only container signals: %#v", fresh)
	}
	if fresh[SignalContainerCPU].Tier != FreshnessRate {
		t.Fatalf("containerCpu default: %+v", fresh[SignalContainerCPU])
	}
}
