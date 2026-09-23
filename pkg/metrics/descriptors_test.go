package metrics

import "testing"

func TestDescriptors_CoverEveryComponentInternalsField(t *testing.T) {
	t.Parallel()

	internals := BuildComponentInternals(maximalDeploymentMetricsForDescriptors())
	signalKeys := collectInternalsSignalKeys(internals)
	if len(signalKeys) == 0 {
		t.Fatal("expected signal keys from maximal fixture")
	}

	for key := range signalKeys {
		if _, ok := signalDescriptorCatalog[key]; !ok {
			t.Fatalf("missing descriptor catalog entry for signal %q", key)
		}
	}

	descs := BuildDescriptors(internals)
	descIndex := make(map[string]SignalDescriptor, len(descs))
	for _, d := range descs {
		descIndex[d.Component+"\x00"+d.Name] = d
	}

	for role, comp := range internals {
		for _, inst := range comp.Instances {
			for signalName := range inst.Signals {
				key := role + "\x00" + signalName
				if _, ok := descIndex[key]; !ok {
					t.Fatalf("BuildDescriptors missing %s.%s", role, signalName)
				}
			}
		}
	}
}

func TestDescriptors_FreshnessFromTierLogic(t *testing.T) {
	t.Parallel()

	dm := &DeploymentMetrics{
		Components: map[string]ComponentMetrics{
			"db": {
				Component:   "db",
				DbQueryRate: 10,
				CPUSource:   ResourceSourceMetricsAPI,
				MemSource:   ResourceSourceMetricsAPI,
				Layers: []ComponentLayerMetrics{
					{
						ID:          dbLayerIDPostgresPrimary,
						Role:        dbLayerRolePrimary,
						Engine:      "postgresql",
						DbQueryRate: 10,
						CPUUsage:    0.4,
						CPUSource:   ResourceSourceMetricsAPI,
					},
				},
			},
		},
	}
	internals := BuildComponentInternals(dm)
	descs := BuildDescriptors(internals)

	var cpuDesc *SignalDescriptor
	for i := range descs {
		if descs[i].Component == "db" && descs[i].Name == "cpuUsage" {
			cpuDesc = &descs[i]
			break
		}
	}
	if cpuDesc == nil {
		t.Fatalf("missing db.cpuUsage descriptor: %+v", descs)
	}
	if cpuDesc.Freshness.Tier != FreshnessMetricsAPI {
		t.Fatalf("cpuUsage freshness tier: %+v", cpuDesc.Freshness)
	}
	if cpuDesc.Freshness.NominalLagSeconds != nominalLagSeconds(FreshnessMetricsAPI) {
		t.Fatalf("cpuUsage nominal lag: %+v", cpuDesc.Freshness)
	}
}

func collectInternalsSignalKeys(internals ComponentInternals) map[string]bool {
	keys := make(map[string]bool)
	for _, comp := range internals {
		for _, inst := range comp.Instances {
			for k := range inst.Signals {
				keys[k] = true
			}
		}
	}
	return keys
}

func maximalDeploymentMetricsForDescriptors() *DeploymentMetrics {
	lag := 1.5
	lagBytes := int64(4096)
	return &DeploymentMetrics{
		Components: map[string]ComponentMetrics{
			"db": {
				Component:           "db",
				CPUUsage:            0.5,
				MemoryBytes:         1024,
				DbQueryRate:         10,
				DbQueryLatency:      0.02,
				DbTotalQueries:      100,
				DbActiveConnections: 5,
				MaxConnections:      100,
				CPUSource:           ResourceSourceMetricsAPI,
				MemSource:           ResourceSourceMetricsAPI,
				Layers: []ComponentLayerMetrics{
					{
						ID:                  dbLayerIDPostgresPrimary,
						Role:                dbLayerRolePrimary,
						Engine:              "postgresql",
						CPUUsage:            0.5,
						MemoryBytes:         1024,
						DbQueryRate:         10,
						DbTotalQueries:      100,
						DbActiveConnections: 5,
						RestartCount:        1,
						CPUSource:           ResourceSourceMetricsAPI,
						MemSource:           ResourceSourceMetricsAPI,
					},
					{
						ID:                    "postgres-replica-1",
						Role:                  dbLayerRoleReplica,
						Engine:                "postgresql",
						ReplicationLagSeconds: &lag,
						LagBytes:              &lagBytes,
					},
				},
			},
			"cache": {
				Component:         "cache",
				CPUUsage:          0.2,
				MemoryBytes:       2048,
				CacheHits:         50,
				CacheMisses:       5,
				EvictionsPerSec:   1,
				ExpirationsPerSec: 0.5,
				AvgLatency:        0.001,
			},
			"app-server": {
				Component:         "app-server",
				RequestRate:       100,
				TotalRequests:     1000,
				P50Latency:        0.01,
				P95Latency:        0.05,
				P99Latency:        0.1,
				ErrorRate:         0.5,
				TotalErrors9m:     10,
				P50Latency9m:      0.01,
				P95Latency9m:      0.05,
				P99Latency9m:      0.1,
				CacheHitsLast9m:   500,
				CacheMissesLast9m: 50,
				DbQueryRate:       20,
				Replicas: []ReplicaMetrics{
					{Name: "app-1", RequestRate: 50, P99Latency: 0.08, CPUUsage: 0.3, MemoryBytes: 512, Ready: true},
				},
			},
			"lb": {
				Component:         "lb",
				RequestRate:       200,
				ActiveConnections: 42,
				BackendPods: []BackendPod{
					{Name: "nginx-1", RequestRate: 200, P99Latency: 0.02, CPUUsage: 0.1, MemoryBytes: 256},
				},
			},
			"mq": {
				Component:         "mq",
				QueueDepth:        12,
				ProduceRate:       30,
				ConsumeRate:       25,
				MessagesReady:     8,
				MessagesUnacked:   4,
				MQSaturationRatio: 1.2,
			},
			"worker": {
				Component:        "worker",
				RequestRate:      25,
				ConsumeRate:      25,
				WorkerSaturation: 0.8,
				QueueDepth:       5,
			},
		},
	}
}
