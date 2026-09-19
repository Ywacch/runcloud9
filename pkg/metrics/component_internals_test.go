package metrics

import "testing"

func TestBuildComponentInternals_IncludesExtrasAndFreshness(t *testing.T) {
	t.Parallel()

	dm := &DeploymentMetrics{
		Components: map[string]ComponentMetrics{
			"db": {
				Component:     "db",
				StorageEngine: "postgresql",
				StorageRole:   StorageRolePrimary,
				CPUSource:     ResourceSourceMetricsAPI,
				MemSource:     ResourceSourceMetricsAPI,
				DbQueryRate:   42,
				StorageIO: &StorageIO{
					ReadRate:  100,
					WriteRate: 25,
					Unit:      "rows/s",
					ReadLabel: "Rows scanned/s",
				},
				Consistency: &ConsistencyStatus{
					RYWViolations:       1,
					PartitionStaleReads: 2,
				},
				Layers: []ComponentLayerMetrics{
					{ID: dbLayerIDPostgresPrimary, Role: dbLayerRolePrimary, Engine: "postgresql"},
					{
						ID:                    "postgres-replica-1",
						Role:                  dbLayerRoleReplica,
						Engine:                "postgresql",
						ReplicationLagSeconds: float64Ptr(0.1),
					},
				},
			},
			"app-server": {
				Component:   "app-server",
				RequestRate: 120,
				P99Latency:  0.08,
				Replicas: []ReplicaMetrics{
					{Name: "app-abc", RequestRate: 60, P99Latency: 0.05, Ready: true},
					{Name: "app-def", RequestRate: 60, P99Latency: 0.11},
				},
			},
		},
	}

	got := BuildComponentInternals(dm)

	db := got["db"]
	if len(db.Instances) != 2 {
		t.Fatalf("db instances: want 2 layers, got %d", len(db.Instances))
	}

	primary := db.Instances[0]
	storageIO, ok := primary.EngineExtras["storageIo"].(*StorageIO)
	if !ok || storageIO.ReadRate != 100 {
		t.Fatalf("primary storageIo: %+v", primary.EngineExtras)
	}
	cons, ok := primary.EngineExtras["consistency"].(*ConsistencyStatus)
	if !ok || cons.RYWViolations != 1 || cons.PartitionStaleReads != 2 {
		t.Fatalf("primary consistency: %+v", primary.EngineExtras)
	}
	throughputFresh, ok := primary.Freshness[SignalThroughput]
	if !ok || throughputFresh.Tier != FreshnessRate {
		t.Fatalf("primary should merge component throughput freshness: %+v", primary.Freshness)
	}

	replica := db.Instances[1]
	lag, ok := replica.Signals["replicationLagSeconds"].(float64)
	if !ok || lag != 0.1 {
		t.Fatalf("replica lag: %+v", replica.Signals)
	}
	repFresh, ok := replica.Freshness[SignalReplication]
	if !ok || repFresh.Tier != FreshnessRate {
		t.Fatalf("replica freshness: %+v", replica.Freshness)
	}

	app := got["app-server"]
	if len(app.Instances) != 2 {
		t.Fatalf("app instances: want 2 replicas, got %d", len(app.Instances))
	}
	var abc InstanceInternal
	for _, inst := range app.Instances {
		if inst.ID == "app-abc" {
			abc = inst
			break
		}
	}
	if abc.ID != "app-abc" {
		t.Fatalf("want app-abc replica, got %+v", app.Instances)
	}
	rate, ok := abc.Signals["requestRate"].(float64)
	if !ok || rate != 60 {
		t.Fatalf("app-abc requestRate: %+v", abc.Signals)
	}
	if abc.Freshness != nil {
		t.Fatal("per-pod instances should not inherit component freshness")
	}
}
