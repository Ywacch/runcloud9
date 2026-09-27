package metrics

import (
	"math"
	"testing"
)

func TestReplicaReadShares_evenThreeWaySplit(t *testing.T) {
	t.Parallel()
	rates := map[string]float64{
		"postgres-replica-0": 10,
		"postgres-replica-1": 10,
		"postgres-replica-2": 10,
	}
	shares, ok := ReplicaReadShares(rates)
	if !ok {
		t.Fatal("expected shares")
	}
	for host, rate := range rates {
		want := rate / 30
		if math.Abs(shares[host]-want) > 1e-9 {
			t.Fatalf("%s share: got %v want %v", host, shares[host], want)
		}
	}
	if imb := ReplicaReadImbalance(shares); imb != 0 {
		t.Fatalf("imbalance: got %v want 0", imb)
	}
}

func TestReplicaReadShares_oneHotReplica(t *testing.T) {
	t.Parallel()
	rates := map[string]float64{
		"postgres-replica-0": 70,
		"postgres-replica-1": 15,
		"postgres-replica-2": 15,
	}
	shares, ok := ReplicaReadShares(rates)
	if !ok {
		t.Fatal("expected shares")
	}
	if shares["postgres-replica-0"] != 0.7 {
		t.Fatalf("hot share: %v", shares["postgres-replica-0"])
	}
	wantImb := 0.7 - 1.0/3.0
	if imb := ReplicaReadImbalance(shares); imb != wantImb {
		t.Fatalf("imbalance: got %v want %v", imb, wantImb)
	}
}

func TestReplicaReadShares_singleReplica(t *testing.T) {
	t.Parallel()
	rates := map[string]float64{"postgres-replica-0": 42}
	shares, ok := ReplicaReadShares(rates)
	if !ok {
		t.Fatal("expected shares")
	}
	if shares["postgres-replica-0"] != 1 {
		t.Fatalf("share: %v", shares["postgres-replica-0"])
	}
	if imb := ReplicaReadImbalance(shares); imb != 0 {
		t.Fatalf("single replica imbalance: got %v want 0", imb)
	}
}

func TestReplicaReadShares_omitsLeaderOnly(t *testing.T) {
	t.Parallel()
	_, ok := ReplicaReadShares(map[string]float64{"leader": 100})
	if ok {
		t.Fatal("leader-only rates should omit share facts")
	}
}

func TestBuildLedger_EmitsReplicaReadShareFacts(t *testing.T) {
	t.Parallel()
	final := &DeploymentMetrics{
		Components: map[string]ComponentMetrics{
			"db": {
				Component: "db",
				Consistency: &ConsistencyStatus{
					ReadRateByReplica: map[string]float64{
						"postgres-replica-0": 30,
						"postgres-replica-1": 30,
						"postgres-replica-2": 30,
					},
				},
			},
		},
	}
	l := BuildLedger(LedgerInput{Final: final})
	if _, ok := l.Resolve(FactRef("fact:metric:db.readShare.postgres-replica-0")); !ok {
		t.Fatal("missing readShare fact")
	}
	if _, ok := l.Resolve(FactRef("fact:derived:db.replicaReadImbalance")); !ok {
		t.Fatal("missing imbalance fact")
	}
}
