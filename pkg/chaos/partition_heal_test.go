package chaos

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestHealPartitions_andExpireDue(t *testing.T) {
	ns := "cloud9-heal"
	expired := time.Now().Add(-time.Minute)
	active := time.Now().Add(10 * time.Minute)

	expiredPolicy, err := BuildPartitionNetworkPolicy(PartitionPolicyInput{
		Namespace:       ns,
		Scenario:        scenarioSplitPrimaryReplica,
		TargetComponent: "db",
		ExpiresAt:       expired,
		DestAppLabel:    postgresPrimaryAppLabel,
		Port:            postgresPartitionPort,
		Exclude:         PartitionPeerExclusion{AppLabel: postgresReplicaAppLabel},
	})
	if err != nil {
		t.Fatalf("build expired policy: %v", err)
	}
	activeStall := buildStallStateConfigMap(ns, "postgres-replica-0", "db", active)

	client := fake.NewClientset(expiredPolicy, activeStall)

	due, err := ExpireDuePartitions(context.Background(), client, ns, time.Now())
	if err != nil {
		t.Fatalf("ExpireDuePartitions: %v", err)
	}
	if len(due) != 1 || due[0].Scenario != scenarioSplitPrimaryReplica {
		t.Fatalf("due: %+v", due)
	}

	remaining, err := ListActivePartitions(context.Background(), client, ns)
	if err != nil {
		t.Fatalf("ListActivePartitions: %v", err)
	}
	if len(remaining) != 1 || remaining[0].Scenario != scenarioStallReplica {
		t.Fatalf("remaining: %+v", remaining)
	}
	if remaining[0].ReplicaPod != "postgres-replica-0" {
		t.Fatalf("remaining replica: %+v", remaining[0])
	}

	healed, err := HealPartitions(context.Background(), client, ns)
	if err != nil {
		t.Fatalf("HealPartitions: %v", err)
	}
	if len(healed) != 1 || healed[0].Scenario != scenarioStallReplica {
		t.Fatalf("healed: %+v", healed)
	}

	list, err := ListActivePartitions(context.Background(), client, ns)
	if err != nil {
		t.Fatalf("ListActivePartitions after heal: %v", err)
	}
	if len(list) != 0 {
		t.Fatalf("expected no active partitions, got %+v", list)
	}
}

func TestHealPartitions_noActive(t *testing.T) {
	client := fake.NewClientset()
	healed, err := HealPartitions(context.Background(), client, "empty-ns")
	if err != nil {
		t.Fatalf("HealPartitions: %v", err)
	}
	if len(healed) != 0 {
		t.Fatalf("expected nil healed, got %+v", healed)
	}
}

func TestListActivePartitions_readsLabels(t *testing.T) {
	ns := "cloud9-list"
	expires := time.Date(2026, 9, 12, 16, 0, 0, 0, time.UTC)
	policy, err := BuildPartitionNetworkPolicy(PartitionPolicyInput{
		Namespace:       ns,
		Scenario:        scenarioSplitAppStore,
		TargetComponent: "cache",
		ExpiresAt:       expires,
		DestAppLabel:    "redis",
		Port:            redisPartitionPort,
		Exclude:         PartitionPeerExclusion{AppLabel: appServerAppLabel},
	})
	if err != nil {
		t.Fatalf("BuildPartitionNetworkPolicy: %v", err)
	}
	client := fake.NewClientset(policy)

	active, err := ListActivePartitions(context.Background(), client, ns)
	if err != nil {
		t.Fatalf("ListActivePartitions: %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("active: %+v", active)
	}
	if active[0].Scenario != scenarioSplitAppStore || active[0].TargetComponent != "cache" {
		t.Fatalf("partition: %+v", active[0])
	}
	if !active[0].ExpiresAt.Equal(expires) {
		t.Fatalf("expires: got %v want %v", active[0].ExpiresAt, expires)
	}

	_, err = client.NetworkingV1().NetworkPolicies(ns).Get(context.Background(), policy.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("policy should exist: %v", err)
	}
}

func TestListActivePartitions_stallConfigMap(t *testing.T) {
	ns := "cloud9-list-stall"
	expires := time.Date(2026, 9, 17, 21, 0, 0, 0, time.UTC)
	cm := buildStallStateConfigMap(ns, "postgres-replica-0", "db", expires)
	client := fake.NewClientset(cm)

	active, err := ListActivePartitions(context.Background(), client, ns)
	if err != nil {
		t.Fatalf("ListActivePartitions: %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("active: %+v", active)
	}
	if active[0].Scenario != scenarioStallReplica || active[0].TargetComponent != "db" {
		t.Fatalf("partition: %+v", active[0])
	}
	if active[0].ReplicaPod != "postgres-replica-0" {
		t.Fatalf("replica: %+v", active[0])
	}
	if !active[0].ExpiresAt.Equal(expires) {
		t.Fatalf("expires: got %v want %v", active[0].ExpiresAt, expires)
	}

	policies, err := client.NetworkingV1().NetworkPolicies(ns).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list policies: %v", err)
	}
	if len(policies.Items) != 0 {
		t.Fatalf("stall must not require a NetworkPolicy, got %+v", policies.Items)
	}
}
