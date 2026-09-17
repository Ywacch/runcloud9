package chaos

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

func dbDesignWithReplicas() deployer.DeploymentDesign {
	return deployer.DeploymentDesign{
		Template: "url-shortener",
		Nodes: []deployer.DeploymentNode{
			{Type: "app-server", Name: "App"},
			{Type: "db", Name: "DB", Config: map[string]any{
				"engine":       "postgresql",
				"readReplicas": 1,
			}},
		},
	}
}

func cacheDesign() deployer.DeploymentDesign {
	return deployer.DeploymentDesign{
		Template: "url-shortener",
		Nodes: []deployer.DeploymentNode{
			{Type: "app-server", Name: "App"},
			{Type: "cache", Name: "Cache"},
		},
	}
}

func runningReplicaPod(ns, name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    map[string]string{"app": postgresReplicaAppLabel},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func testStallScenario(client kubernetes.Interface) (*PartitionScenario, *[]string, *[]string) {
	hosts := []string{}
	dropped := []string{}
	scenario := &PartitionScenario{
		Client:   client,
		Scenario: scenarioStallReplica,
		SetReplicaPrimaryHost: func(_ context.Context, _ kubernetes.Interface, _, _, host string) error {
			hosts = append(hosts, host)
			return nil
		},
		DropReplication: func(_ context.Context, _ kubernetes.Interface, _, applicationName string) error {
			dropped = append(dropped, applicationName)
			return nil
		},
	}
	return scenario, &hosts, &dropped
}

func TestPartitionScenario_ApplyAndRevert(t *testing.T) {
	ns := "cloud9-partition"
	client := fake.NewClientset(runningReplicaPod(ns, "postgres-replica-0"))
	design := dbDesignWithReplicas()
	scenario, hosts, dropped := testStallScenario(client)

	if err := scenario.Apply(context.Background(), ns, design, "db"); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	_, err := client.NetworkingV1().NetworkPolicies(ns).Get(context.Background(), "c9-chaos-stall-replica", metav1.GetOptions{})
	if err == nil {
		t.Fatal("stall_replica must not create a NetworkPolicy")
	}
	cm, err := client.CoreV1().ConfigMaps(ns).Get(context.Background(), "c9-chaos-stall-replica", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get stall state: %v", err)
	}
	if cm.Labels[ChaosLabelScenario] != scenarioStallReplica {
		t.Fatalf("scenario label: %q", cm.Labels[ChaosLabelScenario])
	}
	if cm.Annotations[ChaosAnnotationReplicaPod] != "postgres-replica-0" {
		t.Fatalf("replica pod: %q", cm.Annotations[ChaosAnnotationReplicaPod])
	}
	if len(*hosts) != 1 || (*hosts)[0] != stallBlackholeHost {
		t.Fatalf("hosts: %v", *hosts)
	}
	if len(*dropped) != 1 || (*dropped)[0] != "postgres-replica-0" {
		t.Fatalf("dropped: %v", *dropped)
	}

	if err := scenario.Revert(context.Background(), ns, design, "db"); err != nil {
		t.Fatalf("Revert: %v", err)
	}
	if len(*hosts) != 2 || (*hosts)[1] != stallPrimaryHost {
		t.Fatalf("restore host: %v", *hosts)
	}
	_, err = client.CoreV1().ConfigMaps(ns).Get(context.Background(), "c9-chaos-stall-replica", metav1.GetOptions{})
	if err == nil {
		t.Fatal("expected stall state deleted")
	}
}

func TestPartitionScenario_doubleApplyIdempotent(t *testing.T) {
	ns := "cloud9-double-apply"
	client := fake.NewClientset(runningReplicaPod(ns, "postgres-replica-0"))
	design := dbDesignWithReplicas()
	scenario := &PartitionScenario{Client: client, Scenario: scenarioSplitPrimaryReplica}

	if err := scenario.Apply(context.Background(), ns, design, "db"); err != nil {
		t.Fatalf("first Apply: %v", err)
	}
	if err := scenario.Apply(context.Background(), ns, design, "db"); err != nil {
		t.Fatalf("second Apply: %v", err)
	}
	list, err := client.NetworkingV1().NetworkPolicies(ns).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("policy count: got %d want 1", len(list.Items))
	}
}

func TestPartitionScenario_doubleRevertIdempotent(t *testing.T) {
	ns := "cloud9-double-revert"
	client := fake.NewClientset(runningReplicaPod(ns, "postgres-replica-0"))
	design := dbDesignWithReplicas()
	scenario := &PartitionScenario{Client: client, Scenario: scenarioSplitPrimaryReplica}

	if err := scenario.Apply(context.Background(), ns, design, "db"); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if err := scenario.Revert(context.Background(), ns, design, "db"); err != nil {
		t.Fatalf("first Revert: %v", err)
	}
	if err := scenario.Revert(context.Background(), ns, design, "db"); err != nil {
		t.Fatalf("second Revert: %v", err)
	}
}

func TestPartitionScenario_secondPartitionRejected(t *testing.T) {
	ns := "cloud9-one-slot"
	client := fake.NewClientset(runningReplicaPod(ns, "postgres-replica-0"))
	design := dbDesignWithReplicas()

	stall, _, _ := testStallScenario(client)
	split := &PartitionScenario{Client: client, Scenario: scenarioSplitPrimaryReplica}

	if err := stall.Apply(context.Background(), ns, design, "db"); err != nil {
		t.Fatalf("stall Apply: %v", err)
	}
	err := split.Apply(context.Background(), ns, design, "db")
	if !errors.Is(err, ErrPartitionAlreadyActive) {
		t.Fatalf("expected ErrPartitionAlreadyActive, got %v", err)
	}
}

func TestPartitionScenario_zeroReplicasError(t *testing.T) {
	ns := "cloud9-no-replicas"
	client := fake.NewClientset()
	design := deployer.DeploymentDesign{
		Nodes: []deployer.DeploymentNode{
			{Type: "db", Name: "DB", Config: map[string]any{"engine": "postgresql", "readReplicas": 0}},
		},
	}
	scenario := &PartitionScenario{Client: client, Scenario: scenarioStallReplica}
	if err := scenario.Apply(context.Background(), ns, design, "db"); err == nil {
		t.Fatal("expected error for zero replicas")
	}
}

func TestPartitionScenario_unsupportedEngineError(t *testing.T) {
	ns := "cloud9-mysql"
	client := fake.NewClientset()
	design := deployer.DeploymentDesign{
		Nodes: []deployer.DeploymentNode{
			{Type: "db", Name: "DB", Config: map[string]any{"engine": "mysql", "readReplicas": 1}},
		},
	}
	scenario := &PartitionScenario{Client: client, Scenario: scenarioSplitPrimaryReplica}
	if err := scenario.Apply(context.Background(), ns, design, "db"); err == nil {
		t.Fatal("expected error for unsupported engine")
	}
}

func TestPartitionScenario_splitAppStore_cache(t *testing.T) {
	ns := "cloud9-app-cache"
	client := fake.NewClientset()
	design := cacheDesign()
	scenario := &PartitionScenario{Client: client, Scenario: scenarioSplitAppStore}

	if err := scenario.Apply(context.Background(), ns, design, "cache"); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	policy, err := client.NetworkingV1().NetworkPolicies(ns).Get(context.Background(), "c9-chaos-split-app-store", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if policy.Spec.PodSelector.MatchLabels["app"] != "redis" {
		t.Fatalf("dest app: %v", policy.Spec.PodSelector.MatchLabels)
	}
}

func TestPartitionScenario_Stateful(t *testing.T) {
	s := &PartitionScenario{Scenario: scenarioStallReplica}
	if !s.Stateful() {
		t.Fatal("partition scenarios should be stateful")
	}
}

func TestActivePartitionScenarios(t *testing.T) {
	ns := "cloud9-active"
	expires := time.Now().Add(DefaultPartitionTTL)
	policy, err := BuildPartitionNetworkPolicy(PartitionPolicyInput{
		Namespace:       ns,
		Scenario:        scenarioSplitPrimaryReplica,
		TargetComponent: "db",
		ExpiresAt:       expires,
		DestAppLabel:    postgresPrimaryAppLabel,
		Port:            postgresPartitionPort,
		Exclude:         PartitionPeerExclusion{AppLabel: postgresReplicaAppLabel},
	})
	if err != nil {
		t.Fatalf("BuildPartitionNetworkPolicy: %v", err)
	}
	client := fake.NewClientset(policy)

	active, err := ActivePartitionScenarios(context.Background(), client, ns)
	if err != nil {
		t.Fatalf("ActivePartitionScenarios: %v", err)
	}
	if len(active) != 1 || active[0] != scenarioSplitPrimaryReplica {
		t.Fatalf("active: %v", active)
	}
}

func TestResolveStallReplicaPod(t *testing.T) {
	t.Parallel()
	ns := "cloud9-resolve-stall"
	client := fake.NewClientset(
		runningReplicaPod(ns, "postgres-replica-0"),
		runningReplicaPod(ns, "postgres-replica-1"),
		dbWriteEndpoints(ns, "postgres-6fbdd9c5dc-writer"),
	)

	tests := []struct {
		name      string
		requested string
		want      string
		wantErr   error
	}{
		{name: "empty picks first", requested: "", want: "postgres-replica-0"},
		{name: "explicit replica 1", requested: "postgres-replica-1", want: "postgres-replica-1"},
		{name: "unknown pod", requested: "postgres-replica-9", wantErr: ErrInvalidStallTarget},
		{name: "invalid name", requested: "postgres_replica_0", wantErr: ErrInvalidStallTarget},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := resolveStallReplicaPod(context.Background(), client, ns, tt.requested)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err: got %v want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			if got != tt.want {
				t.Fatalf("pod: got %q want %q", got, tt.want)
			}
		})
	}
}

func TestResolveStallReplicaPod_rejectsWriter(t *testing.T) {
	t.Parallel()
	ns := "cloud9-stall-writer"
	client := fake.NewClientset(
		runningReplicaPod(ns, "postgres-replica-0"),
		runningReplicaPod(ns, "postgres-replica-1"),
		dbWriteEndpoints(ns, "postgres-replica-0"),
	)
	_, err := resolveStallReplicaPod(context.Background(), client, ns, "postgres-replica-0")
	if !errors.Is(err, ErrInvalidStallTarget) {
		t.Fatalf("err: got %v want ErrInvalidStallTarget", err)
	}
	got, err := resolveStallReplicaPod(context.Background(), client, ns, "")
	if err != nil {
		t.Fatalf("auto pick: %v", err)
	}
	if got != "postgres-replica-1" {
		t.Fatalf("auto pick: got %q want postgres-replica-1", got)
	}
}

func TestPartitionScenario_stallTargetsRequestedPod(t *testing.T) {
	ns := "cloud9-stall-pick"
	client := fake.NewClientset(
		runningReplicaPod(ns, "postgres-replica-0"),
		runningReplicaPod(ns, "postgres-replica-1"),
	)
	design := dbDesignWithReplicas()
	scenario, _, dropped := testStallScenario(client)
	scenario.TargetPod = "postgres-replica-1"

	if err := scenario.Apply(context.Background(), ns, design, "db"); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if scenario.TargetPod != "postgres-replica-1" {
		t.Fatalf("resolved TargetPod: %q", scenario.TargetPod)
	}
	cm, err := client.CoreV1().ConfigMaps(ns).Get(context.Background(), "c9-chaos-stall-replica", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get stall state: %v", err)
	}
	if cm.Annotations[ChaosAnnotationReplicaPod] != "postgres-replica-1" {
		t.Fatalf("replica annotation: %q", cm.Annotations[ChaosAnnotationReplicaPod])
	}
	if len(*dropped) != 1 || (*dropped)[0] != "postgres-replica-1" {
		t.Fatalf("dropped: %v", *dropped)
	}
}

func TestPartitionScenario_stallDoubleApplyStillDrops(t *testing.T) {
	ns := "cloud9-stall-double"
	client := fake.NewClientset(runningReplicaPod(ns, "postgres-replica-0"))
	design := dbDesignWithReplicas()
	scenario, hosts, dropped := testStallScenario(client)

	if err := scenario.Apply(context.Background(), ns, design, "db"); err != nil {
		t.Fatalf("first Apply: %v", err)
	}
	if err := scenario.Apply(context.Background(), ns, design, "db"); err != nil {
		t.Fatalf("second Apply: %v", err)
	}
	if len(*hosts) != 2 || (*hosts)[0] != stallBlackholeHost || (*hosts)[1] != stallBlackholeHost {
		t.Fatalf("hosts: %v", *hosts)
	}
	if len(*dropped) != 2 || (*dropped)[0] != "postgres-replica-0" || (*dropped)[1] != "postgres-replica-0" {
		t.Fatalf("dropped: %v", *dropped)
	}
}

func TestPartitionScenario_splitDoesNotDropReplication(t *testing.T) {
	ns := "cloud9-split-no-drop"
	client := fake.NewClientset(runningReplicaPod(ns, "postgres-replica-0"))
	design := dbDesignWithReplicas()
	var hosts []string
	var dropped []string
	scenario := &PartitionScenario{
		Client:   client,
		Scenario: scenarioSplitPrimaryReplica,
		SetReplicaPrimaryHost: func(_ context.Context, _ kubernetes.Interface, _, _, host string) error {
			hosts = append(hosts, host)
			return nil
		},
		DropReplication: func(_ context.Context, _ kubernetes.Interface, _, applicationName string) error {
			dropped = append(dropped, applicationName)
			return nil
		},
	}
	if err := scenario.Apply(context.Background(), ns, design, "db"); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(hosts) != 0 {
		t.Fatalf("hosts: %v", hosts)
	}
	if len(dropped) != 0 {
		t.Fatalf("dropped: %v", dropped)
	}
}

func TestResolve_partitionScenarios(t *testing.T) {
	client := fake.NewClientset(runningReplicaPod("ns", "postgres-replica-0"))
	design := dbDesignWithReplicas()

	for _, name := range []string{scenarioStallReplica, scenarioSplitPrimaryReplica} {
		s, err := Resolve(name, "db", design, client)
		if err != nil {
			t.Fatalf("Resolve(%q): %v", name, err)
		}
		if !s.Stateful() {
			t.Fatalf("%q should be stateful", name)
		}
	}

	s, err := Resolve(scenarioSplitAppStore, "cache", cacheDesign(), client)
	if err != nil {
		t.Fatalf("Resolve split_app_store cache: %v", err)
	}
	if !s.Stateful() {
		t.Fatal("split_app_store should be stateful")
	}
}
