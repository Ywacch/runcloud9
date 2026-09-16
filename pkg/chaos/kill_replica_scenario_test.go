package chaos

import (
	"context"
	"strings"
	"testing"

	"github.com/ywacch/cloud9/pkg/deployer"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestKillReplicaScenario_Apply(t *testing.T) {
	t.Parallel()

	ns := "ns-kill-replica"
	client := fake.NewClientset(
		runningReplicaPod(ns, "postgres-replica-0"),
		runningReplicaPod(ns, "postgres-replica-1"),
		dbWriteEndpoints(ns, "postgres-replica-1"),
	)
	scenario := &KillReplicaScenario{Client: client}
	design := dbDesignWithReplicas()

	if err := scenario.Apply(context.Background(), ns, design, "db"); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	_, err := client.CoreV1().Pods(ns).Get(context.Background(), "postgres-replica-0", metav1.GetOptions{})
	if err == nil {
		t.Fatal("expected standby replica pod deleted")
	}
	_, err = client.CoreV1().Pods(ns).Get(context.Background(), "postgres-replica-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("write pod should remain: %v", err)
	}
}

func TestKillReplicaScenario_onlyWriterRemains(t *testing.T) {
	t.Parallel()

	ns := "ns-kill-only-writer"
	client := fake.NewClientset(
		runningReplicaPod(ns, "postgres-replica-0"),
		dbWriteEndpoints(ns, "postgres-replica-0"),
	)
	scenario := &KillReplicaScenario{Client: client}
	err := scenario.Apply(context.Background(), ns, dbDesignWithReplicas(), "db")
	if err == nil || !strings.Contains(err.Error(), "only write pod") {
		t.Fatalf("expected only-writer error, got %v", err)
	}
}

func dbWriteEndpoints(ns, writer string) *corev1.Endpoints {
	return &corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: ns},
		Subsets: []corev1.EndpointSubset{{
			Addresses: []corev1.EndpointAddress{{
				TargetRef: &corev1.ObjectReference{Kind: "Pod", Name: writer},
			}},
		}},
	}
}

func TestKillReplicaScenario_zeroReplicasError(t *testing.T) {
	t.Parallel()

	scenario := &KillReplicaScenario{Client: fake.NewClientset()}
	design := deployer.DeploymentDesign{
		Nodes: []deployer.DeploymentNode{
			{Type: "db", Config: map[string]any{"engine": "postgresql", "readReplicas": 0}},
		},
	}
	err := scenario.Apply(context.Background(), "ns", design, "db")
	if err == nil || !strings.Contains(err.Error(), "readReplicas") {
		t.Fatalf("expected readReplicas error, got %v", err)
	}
}

func TestKillReplicaScenario_Stateful(t *testing.T) {
	t.Parallel()

	s := &KillReplicaScenario{}
	if s.Stateful() {
		t.Fatal("kill_replica should not be stateful")
	}
}

func TestResolve_killReplica(t *testing.T) {
	t.Parallel()

	client := fake.NewClientset(runningReplicaPod("ns", "postgres-replica-0"))
	s, err := Resolve(ScenarioKillReplica, "db", dbDesignWithReplicas(), client)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if s.Stateful() {
		t.Fatal("kill_replica should not be stateful")
	}
	if _, ok := s.(*KillReplicaScenario); !ok {
		t.Fatalf("expected KillReplicaScenario, got %T", s)
	}
}
