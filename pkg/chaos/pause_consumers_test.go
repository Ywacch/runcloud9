package chaos

import (
	"context"
	"testing"

	"github.com/ywacch/cloud9/pkg/deployer"
	"github.com/ywacch/cloud9/pkg/placement"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestPauseConsumers_ScalesWorker(t *testing.T) {
	t.Parallel()
	const ns = "ns-pause"
	replicas := int32(2)
	client := fake.NewClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: workerDeploymentName, Namespace: ns},
			Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
		},
	)

	s := &PauseConsumersScenario{Client: client}
	design := deployer.DeploymentDesign{
		ResolvedPlacement: &placement.Topology{
			Jobs: []placement.Job{{Job: "work_queue", Component: "mq", NodeID: "mq-1", Engine: "redis"}},
			Nodes: []placement.Node{{
				NodeID: "mq-1",
				Jobs:   []string{"work_queue"},
			}},
		},
	}
	if err := s.Apply(context.Background(), ns, design, "mq"); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	dep, err := client.AppsV1().Deployments(ns).Get(context.Background(), workerDeploymentName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get deployment: %v", err)
	}
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 0 {
		t.Fatalf("replicas after apply: %v want 0", dep.Spec.Replicas)
	}

	cm, err := client.CoreV1().ConfigMaps(ns).Get(context.Background(), pauseConsumersConfigMapName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get configmap: %v", err)
	}
	if cm.Data[pauseConsumersReplicasKey] != "2" {
		t.Fatalf("stored replicas: %q", cm.Data[pauseConsumersReplicasKey])
	}

	if err := s.Revert(context.Background(), ns, design, "mq"); err != nil {
		t.Fatalf("Revert: %v", err)
	}
	dep, err = client.AppsV1().Deployments(ns).Get(context.Background(), workerDeploymentName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get deployment after revert: %v", err)
	}
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 2 {
		t.Fatalf("replicas after revert: %v want 2", dep.Spec.Replicas)
	}
	if _, err := client.CoreV1().ConfigMaps(ns).Get(context.Background(), pauseConsumersConfigMapName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("configmap should be deleted, err=%v", err)
	}
}
