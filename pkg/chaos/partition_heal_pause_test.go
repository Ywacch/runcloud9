package chaos

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestHealPartitions_pauseConsumers(t *testing.T) {
	t.Parallel()
	const ns = "ns-heal-pause"
	replicas := int32(1)
	client := fake.NewClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: workerDeploymentName, Namespace: ns},
			Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
		},
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pauseConsumersConfigMapName,
				Namespace: ns,
				Labels: map[string]string{
					ChaosLabelScenario: scenarioPauseConsumers,
					ChaosLabelTarget:   "mq",
				},
			},
			Data: map[string]string{pauseConsumersReplicasKey: "1"},
		},
	)
	// Simulate paused workers.
	zero := int32(0)
	dep, _ := client.AppsV1().Deployments(ns).Get(context.Background(), workerDeploymentName, metav1.GetOptions{})
	dep.Spec.Replicas = &zero
	_, _ = client.AppsV1().Deployments(ns).Update(context.Background(), dep, metav1.UpdateOptions{})

	healed, err := HealPartitions(context.Background(), client, ns)
	if err != nil {
		t.Fatalf("HealPartitions: %v", err)
	}
	if len(healed) != 1 || healed[0].Scenario != scenarioPauseConsumers {
		t.Fatalf("healed: %+v", healed)
	}
	dep, err = client.AppsV1().Deployments(ns).Get(context.Background(), workerDeploymentName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get deployment: %v", err)
	}
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 1 {
		t.Fatalf("replicas: %v want 1", dep.Spec.Replicas)
	}
}
