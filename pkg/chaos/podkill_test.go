package chaos

import (
	"context"
	"testing"

	"github.com/ywacch/cloud9/pkg/deployer"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestPodKillScenario_Apply(t *testing.T) {
	ns := "cloud9-test"
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "app-server-abc",
			Namespace: ns,
			Labels:    map[string]string{"app": "app-server"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	client := fake.NewClientset(pod)

	design := deployer.DeploymentDesign{
		Nodes: []deployer.DeploymentNode{{Type: "app-server", Name: "App"}},
	}
	scenario := &PodKillScenario{Client: client}

	if err := scenario.Apply(context.Background(), ns, design, "app-server"); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	_, err := client.CoreV1().Pods(ns).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err == nil {
		t.Fatal("expected pod to be deleted")
	}
}

func TestPodKillScenario_Apply_dbUsesWritePod(t *testing.T) {
	t.Parallel()

	ns := "cloud9-podkill-db"
	writer := "postgres-replica-0"
	client := fake.NewClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      writer,
				Namespace: ns,
				Labels:    map[string]string{"app": "postgres-replica"},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		},
		&corev1.Endpoints{
			ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: ns},
			Subsets: []corev1.EndpointSubset{{
				Addresses: []corev1.EndpointAddress{{
					TargetRef: &corev1.ObjectReference{Kind: "Pod", Name: writer},
				}},
			}},
		},
	)
	design := deployer.DeploymentDesign{
		Nodes: []deployer.DeploymentNode{{Type: "db", Name: "DB", Config: map[string]any{"engine": "postgresql"}}},
	}
	scenario := &PodKillScenario{Client: client}

	if err := scenario.Apply(context.Background(), ns, design, "db"); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	_, err := client.CoreV1().Pods(ns).Get(context.Background(), writer, metav1.GetOptions{})
	if err == nil {
		t.Fatal("expected write pod deleted")
	}
}
